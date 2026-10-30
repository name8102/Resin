package routing

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

type getEntryCountPool struct {
	inner    PoolAccessor
	getCalls int
}

func (p *getEntryCountPool) GetEntry(hash node.Hash) (*node.NodeEntry, bool) {
	p.getCalls++
	return p.inner.GetEntry(hash)
}

func (p *getEntryCountPool) GetPlatform(id string) (*platform.Platform, bool) {
	return p.inner.GetPlatform(id)
}

func (p *getEntryCountPool) GetPlatformByName(name string) (*platform.Platform, bool) {
	return p.inner.GetPlatformByName(name)
}

func (p *getEntryCountPool) RangePlatforms(fn func(*platform.Platform) bool) {
	p.inner.RangePlatforms(fn)
}

type transientMissingPool struct {
	inner  PoolAccessor
	target node.Hash
	missed atomic.Bool
}

func (p *transientMissingPool) GetEntry(hash node.Hash) (*node.NodeEntry, bool) {
	if hash == p.target && !p.missed.Swap(true) {
		return nil, false
	}
	return p.inner.GetEntry(hash)
}

func (p *transientMissingPool) GetPlatform(id string) (*platform.Platform, bool) {
	return p.inner.GetPlatform(id)
}

func (p *transientMissingPool) GetPlatformByName(name string) (*platform.Platform, bool) {
	return p.inner.GetPlatformByName(name)
}

func (p *transientMissingPool) RangePlatforms(fn func(*platform.Platform) bool) {
	p.inner.RangePlatforms(fn)
}

func TestRouteRequest_DefaultPlatformRequiresWellKnownID(t *testing.T) {
	pool := newRouterTestPool()
	// Default name alone is not sufficient; only the well-known ID is treated as default.
	plat := platform.NewPlatform("legacy-default-id", platform.DefaultPlatformName, nil, nil)
	pool.addPlatform(plat)

	defaultHash, defaultEntry := newRoutableEntry(t, `{"id":"default-by-name"}`, "198.51.100.10")
	pool.addEntry(defaultHash, defaultEntry)
	pool.rebuildPlatformView(plat)

	router := newTestRouter(pool, nil)
	_, err := router.RouteRequest("", "", "https://example.com")
	if !errors.Is(err, ErrPlatformNotFound) {
		t.Fatalf("expected ErrPlatformNotFound without default ID, got %v", err)
	}
}

func TestRouteRequest_EmptyAccountStaleViewReturnsError(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-stale", "Plat-Stale", nil, nil)
	pool.addPlatform(plat)

	h, e := newRoutableEntry(t, `{"id":"stale-view-node"}`, "203.0.113.10")
	pool.addEntry(h, e)
	pool.rebuildPlatformView(plat)
	if !plat.View().Contains(h) {
		t.Fatal("setup failed: expected node in view")
	}

	// Create stale view: remove from pool without notifying platform.
	pool.removeEntry(h)

	router := newTestRouter(pool, nil)
	_, err := router.RouteRequest(plat.Name, "", "https://example.com")
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("expected ErrNoAvailableNodes for stale view, got %v", err)
	}
}

func TestRouteRequest_EmptyAccount_TransientMissingEntryRetriesOnce(t *testing.T) {
	basePool := newRouterTestPool()
	plat := platform.NewPlatform("plat-retry", "Plat-Retry", nil, nil)
	basePool.addPlatform(plat)

	h, e := newRoutableEntry(t, `{"id":"transient-miss"}`, "203.0.113.55")
	basePool.addEntry(h, e)
	basePool.rebuildPlatformView(plat)
	if !plat.View().Contains(h) {
		t.Fatal("setup failed: expected node in view")
	}

	pool := &transientMissingPool{
		inner:  basePool,
		target: h,
	}
	router := newTestRouter(pool, nil)

	res, err := router.RouteRequest(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("expected retry to recover from transient miss, got %v", err)
	}
	if res.NodeHash != h {
		t.Fatalf("unexpected selected node after retry: got=%s want=%s", res.NodeHash.Hex(), h.Hex())
	}
}

func TestRandomRoute_SingleNodeTrustsViewWithoutPoolValidation(t *testing.T) {
	basePool := newRouterTestPool()
	plat := platform.NewPlatform("plat-single", "Plat-Single", nil, nil)
	basePool.addPlatform(plat)

	h, e := newRoutableEntry(t, `{"id":"single-view-only"}`, "203.0.113.42")
	basePool.addEntry(h, e)
	basePool.rebuildPlatformView(plat)

	countingPool := &getEntryCountPool{inner: basePool}
	stats := NewIPLoadStats()

	got, err := randomRoute(
		plat,
		stats,
		countingPool,
		"example.com",
		[]string{"cloudflare.com"},
		10*time.Minute,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("randomRoute failed: %v", err)
	}
	if got != h {
		t.Fatalf("unexpected selected node: got=%s want=%s", got.Hex(), h.Hex())
	}
	if countingPool.getCalls != 0 {
		t.Fatalf("single-node randomRoute should trust view and skip pool validation, getEntry calls=%d", countingPool.getCalls)
	}
}

func TestRandomRoute_PrefersMeasuredTargetOverUnknownAndAuthority(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("measured", "Measured", nil, nil)
	pool.addPlatform(plat)
	target, entry := newRoutableEntry(t, `{"id":"known-target"}`, "203.0.113.20")
	entry.LatencyTable.Update("example.com", 50*time.Millisecond, time.Minute)
	pool.addEntry(target, entry)
	authority, entry := newRoutableEntry(t, `{"id":"known-authority"}`, "203.0.113.21")
	entry.LatencyTable.Update("cloudflare.com", time.Millisecond, time.Minute)
	pool.addEntry(authority, entry)
	for i := 0; i < 100; i++ {
		h, e := newRoutableEntry(t, fmt.Sprintf(`{"unknown":%d}`, i), "203.0.113.22")
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(plat)
	stats := NewIPLoadStats()
	for i := 0; i < 20; i++ {
		got, err := randomRoute(plat, stats, pool, "example.com", []string{"cloudflare.com"}, time.Minute, nil, nil)
		if err != nil || got != target {
			t.Fatalf("known target route lost to unmeasured candidate: %v %v", got, err)
		}
	}
	got, err := randomRoute(plat, stats, pool, "example.com", []string{"cloudflare.com"}, time.Minute, func(h node.Hash) bool { return h != target }, nil)
	if err != nil || got != authority {
		t.Fatalf("excluded target should fall back to known authority: %v %v", got, err)
	}
}

func TestRandomRoute_IgnoresExpiredEvidenceAndChoosesLowerScore(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("recent", "Recent", nil, nil)
	pool.addPlatform(plat)
	expired, entry := newRoutableEntry(t, `{"id":"expired"}`, "203.0.113.30")
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{Ewma: time.Millisecond, LastUpdated: time.Now().Add(-time.Hour)})
	pool.addEntry(expired, entry)
	slow, entry := newRoutableEntry(t, `{"id":"slow"}`, "203.0.113.31")
	entry.LatencyTable.Update("example.com", time.Second, time.Minute)
	pool.addEntry(slow, entry)
	fast, entry := newRoutableEntry(t, `{"id":"fast"}`, "203.0.113.32")
	entry.LatencyTable.Update("example.com", 50*time.Millisecond, time.Minute)
	pool.addEntry(fast, entry)
	pool.rebuildPlatformView(plat)
	got, err := randomRoute(plat, NewIPLoadStats(), pool, "example.com", nil, time.Minute, nil, nil)
	if err != nil || got != fast {
		t.Fatalf("want recent fastest route, got %v %v", got, err)
	}
}

func TestChooseSameIPRotationCandidate_PicksLowestLatency(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-rotate", "Plat-Rotate", nil, nil)
	pool.addPlatform(plat)

	currentHash, currentEntry := newRoutableEntry(t, `{"id":"current-rotate"}`, "198.51.100.77")
	candidateA, entryA := newRoutableEntry(t, `{"id":"candidate-a-rotate"}`, "198.51.100.77")
	candidateB, entryB := newRoutableEntry(t, `{"id":"candidate-b-rotate"}`, "198.51.100.77")
	entryA.LatencyTable.Update("example.com", 70*time.Millisecond, 10*time.Minute)
	entryB.LatencyTable.Update("example.com", 15*time.Millisecond, 10*time.Minute)
	waitForDomainLatency(t, entryA, "example.com")
	waitForDomainLatency(t, entryB, "example.com")

	pool.addEntry(currentHash, currentEntry)
	pool.addEntry(candidateA, entryA)
	pool.addEntry(candidateB, entryB)
	pool.rebuildPlatformView(plat)

	// Invalidate current lease node and keep same-IP candidates routable.
	currentEntry.CircuitOpenSince.Store(time.Now().UnixNano())
	plat.NotifyDirty(
		currentHash,
		pool.GetEntry,
		func(_ string, _ node.Hash) (string, bool, []string, bool) { return "", true, nil, true },
		func(_ netip.Addr) string { return "" },
	)

	hash, ok := chooseSameIPRotationCandidate(
		plat,
		pool,
		netip.MustParseAddr("198.51.100.77"),
		"example.com",
		[]string{"cloudflare.com"},
		10*time.Minute,
		nil,
	)
	if !ok {
		t.Fatal("expected same-ip rotation candidate")
	}
	if hash != candidateB {
		t.Fatalf("expected lowest-latency candidate %s, got %s", candidateB.Hex(), hash.Hex())
	}
}

func TestRouteRequest_SameIPRotationMissRecreatesLease(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-miss", "Plat-Miss", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	currentHash, currentEntry := newRoutableEntry(t, `{"id":"current-miss"}`, "203.0.113.60")
	replacementHash, replacementEntry := newRoutableEntry(t, `{"id":"replacement-miss"}`, "203.0.113.61")
	pool.addEntry(currentHash, currentEntry)
	pool.addEntry(replacementHash, replacementEntry)
	pool.rebuildPlatformView(plat)

	// Invalidate current lease node so route must rotate.
	currentEntry.CircuitOpenSince.Store(time.Now().UnixNano())
	plat.NotifyDirty(
		currentHash,
		pool.GetEntry,
		func(_ string, _ node.Hash) (string, bool, []string, bool) { return "", true, nil, true },
		func(_ netip.Addr) string { return "" },
	)

	var events []LeaseEvent
	router := newTestRouter(pool, func(e LeaseEvent) {
		events = append(events, e)
	})
	state, _ := router.states.LoadOrCompute(plat.ID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})

	oldExpiry := time.Now().Add(time.Hour).UnixNano()
	oldLease := Lease{
		NodeHash:       currentHash,
		EgressIP:       currentEntry.GetEgressIP(),
		ExpiryNs:       oldExpiry,
		LastAccessedNs: time.Now().UnixNano(),
	}
	state.Leases.CreateLease("acct-miss", oldLease)

	res, err := router.RouteRequest(plat.Name, "acct-miss", "https://example.com")
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}
	if !res.LeaseCreated {
		t.Fatal("expected lease recreation when same-ip rotation misses")
	}
	if res.NodeHash != replacementHash {
		t.Fatalf("expected replacement node %s, got %s", replacementHash.Hex(), res.NodeHash.Hex())
	}

	newLease, ok := state.Leases.GetLease("acct-miss")
	if !ok {
		t.Fatal("expected recreated lease")
	}
	if newLease.NodeHash != replacementHash {
		t.Fatalf("lease node not updated: got=%s want=%s", newLease.NodeHash.Hex(), replacementHash.Hex())
	}
	if newLease.ExpiryNs == oldExpiry {
		t.Fatalf("recreated lease must have new expiry, still got old %d", oldExpiry)
	}
	if got := state.IPLoadStats.Get(oldLease.EgressIP); got != 0 {
		t.Fatalf("old egress ip load should be 0, got %d", got)
	}
	if got := state.IPLoadStats.Get(replacementEntry.GetEgressIP()); got != 1 {
		t.Fatalf("new egress ip load should be 1, got %d", got)
	}

	foundRemove := false
	foundCreate := false
	for _, e := range events {
		if e.Type == LeaseRemove && e.Account == "acct-miss" && e.NodeHash == currentHash {
			foundRemove = true
		}
		if e.Type == LeaseCreate && e.Account == "acct-miss" && e.NodeHash == replacementHash {
			foundCreate = true
		}
	}
	if !foundRemove {
		t.Fatal("expected LeaseRemove event for invalidated old lease")
	}
	if !foundCreate {
		t.Fatal("expected LeaseCreate event for recreated lease")
	}
}

func TestLeaseCleaner_SweepExpiresLeaseDeterministically(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-cleaner", "Plat-Cleaner", nil, nil)
	pool.addPlatform(plat)

	var events []LeaseEvent
	router := newTestRouter(pool, func(e LeaseEvent) {
		events = append(events, e)
	})
	state, _ := router.states.LoadOrCompute(plat.ID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})

	h := node.HashFromRawOptions(json.RawMessage(`{"id":"expired-lease-node"}`))
	ip := netip.MustParseAddr("203.0.113.88")
	state.Leases.CreateLease("acct-expire", Lease{
		NodeHash:       h,
		EgressIP:       ip,
		ExpiryNs:       time.Now().Add(-1 * time.Minute).UnixNano(),
		LastAccessedNs: time.Now().Add(-2 * time.Minute).UnixNano(),
	})
	if got := state.IPLoadStats.Get(ip); got != 1 {
		t.Fatalf("setup ip load: got=%d want=1", got)
	}

	cleaner := NewLeaseCleaner(router)
	cleaner.sweep()

	if _, ok := state.Leases.GetLease("acct-expire"); ok {
		t.Fatal("expected expired lease to be removed by sweep")
	}
	if got := state.IPLoadStats.Get(ip); got != 0 {
		t.Fatalf("expected ip load to be decremented after sweep, got %d", got)
	}

	foundExpire := false
	for _, e := range events {
		if e.Type == LeaseExpire && e.PlatformID == plat.ID && e.Account == "acct-expire" && e.NodeHash == h {
			foundExpire = true
			break
		}
	}
	if !foundExpire {
		t.Fatal("expected LeaseExpire event from sweep")
	}
}
