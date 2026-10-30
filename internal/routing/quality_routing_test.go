package routing

import (
	"fmt"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func TestKnownWorkingNodesWinNewOriginsWithoutBlindPoolSampling(t *testing.T) {
	r, pool, p, good := targetTestRouter(t)
	r.authorities = func() []string { return nil }
	reportSelectedSuccess(t, r, p, good, "www.google.com:443")
	for i := 0; i < 128; i++ {
		h, e := newRoutableEntry(t, fmt.Sprintf(`{"cold":%d}`, i), "203.0.113.2")
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(p)
	for i := 0; i < 512; i++ {
		route, err := r.RouteRequest(p.Name, "", "unvisited.example:443")
		if err != nil || route.NodeHash != good {
			t.Fatalf("blind pool node displaced a real working route: %v %v", route, err)
		}
	}
	// Peer traffic must not establish web confidence on another node.
	h, e := newRoutableEntry(t, `{"syncthing":true}`, "203.0.113.3")
	pool.addEntry(h, e)
	pool.rebuildPlatformView(p)
	reportSelectedSuccess(t, r, p, h, "peer.example:22000")
	if got := r.routeTarget(p, "unvisited.example:443", nil, time.Now()).evidence(h); got.proven != 0 {
		t.Fatalf("Syncthing leaked confidence: %+v", got)
	}
	r.ReportTargetFailure(RouteResult{PlatformID: p.ID, NodeHash: good}, "peer.example:22000")
	if got := r.routeTarget(p, "unvisited.example:443", nil, time.Now()).evidence(good); got.proven == 0 {
		t.Fatal("Syncthing revoked web confidence")
	}
}

func TestPassiveThroughputWeightsWorkingCandidatesWithoutPinning(t *testing.T) {
	r, pool, p, fast := targetTestRouter(t)
	slow, e := newRoutableEntry(t, `{"quality":"slow"}`, "203.0.113.2")
	pool.addEntry(slow, e)
	pool.rebuildPlatformView(p)
	for _, h := range []node.Hash{fast, slow} {
		reportSelectedSuccess(t, r, p, h, "known.example:443")
		reportSelectedSuccess(t, r, p, h, "known.example:443")
		entry, _ := pool.GetEntry(h)
		entry.LatencyTable.LoadEntry("example", node.DomainLatencyStats{Ewma: 50 * time.Millisecond, LastUpdated: time.Now()})
	}
	fastEntry, _ := pool.GetEntry(fast)
	slowEntry, _ := pool.GetEntry(slow)
	fastEntry.Traffic.ObserveDownload(8*1024*1024, time.Second, time.Now())
	slowEntry.Traffic.ObserveDownload(128*1024, time.Second, time.Now())
	seen := map[node.Hash]int{}
	for i := 0; i < 2048; i++ {
		route, err := r.RouteRequest(p.Name, "", "fresh.example:443")
		if err != nil {
			t.Fatal(err)
		}
		seen[route.NodeHash]++
	}
	if seen[fast] < 1500 || seen[slow] == 0 {
		t.Fatalf("throughput not weighted or route pinned: fast=%d slow=%d", seen[fast], seen[slow])
	}
	// A high throughput sample must never bypass an exact-origin failure.
	r.ReportTargetFailure(RouteResult{PlatformID: p.ID, NodeHash: fast}, "fresh.example:443")
	for i := 0; i < 32; i++ {
		route, err := r.RouteRequest(p.Name, "", "fresh.example:443")
		if err != nil || route.NodeHash != slow {
			t.Fatalf("speed bypassed target quarantine: %v %v", route, err)
		}
	}
}

func TestRestoreTargetHistoryRespectsFailureOrderingAndStaleness(t *testing.T) {
	r, _, p, h := targetTestRouter(t)
	now := time.Now()
	records := []TargetObservation{
		{PlatformID: p.ID, NodeHash: h, SelectedAt: now.Add(-3 * time.Minute), ObservedAt: now.Add(-time.Minute), Target: "api.example:443", Success: true},
		{PlatformID: p.ID, NodeHash: h, SelectedAt: now.Add(-4 * time.Minute), ObservedAt: now.Add(-2 * time.Minute), Target: "api.example:443", Success: false},
		{PlatformID: p.ID, NodeHash: h, SelectedAt: now.Add(-time.Hour), ObservedAt: now.Add(-time.Hour), Target: "expired.example:443", Success: true},
	}
	r.RestoreTargetHistory(records)
	got := r.routeTarget(p, "api.example:443", nil, now).evidence(h)
	if !got.cooling || got.successes != 0 || got.proven != 0 {
		t.Fatalf("late old connection erased newer failure: %+v", got)
	}
	if got := r.routeTarget(p, "expired.example:443", nil, now).evidence(h); got.successes != 0 {
		t.Fatalf("restored stale evidence: %+v", got)
	}
	// An unrelated fresh node's historical response also primes new origins.
	r2, _, p2, h2 := targetTestRouter(t)
	r2.RestoreTargetHistory([]TargetObservation{{PlatformID: p2.ID, NodeHash: h2, SelectedAt: now.Add(-time.Minute), ObservedAt: now.Add(-30 * time.Second), Target: "working.example:443", Success: true}})
	if got := r2.routeTarget(p2, "unvisited.example:443", nil, now).evidence(h2); got.proven != 1 {
		t.Fatalf("startup discarded real traffic evidence: %+v", got)
	}
}

func TestProvenPoolAllowsOnlySmallRankedProbeExploration(t *testing.T) {
	r, pool, p, good := targetTestRouter(t)
	reportSelectedSuccess(t, r, p, good, "working.example:443")
	for i := 0; i < 64; i++ {
		h, e := newRoutableEntry(t, fmt.Sprintf(`{"probe-only":%d}`, i), "203.0.113.2")
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(p)
	proven := 0
	for i := 0; i < 2048; i++ {
		route, err := r.RouteRequest(p.Name, "", "new.example:443")
		if err != nil {
			t.Fatal(err)
		}
		if route.NodeHash == good {
			proven++
		}
	}
	if proven < 1840 || proven == 2048 {
		t.Fatalf("known nodes/trial balance=%d/2048", proven)
	}
}
