package routing

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

func targetTestRouter(t *testing.T) (*Router, *routerTestPool, *platform.Platform, node.Hash) {
	t.Helper()
	pool := newRouterTestPool()
	plat := platform.NewPlatform("target-platform", "Target", nil, nil)
	pool.addPlatform(plat)
	h, e := newRoutableEntry(t, `{"id":"target"}`, "203.0.113.1")
	pool.addEntry(h, e)
	pool.rebuildPlatformView(plat)
	return newTestRouter(pool, nil), pool, plat, h
}

func TestTargetFailure_IsolatesExactHostPort(t *testing.T) {
	r, _, p, h := targetTestRouter(t)
	route := RouteResult{PlatformID: p.ID, NodeHash: h}
	r.ReportTargetFailure(route, "API.GitHub.COM.:443")
	for _, target := range []string{"api.github.com:443", "https://api.github.com/path?q=ignored"} {
		if _, err := r.RouteRequest(p.Name, "", target); !errors.Is(err, ErrNoAvailableNodes) {
			t.Fatalf("cooled target %q: %v", target, err)
		}
	}
	for _, target := range []string{"api.github.com:22", "github.com:443", "www.google.com:443"} {
		if _, err := r.RouteRequest(p.Name, "", target); err != nil {
			t.Fatalf("unaffected target %q: %v", target, err)
		}
	}
}

func TestTargetFailure_ExpiresAndRemainsBounded(t *testing.T) {
	r, _, p, h := targetTestRouter(t)
	key := targetHealthKey{p.ID, h, "api.github.com:443"}
	r.targetHealth.failure(key, time.Now().Add(-targetFailureCooldown), r.p2cWindow())
	if _, err := r.RouteRequest(p.Name, "", "api.github.com:443"); err != nil {
		t.Fatalf("expired cooldown: %v", err)
	}
	now := time.Now()
	for i := 0; i <= maxTargetHealthEntries; i++ {
		r.targetHealth.failure(targetHealthKey{p.ID, h, fmt.Sprintf("%d.example.com:443", i)}, now, r.p2cWindow())
	}
	if len(r.targetHealth.entries) != maxTargetHealthEntries || r.targetHealth.order.Len() != maxTargetHealthEntries {
		t.Fatal("target cache exceeded capacity")
	}
	if r.targetHealth.evidence(targetHealthKey{p.ID, h, "0.example.com:443"}, now, r.p2cWindow()).cooling {
		t.Fatal("oldest target failure was not evicted")
	}
}

func TestTargetFailure_DisabledPassiveBreakerStillHonorsExclusions(t *testing.T) {
	r, _, p, h := targetTestRouter(t)
	r.ReportTargetFailure(RouteResult{PlatformID: p.ID, NodeHash: h}, "api.github.com:443")
	p.PassiveCircuitBreakerDisabled = true
	if _, err := r.RouteRequest(p.Name, "device", "api.github.com:443"); err != nil {
		t.Fatalf("disabled breaker applied old cooldown: %v", err)
	}
	if _, err := r.RouteRequest(p.Name, "device", "api.github.com:443", h); !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("per-request exclusion ignored: %v", err)
	}
	r.ReportTargetFailure(RouteResult{PlatformID: p.ID, NodeHash: h}, "other.example.com:443")
	p.PassiveCircuitBreakerDisabled = false
	if _, err := r.RouteRequest(p.Name, "", "other.example.com:443"); err != nil {
		t.Fatalf("disabled passive failure was stored: %v", err)
	}
}

func TestTargetFailure_FindsRemainingCandidate(t *testing.T) {
	r, pool, p, good := targetTestRouter(t)
	for i := 0; i < 24; i++ {
		h, e := newRoutableEntry(t, fmt.Sprintf(`{"id":"bad-%d"}`, i), "203.0.113.2")
		pool.addEntry(h, e)
		r.ReportTargetFailure(RouteResult{PlatformID: p.ID, NodeHash: h}, "api.github.com:443")
	}
	pool.rebuildPlatformView(p)
	for i := 0; i < 12; i++ {
		route, err := r.RouteRequest(p.Name, "", "api.github.com:443")
		if err != nil || route.NodeHash != good {
			t.Fatalf("missed available candidate: route=%s err=%v", route.NodeHash.Hex(), err)
		}
	}
}

func TestNormalizeRouteTarget(t *testing.T) {
	for input, want := range map[string]string{
		"EXAMPLE.com.":                  "example.com:80",
		"http://EXAMPLE.com/path":       "example.com:80",
		"https://EXAMPLE.com/path":      "example.com:443",
		"https://example.com:8443/path": "example.com:8443",
		"[::1]:443":                     "[::1]:443",
		"https://[::1]/":                "[::1]:443",
		"api.github.com:22":             "api.github.com:22",
	} {
		if got := normalizeRouteTarget(input); got != want {
			t.Errorf("%q: got %q want %q", input, got, want)
		}
	}
}

func reportSelectedSuccess(t *testing.T, r *Router, p *platform.Platform, h node.Hash, target string) RouteResult {
	t.Helper()
	var excluded []node.Hash
	p.View().Range(func(other node.Hash) bool {
		if other != h {
			excluded = append(excluded, other)
		}
		return true
	})
	route, err := r.RouteRequest(p.Name, "", target, excluded...)
	if err != nil {
		t.Fatal(err)
	}
	r.ReportTargetSuccess(route, target)
	return route
}

func TestTargetSuccess_PrefersExactOriginAndPlatform(t *testing.T) {
	r, pool, p, good := targetTestRouter(t)
	p.AllocationPolicy = platform.AllocationPolicyPreferLowLatency
	fast, entry := newRoutableEntry(t, `{"id":"fast-other-origin"}`, "203.0.113.2")
	entry.LatencyTable.LoadEntry("github.com", node.DomainLatencyStats{Ewma: time.Millisecond, LastUpdated: time.Now()})
	pool.addEntry(fast, entry)
	pool.rebuildPlatformView(p)
	reportSelectedSuccess(t, r, p, good, "API.GitHub.COM.:443")
	for target, want := range map[string]node.Hash{
		"https://api.github.com/path": good,
		"api.github.com:80":           fast,
		"github.com:443":              fast,
		"uploads.github.com:443":      fast,
	} {
		route, err := r.RouteRequest(p.Name, "", target)
		if err != nil || route.NodeHash != want {
			t.Fatalf("target %q: got %s, want %s, err=%v", target, route.NodeHash.Hex(), want.Hex(), err)
		}
	}
	other := platform.NewPlatform("other-platform", "Other", nil, nil)
	pool.addPlatform(other)
	pool.rebuildPlatformView(other)
	route, err := r.RouteRequest(other.Name, "", "api.github.com:443")
	if err != nil || route.NodeHash != fast {
		t.Fatalf("target success leaked across platforms: %v %v", route, err)
	}
}

func TestTargetSuccess_StableWinsAndSharesThreeBestScores(t *testing.T) {
	r, pool, p, first := targetTestRouter(t)
	p.AllocationPolicy = platform.AllocationPolicyPreferLowLatency
	hashes := []node.Hash{first}
	for i := 1; i < 4; i++ {
		h, e := newRoutableEntry(t, fmt.Sprintf(`{"stable":%d}`, i), fmt.Sprintf("203.0.113.%d", i+1))
		pool.addEntry(h, e)
		hashes = append(hashes, h)
	}
	for i, h := range hashes {
		entry, _ := pool.GetEntry(h)
		latency := time.Duration(i+1) * 10 * time.Millisecond
		if i == 3 {
			latency = time.Second
		}
		entry.LatencyTable.LoadEntry("github.com", node.DomainLatencyStats{Ewma: latency, LastUpdated: time.Now()})
	}
	pool.rebuildPlatformView(p)
	for _, h := range hashes {
		reportSelectedSuccess(t, r, p, h, "api.github.com:443")
	}
	// The slowest node has two recent responses; all faster nodes have only one.
	reportSelectedSuccess(t, r, p, hashes[3], "api.github.com:443")
	for i := 0; i < 10; i++ {
		route, err := r.RouteRequest(p.Name, "", "api.github.com:443")
		if err != nil || route.NodeHash != hashes[3] {
			t.Fatalf("stable evidence lost to one response: %v %v", route, err)
		}
	}
	for _, h := range hashes[:3] {
		reportSelectedSuccess(t, r, p, h, "api.github.com:443")
	}
	seen := make(map[node.Hash]int)
	for i := 0; i < 120; i++ {
		route, err := r.RouteRequest(p.Name, "", "api.github.com:443")
		if err != nil {
			t.Fatal(err)
		}
		if route.NodeHash == hashes[3] {
			t.Fatal("selected clearly worse fourth stable candidate")
		}
		seen[route.NodeHash]++
	}
	if len(seen) != 3 {
		t.Fatalf("stable pool did not share connections: %v", seen)
	}
	route, err := r.RouteRequest(p.Name, "account", "api.github.com:443")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		next, err := r.RouteRequest(p.Name, "account", "api.github.com:443")
		if err != nil || next.NodeHash != route.NodeHash || next.LeaseCreated {
			t.Fatalf("stable pool broke account lease: %v %v", next, err)
		}
	}
	next, err := r.RouteRequest(p.Name, "account", "api.github.com:443", route.NodeHash)
	if err != nil || next.NodeHash == route.NodeHash {
		t.Fatalf("stable pool ignored request exclusion: %v %v", next, err)
	}
}

func TestTargetFailure_ExpiredCooldownDoesNotReviveOldFastSample(t *testing.T) {
	r, pool, p, failed := targetTestRouter(t)
	entry, _ := pool.GetEntry(failed)
	entry.LatencyTable.LoadEntry("github.com", node.DomainLatencyStats{Ewma: time.Millisecond, LastUpdated: time.Now()})
	unknown, e := newRoutableEntry(t, `{"id":"unknown-origin"}`, "203.0.113.2")
	pool.addEntry(unknown, e)
	pool.rebuildPlatformView(p)
	r.authorities = func() []string { return nil }
	key := targetHealthKey{p.ID, failed, "api.github.com:443"}
	r.targetHealth.failure(key, time.Now().Add(-targetFailureCooldown-time.Second), r.p2cWindow())
	for i := 0; i < 10; i++ {
		route, err := r.RouteRequest(p.Name, "", "api.github.com:443")
		if err != nil || route.NodeHash != unknown {
			t.Fatalf("old fast sample attracted traffic after cooldown: %v %v", route, err)
		}
	}
	// When no healthier candidate is available, permit a recovery attempt.
	route, err := r.RouteRequest(p.Name, "", "api.github.com:443", unknown)
	if err != nil || route.NodeHash != failed {
		t.Fatalf("recovery candidate unavailable: %v %v", route, err)
	}
	r.ReportTargetSuccess(route, "api.github.com:443")
	recovered, err := r.RouteRequest(p.Name, "", "api.github.com:443")
	if err != nil || recovered.NodeHash != failed {
		t.Fatalf("actual origin response did not restore route: %v %v", recovered, err)
	}
}

func TestTargetSuccess_OldConnectionCannotClearNewFailure(t *testing.T) {
	r, _, p, h := targetTestRouter(t)
	route := reportSelectedSuccess(t, r, p, h, "api.github.com:443")
	r.ReportTargetFailure(route, "api.github.com:443")
	r.ReportTargetSuccess(route, "api.github.com:443")
	key := targetHealthKey{p.ID, h, "api.github.com:443"}
	if got := r.targetHealth.evidence(key, time.Now(), r.p2cWindow()); !got.cooling || !got.failed || got.successes != 0 {
		t.Fatalf("old connection overrode failure: %+v", got)
	}
	// Stale responses remain stale even after the cooldown, and a route picked
	// after the failure cannot clear it while the cooldown is still active.
	now := time.Now()
	r.targetHealth.success(key, route.selectedAt, now.Add(targetFailureCooldown+time.Second), r.p2cWindow())
	r.targetHealth.success(key, now, now, r.p2cWindow())
	if got := r.targetHealth.evidence(key, now.Add(targetFailureCooldown+time.Second), r.p2cWindow()); got.cooling || !got.failed || got.successes != 0 {
		t.Fatalf("stale or cooling success rehabilitated failure: %+v", got)
	}
}

func TestTargetHealth_BackoffRetentionAndSuccessWindow(t *testing.T) {
	var cache targetHealthCache
	key := targetHealthKey{target: "api.github.com:443"}
	now := time.Now()
	window := 10 * time.Minute
	for _, cooldown := range []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute, 30 * time.Minute, 30 * time.Minute} {
		cache.failure(key, now, window)
		cache.mu.Lock()
		health := cache.get(key, now, window)
		cache.mu.Unlock()
		if got := health.until.Sub(now); got != cooldown {
			t.Fatalf("cooldown = %v, want %v", got, cooldown)
		}
		now = health.until.Add(time.Second)
	}
	if got := cache.evidence(key, now, window); !got.failed || got.cooling {
		t.Fatalf("failure should survive cooldown: %+v", got)
	}
	expired := now.Add(targetFailureRetention)
	if got := cache.evidence(key, expired, window); got != (targetEvidence{}) {
		t.Fatalf("retained failure did not expire: %+v", got)
	}
	cache.failure(key, expired, window)
	cache.mu.Lock()
	health := cache.get(key, expired, window)
	cache.mu.Unlock()
	if got := health.until.Sub(expired); got != targetFailureCooldown {
		t.Fatalf("expired failure did not reset backoff: %v", got)
	}
	now = health.until.Add(time.Second)
	cache.success(key, now, now, window)
	cache.success(key, now.Add(time.Minute), now.Add(time.Minute), window)
	if got := cache.evidence(key, now.Add(time.Minute), window); got.successes != 2 || got.failed {
		t.Fatalf("recent successes not stable: %+v", got)
	}
	if got := cache.evidence(key, now.Add(window+30*time.Second), window); got.successes != 1 {
		t.Fatalf("old first success counted as recent: %+v", got)
	}
	if got := cache.evidence(key, now.Add(window+2*time.Minute), window); got.successes != 0 || got.failed {
		t.Fatalf("success expiry: %+v", got)
	}
	cache.failure(key, now.Add(window+3*time.Minute), window)
	cache.mu.Lock()
	health = cache.get(key, now.Add(window+3*time.Minute), window)
	cache.mu.Unlock()
	if health.until.Sub(health.lastFailure) != targetFailureCooldown {
		t.Fatal("actual recovery did not reset failure backoff")
	}
}

func TestTargetHealth_SuccessEntriesShareBoundedCache(t *testing.T) {
	var cache targetHealthCache
	now := time.Now()
	for i := 0; i <= maxTargetHealthEntries; i++ {
		key := targetHealthKey{target: fmt.Sprintf("%d.example.com:443", i)}
		cache.success(key, now, now, time.Minute)
	}
	if len(cache.entries) != maxTargetHealthEntries || cache.order.Len() != maxTargetHealthEntries {
		t.Fatal("success entries bypassed bounded cache")
	}
	if cache.evidence(targetHealthKey{target: "0.example.com:443"}, now, time.Minute).successes != 0 {
		t.Fatal("oldest success was not evicted")
	}
	expired := now.Add(2 * time.Minute)
	if got := cache.evidence(targetHealthKey{target: "1.example.com:443"}, expired, time.Minute); got != (targetEvidence{}) {
		t.Fatalf("expired success retained: %+v", got)
	}
	if len(cache.entries) != maxTargetHealthEntries-1 {
		t.Fatal("expired success entry was not removed")
	}
}
