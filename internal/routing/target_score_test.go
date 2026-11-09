package routing

import (
	"math"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

func TestTargetQuality_SeparateFromReachabilityAndOtherOrigins(t *testing.T) {
	r, _, p, h := targetTestRouter(t)
	route, err := r.RouteRequest(p.Name, "", "a.example:443")
	if err != nil {
		t.Fatal(err)
	}
	r.ReportTargetQuality(route, "a.example:443", 20*time.Millisecond, 8*1024*1024)
	got := r.routeTarget(p, "a.example:443", nil, time.Now()).evidence(h)
	if got.reliability != 0 || got.proven != 0 || got.latency <= 0 || got.speed <= 0 {
		t.Fatalf("quality established reachability or lost valid metrics: %+v", got)
	}
	other := r.routeTarget(p, "b.example:443", nil, time.Now()).evidence(h)
	if other.latency != 0 || other.speed != 0 {
		t.Fatalf("quality leaked to sibling origin: %+v", other)
	}
	r.ReportTargetSuccess(route, "a.example:443")
	r.ReportTargetFailure(route, "b.example:443")
	other = r.routeTarget(p, "c.example:443", nil, time.Now()).evidence(h)
	if other.proven <= 0 || other.failed || other.cooling {
		t.Fatalf("target failure polluted node reachability: %+v", other)
	}
}

func TestTargetQuality_MissingMetricsAndStaleConnection(t *testing.T) {
	r, _, p, h := targetTestRouter(t)
	route := reportSelectedSuccess(t, r, p, h, "a.example:443")
	r.ReportTargetQuality(route, "a.example:443", 0, math.NaN())
	r.ReportTargetQuality(route, "a.example:443", -time.Second, math.Inf(1))
	got := r.routeTarget(p, "a.example:443", nil, time.Now()).evidence(h)
	if got.latency != 0 || got.speed != 0 {
		t.Fatalf("missing metrics became evidence: %+v", got)
	}
	r.ReportTargetQuality(route, "a.example:443", 30*time.Millisecond, 4*1024*1024)
	r.ReportTargetFailure(route, "a.example:443")
	r.ReportTargetQuality(route, "a.example:443", time.Millisecond, 100*1024*1024)
	got = r.routeTarget(p, "a.example:443", nil, time.Now()).evidence(h)
	if !got.failed || !got.cooling || got.latency != 0 || got.speed != 0 {
		t.Fatalf("old quality overrode new failure: %+v", got)
	}
}

func TestTargetQuality_FreshnessAndIndependentWindows(t *testing.T) {
	var cache targetHealthCache
	now := time.Now()
	window := 10 * time.Minute
	key := targetHealthKey{target: "a.example:443"}
	health := targetHealth{key: key, success: now}
	health.latency.observe(float64(10*time.Millisecond), now, window)
	health.speed.observe(8*1024*1024, now, passiveSpeedWindow)
	cache.put(health)
	fresh := cache.evidence(key, now, window)
	aged := cache.evidence(key, now.Add(window/2), window)
	if aged.reliability >= fresh.reliability || aged.reliability <= 0 || aged.latency <= fresh.latency || aged.speed >= fresh.speed {
		t.Fatalf("evidence did not decay toward priors: fresh=%+v aged=%+v", fresh, aged)
	}
	expired := cache.evidence(key, now.Add(window+time.Second), window)
	if expired.reliability != 0 || expired.latency != 0 || expired.speed <= 0 {
		t.Fatalf("independent quality windows lost: %+v", expired)
	}
	expired = cache.evidence(key, now.Add(passiveSpeedWindow+time.Second), window)
	if expired != (targetEvidence{}) {
		t.Fatalf("expired quality remained: %+v", expired)
	}
}

func TestTargetQuality_PerformanceWinsWithoutSuccessCountTiers(t *testing.T) {
	r, pool, p, fast := targetTestRouter(t)
	p.AllocationPolicy = platform.AllocationPolicyPreferLowLatency
	slow, e := newRoutableEntry(t, `{"quality-slow":true}`, "203.0.113.2")
	pool.addEntry(slow, e)
	pool.rebuildPlatformView(p)
	for _, h := range []node.Hash{fast, slow} {
		route := reportSelectedSuccess(t, r, p, h, "a.example:443")
		latency, speed := 10*time.Millisecond, float64(8*1024*1024)
		if h == slow {
			latency, speed = time.Second, 128*1024
		}
		r.ReportTargetQuality(route, "a.example:443", latency, speed)
	}
	for i := 0; i < 10; i++ {
		reportSelectedSuccess(t, r, p, slow, "a.example:443")
	}
	seen := map[node.Hash]int{}
	for i := 0; i < 1024; i++ {
		route, err := r.RouteRequest(p.Name, "", "a.example:443")
		if err != nil {
			t.Fatal(err)
		}
		seen[route.NodeHash]++
	}
	if seen[fast] < 900 || seen[slow] == 0 {
		t.Fatalf("response quality lost to repeated successes: %+v", seen)
	}
}

func TestExplorationBudget_NewRequestsOnlyAndBounded(t *testing.T) {
	r, _, p, h := targetTestRouter(t)
	now := time.Now()
	for i := 0; i < explorationRequestCost-1; i++ {
		if r.routeTarget(p, "a.example:443", nil, now).exploration() {
			t.Fatal("trial before earned request budget")
		}
	}
	for i := 0; i < 100; i++ {
		if r.routeTarget(p, "a.example:443", []node.Hash{h}, now).exploration() {
			t.Fatal("retry explored")
		}
	}
	target := r.routeTarget(p, "a.example:443", nil, now)
	if !target.exploration() || target.exploration() {
		t.Fatal("new request budget not consumed exactly once")
	}
	for i := 0; i < 1000; i++ {
		r.routeTarget(p, "a.example:443", nil, now)
	}
	key := targetHealthKey{platformID: p.ID, target: "a.example:443"}
	for i := 0; i < 2; i++ {
		if !r.targetHealth.spendExploration(key, now, r.p2cWindow()) {
			t.Fatal("bounded bank missing")
		}
	}
	if r.targetHealth.spendExploration(key, now, r.p2cWindow()) {
		t.Fatal("exploration bank exceeded cap")
	}
	if r.routeTarget(p, "b.example:443", nil, now).exploration() {
		t.Fatal("budget leaked across target")
	}
}

func TestExplorationBudget_TrialsHonorFiltersAndBlacklist(t *testing.T) {
	r, pool, p, good := targetTestRouter(t)
	reportSelectedSuccess(t, r, p, good, "a.example:443")
	trial, e := newRoutableEntry(t, `{"trial":true}`, "203.0.113.2")
	pool.addEntry(trial, e)
	pool.rebuildPlatformView(p)
	e.BlacklistUntil.Store(time.Now().Add(time.Hour).UnixNano())
	for i := 0; i < 2*explorationRequestCost; i++ {
		route, err := r.RouteRequest(p.Name, "", "a.example:443")
		if err != nil || route.NodeHash != good {
			t.Fatalf("trial bypassed blacklist: %+v %v", route, err)
		}
	}
	e.BlacklistUntil.Store(0)
	target := r.routeTarget(p, "a.example:443", nil, time.Now())
	allows := func(h node.Hash) bool { return h != trial && target.allows(h) }
	got, err := randomRoute(p, NewIPLoadStats(), pool, target.domain, r.authorities(), r.p2cWindow(), allows, target.evidence, target.exploration)
	if err != nil || got != good {
		t.Fatalf("trial bypassed routing filter: %v %v", got, err)
	}
}

func TestTargetReliability_RecoveryRetainsAndDecaysFailures(t *testing.T) {
	var cache targetHealthCache
	key := targetHealthKey{target: "flaky.example:443"}
	start := time.Now()
	window := 30 * time.Minute
	cache.failure(key, start, window)
	cache.failure(key, start.Add(time.Minute), window)
	recoveredAt := start.Add(12 * time.Minute)
	if !cache.success(key, recoveredAt, recoveredAt, window) {
		t.Fatal("recovery rejected after cooldown")
	}
	recovered := cache.evidence(key, recoveredAt, window)
	if recovered.cooling || recovered.failed || recovered.reliability <= 0 || recovered.reliability >= 0.9 {
		t.Fatalf("one recovery erased failure memory: %+v", recovered)
	}
	// Compare at a fresh successful response in each case: older failures must
	// carry less weight rather than disappear when backoff resets.
	var early, late targetResponseEvidence
	early.observe(false, start, window)
	late = early
	early.observe(true, start.Add(6*time.Minute), window)
	late.observe(true, start.Add(20*time.Minute), window)
	if early.reliability(start.Add(6*time.Minute), window) >= late.reliability(start.Add(20*time.Minute), window) {
		t.Fatal("older failure did not fade toward neutral")
	}
	if early.reliability(start.Add(31*time.Minute), window) >= early.reliability(start.Add(37*time.Minute), window) {
		t.Fatal("retained failure weight did not decay between observations")
	}
	// More accepted responses gradually repair the historical failure ratio.
	nextAt := recoveredAt.Add(time.Second)
	cache.success(key, nextAt, nextAt, window)
	if next := cache.evidence(key, nextAt, window); next.reliability <= recovered.reliability || next.reliability >= 1 {
		t.Fatalf("recovery did not improve reliability gradually: before=%+v after=%+v", recovered, next)
	}
}

func TestTargetReliability_NoFailureSampleCountBonus(t *testing.T) {
	var once, many targetResponseEvidence
	now := time.Now()
	window := 10 * time.Minute
	once.observe(true, now, window)
	for i := 0; i < 100; i++ {
		many.observe(true, now, window)
	}
	for _, at := range []time.Time{now, now.Add(window / 2)} {
		if once.reliability(at, window) != 1 || many.reliability(at, window) != 1 {
			t.Fatal("success volume created a reliability advantage")
		}
	}
}

func TestTargetReliability_ExactHistoryNotMaskedByOtherTargetSuccess(t *testing.T) {
	r, pool, p, h := targetTestRouter(t)
	p.AllocationPolicy = platform.AllocationPolicyPreferLowLatency
	now := time.Now()
	key := targetHealthKey{p.ID, h, "flaky.example:443"}
	r.targetHealth.failure(key, now.Add(-6*time.Minute), r.p2cWindow())
	route := reportSelectedSuccess(t, r, p, h, "flaky.example:443")
	r.ReportTargetSuccess(route, "healthy.example:443")
	health := r.routeTarget(p, "flaky.example:443", nil, time.Now()).evidence(h)
	if health.reliability <= 0 || health.reliability >= health.proven {
		t.Fatalf("exact failure history lost: %+v", health)
	}
	entry, _ := pool.GetEntry(h)
	stats := NewIPLoadStats()
	degraded := connectionScore(h, entry, 50*time.Millisecond, health, p, stats, pool, now)
	health.reliability = 1
	healthy := connectionScore(h, entry, 50*time.Millisecond, health, p, stats, pool, now)
	if degraded <= healthy {
		t.Fatalf("global positive evidence masked exact failure history: degraded=%g healthy=%g", degraded, healthy)
	}
}

func TestTargetReliability_RestoredFailureSurvivesRecovery(t *testing.T) {
	r, _, p, h := targetTestRouter(t)
	now := time.Now()
	r.RestoreTargetHistory([]TargetObservation{
		{PlatformID: p.ID, NodeHash: h, SelectedAt: now.Add(-7 * time.Minute), ObservedAt: now.Add(-6 * time.Minute), Target: "flaky.example:443"},
		{PlatformID: p.ID, NodeHash: h, SelectedAt: now.Add(-time.Minute), ObservedAt: now.Add(-30 * time.Second), Target: "flaky.example:443", Success: true},
	})
	got := r.routeTarget(p, "flaky.example:443", nil, now).evidence(h)
	if got.cooling || got.failed || got.reliability <= 0 || got.reliability >= 0.9 {
		t.Fatalf("restored recovery discarded failure history: %+v", got)
	}
}
