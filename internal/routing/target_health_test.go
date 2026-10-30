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
	key := targetFailureKey{p.ID, h, "api.github.com:443"}
	r.targetFailures.add(key, time.Now().Add(-targetFailureCooldown))
	if _, err := r.RouteRequest(p.Name, "", "api.github.com:443"); err != nil {
		t.Fatalf("expired cooldown: %v", err)
	}
	now := time.Now()
	for i := 0; i <= maxTargetFailures; i++ {
		r.targetFailures.add(targetFailureKey{p.ID, h, fmt.Sprintf("%d.example.com:443", i)}, now)
	}
	if len(r.targetFailures.entries) != maxTargetFailures || r.targetFailures.order.Len() != maxTargetFailures {
		t.Fatal("target cache exceeded capacity")
	}
	if r.targetFailures.contains(targetFailureKey{p.ID, h, "0.example.com:443"}, now) {
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
