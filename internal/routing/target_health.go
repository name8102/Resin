package routing

import (
	"container/list"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

const (
	targetFailureCooldown    = 5 * time.Minute
	maxTargetFailureCooldown = 30 * time.Minute
	targetFailureRetention   = 30 * time.Minute
	maxTargetHealthEntries   = 8192
	knownNodeWindow          = 30 * time.Minute
)

type targetHealthKey struct {
	platformID string
	node       node.Hash
	target     string
}

type targetHealth struct {
	key               targetHealthKey
	lastFailure       time.Time
	until             time.Time
	failures          int
	success           time.Time
	responses         targetResponseEvidence
	latency           targetQualitySample
	speed             targetQualitySample
	explorationAt     time.Time
	explorationCredit int
}

type targetEvidence struct {
	reliability float64
	proven      float64 // fresh real responses on this platform, across exact origins
	latency     time.Duration
	speed       float64
	failed      bool
	cooling     bool
}

// Exact-origin health is bounded and independent of global probe health.
// Authority probes and sibling-domain latency cannot reopen a failed origin.
type targetHealthCache struct {
	mu      sync.Mutex
	entries map[targetHealthKey]*list.Element
	order   list.List
}

func (c *targetHealthCache) get(key targetHealthKey, now time.Time, window time.Duration) targetHealth {
	e := c.entries[key]
	if e == nil {
		return targetHealth{key: key}
	}
	health := e.Value.(targetHealth)
	failureExpired := health.lastFailure.IsZero() || !now.Before(health.until.Add(targetFailureRetention))
	successExpired := health.success.IsZero() || !isRecent(health.success, now, window)
	qualityExpired := health.latency.freshness(now, window) == 0 && health.speed.freshness(now, passiveSpeedWindow) == 0
	explorationExpired := health.explorationAt.IsZero() || !isRecent(health.explorationAt, now, knownNodeWindow)
	if failureExpired && successExpired && qualityExpired && explorationExpired {
		delete(c.entries, key)
		c.order.Remove(e)
		return targetHealth{key: key}
	}
	return health
}

func (c *targetHealthCache) put(health targetHealth) {
	if c.entries == nil {
		c.entries = make(map[targetHealthKey]*list.Element)
	}
	if e := c.entries[health.key]; e != nil {
		e.Value = health
		c.order.MoveToBack(e)
		return
	}
	if len(c.entries) >= maxTargetHealthEntries {
		oldest := c.order.Front()
		delete(c.entries, oldest.Value.(targetHealth).key)
		c.order.Remove(oldest)
	}
	c.entries[health.key] = c.order.PushBack(health)
}

func (c *targetHealthCache) failure(key targetHealthKey, now time.Time, window time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	health := c.get(key, now, window)
	health.responses.observe(false, now, window)
	health.failures = min(health.failures+1, 4)
	cooldown := targetFailureCooldown
	for i := 1; i < health.failures && cooldown < maxTargetFailureCooldown; i++ {
		cooldown = min(cooldown*2, maxTargetFailureCooldown)
	}
	health.lastFailure = now
	health.until = now.Add(cooldown)
	health.success = time.Time{}
	health.latency, health.speed = targetQualitySample{}, targetQualitySample{}
	c.put(health)
}

func (c *targetHealthCache) success(key targetHealthKey, selectedAt, now time.Time, window time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	health := c.get(key, now, window)
	// A response from a connection picked before the latest failure cannot
	// rehabilitate this origin, even if that connection outlives the cooldown.
	if selectedAt.IsZero() || !selectedAt.After(health.lastFailure) || now.Before(health.until) {
		return false
	}
	health.responses.observe(true, now, window)
	health.success = now
	health.failures = 0
	c.put(health)
	return true
}

func (c *targetHealthCache) evidence(key targetHealthKey, now time.Time, window time.Duration) targetEvidence {
	c.mu.Lock()
	defer c.mu.Unlock()
	health := c.get(key, now, window)
	evidence := targetEvidence{failed: !health.lastFailure.IsZero() && health.success.IsZero(), cooling: now.Before(health.until)}
	if !health.success.IsZero() && isRecent(health.success, now, window) {
		evidence.reliability = evidenceFreshness(health.success, now, window) * health.responses.reliability(now, window)
	}
	evidence.latency = time.Duration(health.latency.estimate(now, window, float64(defaultResponseLatency)))
	evidence.speed = health.speed.estimate(now, passiveSpeedWindow, defaultDownloadSpeed)
	return evidence
}

// ReportTargetFailure excludes an exact origin with exponential cooldown.
// Only upstream failures belong here, not cancellation or local response errors.
func (r *Router) ReportTargetFailure(route RouteResult, target string) {
	plat, ok := r.pool.GetPlatform(route.PlatformID)
	if !ok || plat.PassiveCircuitBreakerDisabled {
		return
	}
	now := time.Now()
	r.targetHealth.failure(targetHealthKey{route.PlatformID, route.NodeHash, normalizeRouteTarget(target)}, now, r.p2cWindow())
}

// ReportTargetSuccess records an actual target response, not a successful dial
// or an authority probe. It must be called only once for a connection's first
// valid response; it does not imply full TLS handshake validation.
func (r *Router) ReportTargetSuccess(route RouteResult, target string) {
	if _, ok := r.pool.GetPlatform(route.PlatformID); !ok {
		return
	}
	now := time.Now()
	accepted := r.targetHealth.success(targetHealthKey{route.PlatformID, route.NodeHash, normalizeRouteTarget(target)}, route.selectedAt, now, r.p2cWindow())
	if accepted && contributesNodeEvidence(target) {
		r.targetHealth.success(targetHealthKey{route.PlatformID, route.NodeHash, ""}, route.selectedAt, now, knownNodeWindow)
	}
}

type routeTarget struct {
	domain      string
	allows      func(node.Hash) bool
	evidence    func(node.Hash) targetEvidence
	exploration func() bool
}

func (r *Router) routeTarget(plat *platform.Platform, target string, excluded []node.Hash, now time.Time) routeTarget {
	address := normalizeRouteTarget(target)
	window := r.p2cWindow()
	evidence := func(h node.Hash) targetEvidence {
		result := r.targetHealth.evidence(targetHealthKey{plat.ID, h, address}, now, window)
		result.proven = r.targetHealth.evidence(targetHealthKey{plat.ID, h, ""}, now, knownNodeWindow).reliability
		if plat.PassiveCircuitBreakerDisabled {
			result.failed, result.cooling = false, false
		}
		return result
	}
	// The budget belongs to the destination, not a node. Retries do not mint
	// credits or trigger trials, and sticky hits never reassign their lease.
	budgetKey := targetHealthKey{platformID: plat.ID, target: address}
	if len(excluded) == 0 {
		r.targetHealth.earnExploration(budgetKey, now, window)
	}
	var explored bool
	return routeTarget{
		exploration: func() bool {
			if explored || len(excluded) != 0 {
				return false
			}
			explored = true
			return r.targetHealth.spendExploration(budgetKey, now, window)
		},
		domain: netutil.ExtractDomain(target),
		allows: func(h node.Hash) bool {
			if slices.Contains(excluded, h) {
				return false
			}
			if entry, ok := r.pool.GetEntry(h); ok && entry.BlacklistUntil.Load() != 0 {
				return false
			}
			return !evidence(h).cooling
		},
		evidence: evidence,
	}
}

// Unlike latency scoring's eTLD+1 key, health must distinguish subdomains and
// ports. Plain HTTP hosts default to 80; URL targets retain their scheme's port.
func normalizeRouteTarget(target string) string {
	defaultPort := "80"
	if strings.Contains(target, "://") {
		if u, err := url.Parse(target); err == nil && u.Host != "" {
			target = u.Host
			if u.Scheme == "https" {
				defaultPort = "443"
			}
		}
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		host = strings.Trim(target, "[]")
		port = defaultPort
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	return net.JoinHostPort(host, port)
}

// Syncthing peer reachability does not establish or revoke web/Git confidence.
func contributesNodeEvidence(target string) bool {
	_, port, _ := net.SplitHostPort(normalizeRouteTarget(target))
	return port != "22000" && port != "22067"
}
