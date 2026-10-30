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
)

type targetHealthKey struct {
	platformID string
	node       node.Hash
	target     string
}

type targetHealth struct {
	key         targetHealthKey
	lastFailure time.Time
	until       time.Time
	failures    int
	// The latest two responses suffice to distinguish recent stable success.
	success         time.Time
	previousSuccess time.Time
}

type targetEvidence struct {
	successes int
	failed    bool
	cooling   bool
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
	if failureExpired && successExpired {
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
	health.failures = min(health.failures+1, 4)
	cooldown := targetFailureCooldown
	for i := 1; i < health.failures && cooldown < maxTargetFailureCooldown; i++ {
		cooldown = min(cooldown*2, maxTargetFailureCooldown)
	}
	health.lastFailure = now
	health.until = now.Add(cooldown)
	health.success, health.previousSuccess = time.Time{}, time.Time{}
	c.put(health)
}

func (c *targetHealthCache) success(key targetHealthKey, selectedAt, now time.Time, window time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	health := c.get(key, now, window)
	// A response from a connection picked before the latest failure cannot
	// rehabilitate this origin, even if that connection outlives the cooldown.
	if selectedAt.IsZero() || !selectedAt.After(health.lastFailure) || now.Before(health.until) {
		return
	}
	health.previousSuccess, health.success = health.success, now
	health.failures = 0
	c.put(health)
}

func (c *targetHealthCache) evidence(key targetHealthKey, now time.Time, window time.Duration) targetEvidence {
	c.mu.Lock()
	defer c.mu.Unlock()
	health := c.get(key, now, window)
	evidence := targetEvidence{failed: !health.lastFailure.IsZero() && health.success.IsZero(), cooling: now.Before(health.until)}
	if !health.success.IsZero() && isRecent(health.success, now, window) {
		evidence.successes = 1
		if !health.previousSuccess.IsZero() && isRecent(health.previousSuccess, now, window) {
			evidence.successes = 2
		}
	}
	return evidence
}

// ReportTargetFailure excludes an exact origin with exponential cooldown.
// Only upstream failures belong here, not cancellation or local response errors.
func (r *Router) ReportTargetFailure(route RouteResult, target string) {
	plat, ok := r.pool.GetPlatform(route.PlatformID)
	if !ok || plat.PassiveCircuitBreakerDisabled {
		return
	}
	r.targetHealth.failure(targetHealthKey{route.PlatformID, route.NodeHash, normalizeRouteTarget(target)}, time.Now(), r.p2cWindow())
}

// ReportTargetSuccess records an actual target response, not a successful dial
// or an authority probe. It must be called only once for a connection's first
// valid response; it does not imply full TLS handshake validation.
func (r *Router) ReportTargetSuccess(route RouteResult, target string) {
	if _, ok := r.pool.GetPlatform(route.PlatformID); !ok {
		return
	}
	r.targetHealth.success(targetHealthKey{route.PlatformID, route.NodeHash, normalizeRouteTarget(target)}, route.selectedAt, time.Now(), r.p2cWindow())
}

type routeTarget struct {
	domain   string
	allows   func(node.Hash) bool
	evidence func(node.Hash) targetEvidence
}

func (r *Router) routeTarget(plat *platform.Platform, target string, excluded []node.Hash, now time.Time) routeTarget {
	address := normalizeRouteTarget(target)
	window := r.p2cWindow()
	evidence := func(h node.Hash) targetEvidence {
		result := r.targetHealth.evidence(targetHealthKey{plat.ID, h, address}, now, window)
		if plat.PassiveCircuitBreakerDisabled {
			result.failed, result.cooling = false, false
		}
		return result
	}
	return routeTarget{
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
