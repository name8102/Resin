package routing

import (
	"container/list"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

const (
	targetFailureCooldown = 5 * time.Minute
	maxTargetFailures     = 8192
)

type targetFailureKey struct {
	platformID string
	node       node.Hash
	target     string
}

type targetFailure struct {
	key   targetFailureKey
	until time.Time
}

// Target failures are transient and bounded. Keeping them separate from global
// node health prevents a successful Google probe from reopening a GitHub route.
type targetFailureCache struct {
	mu      sync.Mutex
	entries map[targetFailureKey]*list.Element
	order   list.List
}

func (c *targetFailureCache) add(key targetFailureKey, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[targetFailureKey]*list.Element)
	}
	if e := c.entries[key]; e != nil {
		e.Value = targetFailure{key, now.Add(targetFailureCooldown)}
		c.order.MoveToBack(e)
		return
	}
	if len(c.entries) >= maxTargetFailures {
		oldest := c.order.Front()
		delete(c.entries, oldest.Value.(targetFailure).key)
		c.order.Remove(oldest)
	}
	c.entries[key] = c.order.PushBack(targetFailure{key, now.Add(targetFailureCooldown)})
}

func (c *targetFailureCache) contains(key targetFailureKey, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		return false
	}
	if !now.Before(e.Value.(targetFailure).until) {
		delete(c.entries, key)
		c.order.Remove(e)
		return false
	}
	return true
}

// ReportTargetFailure temporarily excludes this node for this exact host:port.
// Only actual upstream failures belong here, not client cancellation or local
// response/hijack errors. Successes never clear a newer failure; expiry permits
// recovery without being overwritten by probes or older, long-lived tunnels.
func (r *Router) ReportTargetFailure(route RouteResult, target string) {
	plat, ok := r.pool.GetPlatform(route.PlatformID)
	if !ok || plat.PassiveCircuitBreakerDisabled {
		return
	}
	r.targetFailures.add(targetFailureKey{route.PlatformID, route.NodeHash, normalizeRouteTarget(target)}, time.Now())
}

type routeTarget struct {
	domain string
	allows func(node.Hash) bool
}

func (r *Router) targetFilter(plat *platform.Platform, target string, excluded []node.Hash) func(node.Hash) bool {
	address := normalizeRouteTarget(target)
	now := time.Now()
	return func(h node.Hash) bool {
		if slices.Contains(excluded, h) {
			return false
		}
		return plat.PassiveCircuitBreakerDisabled || !r.targetFailures.contains(targetFailureKey{plat.ID, h, address}, now)
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
