package routing

import (
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

var ErrNoAvailableNodes = errors.New("no available nodes")

var randomRouteRNGPool = sync.Pool{
	New: func() any {
		return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	},
}

// randomRoute chooses among nodes with the strongest recent reachability
// evidence: target observations first, authority probes second, unknown last.
// Anonymous requests have no lease load, so random P2C otherwise keeps sending
// traffic to untested nodes even while known-working target routes exist.
func randomRoute(
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
	targetDomain string,
	authorities []string,
	p2cWindow time.Duration,
	allows func(node.Hash) bool,
) (node.Hash, error) {
	view := plat.View()
	size := view.Size()
	if size == 0 {
		return node.Zero, ErrNoAvailableNodes
	}
	rng := randomRouteRNGPool.Get().(*rand.Rand)
	defer randomRouteRNGPool.Put(rng)
	now := time.Now()
	bestTier := -1
	bestScore := 0.0
	ties := 0
	selected := node.Zero
	view.Range(func(h node.Hash) bool {
		if allows != nil && !allows(h) {
			return true
		}
		// Preserve the single-node fast path. The caller rechecks liveness.
		if size == 1 {
			selected = h
			return false
		}
		entry, ok := pool.GetEntry(h)
		if !ok {
			return true
		}
		tier := 0
		latency, measured := lookupRecentDomainLatency(entry, targetDomain, now, p2cWindow)
		if measured {
			tier = 2
		} else if latency, measured = averageRecentAuthorityLatency(entry, authorities, now, p2cWindow); measured {
			tier = 1
		}
		score := calculateScore(h, latency, plat, stats, pool)
		if tier > bestTier || (tier == bestTier && score < bestScore) {
			bestTier, bestScore, ties, selected = tier, score, 1, h
		} else if tier == bestTier && score == bestScore {
			ties++
			if rng.IntN(ties) == 0 {
				selected = h
			}
		}
		return true
	})
	if selected == node.Zero {
		return node.Zero, ErrNoAvailableNodes
	}
	return selected, nil
}

func isRecent(t time.Time, now time.Time, window time.Duration) bool {
	return now.Sub(t) <= window
}

// calculateScore computes the score for a node based on platform allocation policy.
// Lower is better.
func calculateScore(
	h node.Hash,
	latency time.Duration,
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
) float64 {
	entry, _ := pool.GetEntry(h)
	// If entry is nil (race), treat as high load/latency?
	// But we hold ref via pool, only deletion removes it.
	// Assuming existence since we just picked it from view.

	// Lease count from stats.
	var leaseCount int64
	if entry != nil {
		ip := entry.GetEgressIP()
		if ip.IsValid() {
			leaseCount = stats.Get(ip)
		}
	}

	// If latency is 0 (empty/incompatible), score = LeaseCount strictly.
	if latency <= 0 {
		return float64(leaseCount)
	}

	// Policy-based scoring.
	switch plat.AllocationPolicy {
	case platform.AllocationPolicyPreferLowLatency:
		return float64(latency)
	case platform.AllocationPolicyPreferIdleIP:
		return float64(leaseCount)
	case platform.AllocationPolicyBalanced:
		fallthrough
	default:
		// (LeaseCount + 1) * Latency
		return float64(leaseCount+1) * float64(latency)
	}
}
