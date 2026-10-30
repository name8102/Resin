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

const (
	preferredCandidates       = 3
	maxRankedCandidates       = 12
	candidateExplorationEvery = 32
	passiveSpeedWindow        = 30 * time.Minute
)

type routeCandidate struct {
	hash  node.Hash
	tier  int
	score float64
}

func betterCandidate(a, b routeCandidate) bool {
	if a.tier != b.tier {
		return a.tier > b.tier
	}
	if a.score != b.score {
		return a.score < b.score
	}
	// Cold-start ties are ranked deterministically, not sampled from the pool.
	for i := range a.hash {
		if a.hash[i] != b.hash[i] {
			return a.hash[i] < b.hash[i]
		}
	}
	return false
}

// randomRoute ranks the pool by real reachability before considering speed.
// Most new connections share the three best proven candidates with score-based
// weights. A 1/32 trial uses the next ranked, verified candidates; unknown or
// failed origins never displace a working shortlist merely to explore.
func randomRoute(
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
	targetDomain string,
	authorities []string,
	p2cWindow time.Duration,
	allows func(node.Hash) bool,
	evidence func(node.Hash) targetEvidence,
) (node.Hash, error) {
	view := plat.View()
	if view.Size() == 0 {
		return node.Zero, ErrNoAvailableNodes
	}
	rng := randomRouteRNGPool.Get().(*rand.Rand)
	defer randomRouteRNGPool.Put(rng)
	now := time.Now()
	var ranked [maxRankedCandidates]routeCandidate
	count := 0
	view.Range(func(h node.Hash) bool {
		if allows != nil && !allows(h) {
			return true
		}
		// The caller rechecks a singleton for transient deletion, as before.
		if view.Size() == 1 {
			ranked[0] = routeCandidate{hash: h}
			count = 1
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
		if evidence != nil {
			health := evidence(h)
			if health.proven > 0 {
				tier = 3
			}
			if health.successes > 0 {
				tier = 3 + health.successes
			}
			if health.failed {
				tier = -1
			}
		}
		candidate := routeCandidate{h, tier, connectionScore(h, entry, latency, plat, stats, pool, now)}
		i := count
		for i > 0 && betterCandidate(candidate, ranked[i-1]) {
			i--
		}
		if i < len(ranked) {
			if count < len(ranked) {
				count++
			}
			copy(ranked[i+1:count], ranked[i:count-1])
			ranked[i] = candidate
		}
		return true
	})
	if count == 0 {
		return node.Zero, ErrNoAvailableNodes
	}
	if ranked[0].tier < 3 {
		return ranked[0].hash, nil
	}
	primary := 1
	for primary < min(count, preferredCandidates) && ranked[primary].tier == ranked[0].tier {
		primary++
	}
	if primary < count && ranked[primary].tier >= 1 && rng.IntN(candidateExplorationEvery) == 0 {
		end := primary
		for end < count && end < primary+preferredCandidates && ranked[end].tier == ranked[primary].tier {
			end++
		}
		return weightedCandidate(ranked[primary:end], rng.Float64()), nil
	}
	return weightedCandidate(ranked[:primary], rng.Float64()), nil
}

func weightedCandidate(candidates []routeCandidate, ticket float64) node.Hash {
	best := max(candidates[0].score, 1)
	total := 0.0
	for _, c := range candidates {
		total += max(0.05, best/max(c.score, 1))
	}
	ticket *= total
	for _, c := range candidates {
		ticket -= max(0.05, best/max(c.score, 1))
		if ticket < 0 {
			return c.hash
		}
	}
	return candidates[len(candidates)-1].hash
}

// Estimate the cost of a representative 128KiB response from passive windows.
// Unknown throughput gets a neutral 1MiB/s prior, not zero-cost "infinite speed".
// Confidence is ranked separately, so a fast probe never beats real responses.
func connectionScore(h node.Hash, entry *node.NodeEntry, latency time.Duration, plat *platform.Platform, stats *IPLoadStats, pool PoolAccessor, now time.Time) float64 {
	if latency <= 0 {
		latency = 750 * time.Millisecond
	}
	speed, measured := entry.Traffic.DownloadSpeed(now, passiveSpeedWindow)
	if !measured {
		speed = 1024 * 1024
	}
	transfer := time.Duration(float64(128*1024) / max(speed, 1) * float64(time.Second))
	cost := latency + min(transfer, 30*time.Second)
	if plat.AllocationPolicy == platform.AllocationPolicyPreferIdleIP {
		base := calculateScore(h, cost, plat, stats, pool)
		seconds := cost.Seconds()
		return base + seconds/(1+seconds)
	}
	return calculateScore(h, cost, plat, stats, pool)
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
