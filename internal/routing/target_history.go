package routing

import (
	"slices"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

// TargetObservation is bounded startup evidence from completed request logs.
// It restores reachability only: a request's lifetime is not an active-download
// duration, so old log byte totals must never seed throughput estimates.
type TargetObservation struct {
	PlatformID string
	NodeHash   node.Hash
	SelectedAt time.Time
	Target     string
	ObservedAt time.Time
	Success    bool
}

func (r *Router) RestoreTargetHistory(records []TargetObservation) {
	now := time.Now()
	slices.SortFunc(records, func(a, b TargetObservation) int { return a.ObservedAt.Compare(b.ObservedAt) })
	for _, record := range records {
		if record.ObservedAt.After(now) || now.Sub(record.ObservedAt) > knownNodeWindow {
			continue
		}
		plat, ok := r.pool.GetPlatform(record.PlatformID)
		if !ok {
			continue
		}
		exact := targetHealthKey{plat.ID, record.NodeHash, normalizeRouteTarget(record.Target)}
		global := targetHealthKey{plat.ID, record.NodeHash, ""}
		if record.Success {
			r.targetHealth.success(exact, record.SelectedAt, record.ObservedAt, r.p2cWindow())
			if contributesNodeEvidence(record.Target) {
				r.targetHealth.success(global, record.SelectedAt, record.ObservedAt, knownNodeWindow)
			}
		} else if !plat.PassiveCircuitBreakerDisabled {
			r.targetHealth.failure(exact, record.ObservedAt, r.p2cWindow())
			if contributesNodeEvidence(record.Target) {
				r.targetHealth.failure(global, record.ObservedAt, knownNodeWindow)
			}
		}
	}
}
