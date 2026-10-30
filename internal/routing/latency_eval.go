package routing

import (
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func lookupRecentDomainLatency(
	entry *node.NodeEntry,
	domain string,
	now time.Time,
	window time.Duration,
) (time.Duration, bool) {
	if entry == nil || entry.LatencyTable == nil || domain == "" {
		return 0, false
	}
	stats, ok := entry.LatencyTable.GetDomainStats(domain)
	if !ok || !isRecent(stats.LastUpdated, now, window) {
		return 0, false
	}
	return stats.Ewma, true
}

func averageRecentAuthorityLatency(
	entry *node.NodeEntry,
	authorities []string,
	now time.Time,
	window time.Duration,
) (time.Duration, bool) {
	if entry == nil || entry.LatencyTable == nil || len(authorities) == 0 {
		return 0, false
	}
	var sum time.Duration
	var count int64
	for _, domain := range authorities {
		latency, ok := lookupRecentDomainLatency(entry, domain, now, window)
		if !ok {
			continue
		}
		sum += latency
		count++
	}
	if count == 0 {
		return 0, false
	}
	return time.Duration(int64(sum) / count), true
}
