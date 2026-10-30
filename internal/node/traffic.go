package node

import (
	"sync"
	"time"
)

const (
	minDownloadSampleBytes = 64 * 1024
	minDownloadSampleTime  = 100 * time.Millisecond
	downloadSampleExpiry   = 30 * time.Minute
	downloadSpeedAlpha     = 0.25
)

// TrafficStats holds passive download throughput from real proxied traffic.
// Its zero value is ready for concurrent use.
type TrafficStats struct {
	mu               sync.RWMutex
	downloadSpeed    float64
	lastDownloadTime time.Time
}

// ObserveDownload records an active download window, ignoring tiny transfers.
func (s *TrafficStats) ObserveDownload(bytes int64, active time.Duration, now time.Time) {
	if bytes < minDownloadSampleBytes || active < minDownloadSampleTime {
		return
	}
	speed := float64(bytes) / active.Seconds()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastDownloadTime.IsZero() || now.Sub(s.lastDownloadTime) > downloadSampleExpiry {
		s.downloadSpeed = speed
	} else {
		s.downloadSpeed += downloadSpeedAlpha * (speed - s.downloadSpeed)
	}
	s.lastDownloadTime = now
}

// DownloadSpeed returns bytes per second while the latest sample is fresh.
func (s *TrafficStats) DownloadSpeed(now time.Time, window time.Duration) (bytesPerSecond float64, measured bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastDownloadTime.IsZero() || now.Sub(s.lastDownloadTime) > window {
		return 0, false
	}
	return s.downloadSpeed, true
}
