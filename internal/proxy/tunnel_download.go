package proxy

import (
	"sync/atomic"
	"time"
)

const (
	tunnelBulkBytes      = 256 * 1024
	tunnelProgressWindow = 5 * time.Second
	tunnelSlowBytesPerS  = 64 * 1024
)

// TLS hides HTTP response boundaries. Only look for repeated slow progress after
// a bulk transfer starts, and only count time inside upstream Reads so a slow
// downstream writer cannot make a healthy route appear slow.
type tunnelDownloadProgress struct {
	onSlow       func()
	clientWrites atomic.Uint64
	bulkBytes    int
	lastReadEnd  time.Time
	wait         time.Duration
	bytes        int
	slowWindows  int
	reported     bool
}

func (p *tunnelDownloadProgress) observe(n int, started, ended time.Time, writes uint64) {
	if n <= 0 || p.reported {
		return
	}
	elapsed := ended.Sub(started)
	if (!p.lastReadEnd.IsZero() && started.Sub(p.lastReadEnd) > passiveDownloadIdleGap) ||
		(elapsed > passiveDownloadIdleGap && writes != p.clientWrites.Load()) {
		// After backpressure or a new client flight during a quiet read,
		// restart rather than combining potentially unrelated responses.
		p.bulkBytes, p.wait, p.bytes, p.slowWindows = 0, 0, 0, 0
		elapsed = 0
	}
	p.lastReadEnd = ended
	if p.bulkBytes < tunnelBulkBytes {
		if elapsed > passiveDownloadIdleGap {
			p.bulkBytes = 0
		}
		p.bulkBytes += n
		return
	}
	p.wait += elapsed
	p.bytes += n
	if p.wait < tunnelProgressWindow {
		return
	}
	if float64(p.bytes) < tunnelSlowBytesPerS*p.wait.Seconds() {
		p.slowWindows++
	} else {
		p.slowWindows = 0
	}
	p.wait, p.bytes = 0, 0
	if p.slowWindows == 2 {
		p.reported = true
		p.onSlow()
	}
}

func (p *tunnelDownloadProgress) recentlyTransferring(now time.Time) bool {
	return p.bulkBytes >= tunnelBulkBytes && now.Sub(p.lastReadEnd) <= tunnelProgressWindow
}
