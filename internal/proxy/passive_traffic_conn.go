package proxy

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
)

const passiveDownloadIdleGap = 2 * time.Second

// downloadSampler measures active windows, excluding idle gaps and close time.
// Connections and HTTP response bodies share the same sampling rules.
type downloadSampler struct {
	now      func() time.Time
	onSample func(int64, time.Duration, time.Time)
	mu       sync.Mutex
	started  time.Time
	last     time.Time
	bytes    int64
	closed   bool
}

func (s *downloadSampler) observe(n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	now := s.now()
	if s.started.IsZero() || now.Sub(s.last) > passiveDownloadIdleGap {
		s.started = now
		s.bytes = 0
	}
	s.last = now
	s.bytes += int64(n)
	if s.bytes >= 64*1024 && now.Sub(s.started) >= 100*time.Millisecond {
		s.onSample(s.bytes, now.Sub(s.started), now)
		s.started = time.Time{}
		s.bytes = 0
	}
}

func (s *downloadSampler) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if active := s.last.Sub(s.started); s.bytes >= 64*1024 && active >= 100*time.Millisecond {
		s.onSample(s.bytes, active, s.last)
	}
}

// passiveTrafficConn measures live node-wide and optional target-specific rates.
type passiveTrafficConn struct {
	net.Conn
	entry *node.NodeEntry
	downloadSampler
}

func newPassiveTrafficConn(conn net.Conn, entry *node.NodeEntry, onDownload func(float64)) net.Conn {
	if entry == nil && onDownload == nil {
		return conn
	}
	return &passiveTrafficConn{Conn: conn, entry: entry, downloadSampler: downloadSampler{
		now: time.Now,
		onSample: func(bytes int64, active time.Duration, at time.Time) {
			if entry != nil {
				entry.Traffic.ObserveDownload(bytes, active, at)
			}
			if onDownload != nil {
				onDownload(float64(bytes) / active.Seconds())
			}
		},
	}}
}

func passiveTrafficWrapper(pool outbound.PoolAccessor) func(node.Hash, net.Conn) net.Conn {
	return func(hash node.Hash, conn net.Conn) net.Conn {
		if pool == nil {
			return conn
		}
		entry, _ := pool.GetEntry(hash)
		return newPassiveTrafficConn(conn, entry, nil)
	}
}

func samplesPassiveTraffic(target string) bool {
	_, port, _ := net.SplitHostPort(target)
	return port != "22000" && port != "22067"
}

func (c *passiveTrafficConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.observe(n)
	return n, err
}

func (c *passiveTrafficConn) Close() error {
	// Release a concurrent blocked Read before taking the sampling lock.
	err := c.Conn.Close()
	c.finish()
	return err
}

func (c *passiveTrafficConn) CloseWrite() error { return closeWriteErr(c.Conn) }
func (c *passiveTrafficConn) CloseRead() error  { return closeReadErr(c.Conn) }

type sampledResponseBody struct {
	io.ReadCloser
	downloadSampler
}

func newSampledResponseBody(body io.ReadCloser, onDownload func(float64)) io.ReadCloser {
	return &sampledResponseBody{ReadCloser: body, downloadSampler: downloadSampler{
		now:      time.Now,
		onSample: func(bytes int64, active time.Duration, _ time.Time) { onDownload(float64(bytes) / active.Seconds()) },
	}}
}

func (b *sampledResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.observe(n)
	return n, err
}

func (b *sampledResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.finish()
	return err
}
