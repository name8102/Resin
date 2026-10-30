package proxy

import (
	"net"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
)

const passiveDownloadIdleGap = 2 * time.Second

// passiveTrafficConn measures active download windows without polling or probes.
type passiveTrafficConn struct {
	net.Conn
	entry *node.NodeEntry
	now   func() time.Time

	mu      sync.Mutex
	started time.Time
	last    time.Time
	bytes   int64
	closed  bool
}

func newPassiveTrafficConn(conn net.Conn, entry *node.NodeEntry) net.Conn {
	if entry == nil {
		return conn
	}
	return &passiveTrafficConn{Conn: conn, entry: entry, now: time.Now}
}

func passiveTrafficWrapper(pool outbound.PoolAccessor) func(node.Hash, net.Conn) net.Conn {
	return func(hash node.Hash, conn net.Conn) net.Conn {
		if pool == nil {
			return conn
		}
		entry, _ := pool.GetEntry(hash)
		return newPassiveTrafficConn(conn, entry)
	}
}

func samplesPassiveTraffic(target string) bool {
	_, port, _ := net.SplitHostPort(target)
	return port != "22000" && port != "22067"
}

func (c *passiveTrafficConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n <= 0 {
		return n, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return n, err
	}
	now := c.now()
	if c.started.IsZero() || now.Sub(c.last) > passiveDownloadIdleGap {
		c.started = now
		c.bytes = 0
	}
	c.last = now
	c.bytes += int64(n)
	if c.bytes >= 64*1024 && now.Sub(c.started) >= 100*time.Millisecond {
		c.entry.Traffic.ObserveDownload(c.bytes, now.Sub(c.started), now)
		c.started = time.Time{}
		c.bytes = 0
	}
	return n, err
}

func (c *passiveTrafficConn) Close() error {
	// Close the underlying connection before taking the sampling lock, so a
	// concurrent blocked Read is released even during shutdown.
	err := c.Conn.Close()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		if c.bytes > 0 {
			// Only time between positive reads is active; closing after a long
			// keep-alive idle period must not dilute the measured throughput.
			c.entry.Traffic.ObserveDownload(c.bytes, c.last.Sub(c.started), c.last)
		}
	}
	return err
}

func (c *passiveTrafficConn) CloseWrite() error { return closeWriteErr(c.Conn) }
func (c *passiveTrafficConn) CloseRead() error  { return closeReadErr(c.Conn) }
