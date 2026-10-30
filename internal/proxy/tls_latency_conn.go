package proxy

import (
	"net"
	"sync/atomic"
	"time"
)

// tlsLatencyConn measures connection setup plus the first request/response
// exchange. It observes byte boundaries without assuming a TLS payload, and
// excludes the time spent waiting for the client to send its first bytes.
type tlsLatencyConn struct {
	net.Conn
	startTime   atomic.Int64 // first Write start, UnixNano
	reported    atomic.Bool
	dialElapsed time.Duration
	onLatency   func(time.Duration)
}

func newTLSLatencyConn(c net.Conn, dialElapsed time.Duration, onLatency func(time.Duration)) *tlsLatencyConn {
	return &tlsLatencyConn{
		Conn:        c,
		dialElapsed: dialElapsed,
		onLatency:   onLatency,
	}
}

func (c *tlsLatencyConn) Write(b []byte) (int, error) {
	if len(b) > 0 && c.startTime.Load() == 0 {
		c.startTime.CompareAndSwap(0, time.Now().UnixNano())
	}
	return c.Conn.Write(b)
}

func (c *tlsLatencyConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	// Check after reading: the relay may start this Read before its first Write.
	if n > 0 {
		if startNano := c.startTime.Load(); startNano > 0 && c.reported.CompareAndSwap(false, true) {
			latency := c.dialElapsed + time.Duration(time.Now().UnixNano()-startNano)
			if c.onLatency != nil {
				go c.onLatency(latency)
			}
		}
	}
	return n, err
}

func (c *tlsLatencyConn) CloseWrite() error {
	return closeWriteErr(c.Conn)
}

func (c *tlsLatencyConn) CloseRead() error {
	return closeReadErr(c.Conn)
}
