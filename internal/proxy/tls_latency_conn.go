package proxy

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// tlsLatencyConn observes a bounded prefix without terminating TLS or retaining
// application data. For TLS requests, only a ServerHello prefix is positive
// first-response evidence; alerts and wrong-protocol replies are not. This is
// not certificate verification or proof of a completed TLS handshake.
type tlsLatencyConn struct {
	net.Conn
	startTime                  atomic.Int64
	reported                   atomic.Bool
	dialElapsed                time.Duration
	onLatency                  func(time.Duration)
	mu                         sync.Mutex
	clientPrefix, serverPrefix [6]byte
	clientN, serverN           int
	readErr, writeErr          error
}

func newTLSLatencyConn(c net.Conn, dialElapsed time.Duration, onLatency func(time.Duration)) *tlsLatencyConn {
	return &tlsLatencyConn{Conn: c, dialElapsed: dialElapsed, onLatency: onLatency}
}

func appendTunnelPrefix(prefix *[6]byte, n *int, p []byte) {
	*n += copy(prefix[*n:], p)
}

// tlsHelloPrefix checks the TLS record header and first handshake type, not
// the encrypted handshake or certificate. Prefixes may span arbitrary IO calls.
func tlsHelloPrefix(p [6]byte, n int, kind byte) bool {
	return n == len(p) && p[0] == 22 && p[1] == 3 && p[2] <= 4 && (int(p[3])<<8|int(p[4])) >= 4 && p[5] == kind
}

func (c *tlsLatencyConn) Write(p []byte) (int, error) {
	if len(p) > 0 {
		c.startTime.CompareAndSwap(0, time.Now().UnixNano())
		c.mu.Lock()
		appendTunnelPrefix(&c.clientPrefix, &c.clientN, p)
		c.mu.Unlock()
	}
	n, err := c.Conn.Write(p)
	if err != nil {
		c.mu.Lock()
		c.writeErr = err
		c.mu.Unlock()
	}
	return n, err
}

func (c *tlsLatencyConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.mu.Lock()
	appendTunnelPrefix(&c.serverPrefix, &c.serverN, p[:n])
	if err != nil {
		c.readErr = err
	}
	// A fragmented, TLS-looking ClientHello must be classified before recording
	// any response. Non-TLS tunnels retain ordinary first-response measurement.
	classified := c.clientN > 0 && (c.clientPrefix[0] != 22 || c.clientN == len(c.clientPrefix))
	tlsRequest := tlsHelloPrefix(c.clientPrefix, c.clientN, 1)
	valid := classified && c.serverN > 0 && (!tlsRequest || tlsHelloPrefix(c.serverPrefix, c.serverN, 2))
	c.mu.Unlock()
	if valid {
		if start := c.startTime.Load(); start > 0 && c.reported.CompareAndSwap(false, true) {
			latency := c.dialElapsed + time.Duration(time.Now().UnixNano()-start)
			if c.onLatency != nil {
				c.onLatency(latency)
			}
		}
	}
	return n, err
}

type tunnelObservation struct {
	tlsRequest, invalidResponse, truncatedResponse bool
	readErr, writeErr                              error
}

func (c *tlsLatencyConn) observation() tunnelObservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	tlsRequest := tlsHelloPrefix(c.clientPrefix, c.clientN, 1)
	invalid := tlsRequest && c.serverN > 0 && !tlsHelloPrefix(c.serverPrefix, c.serverN, 2) &&
		(c.serverN == len(c.serverPrefix) || c.serverPrefix[0] != 22)
	return tunnelObservation{
		tlsRequest: tlsRequest, invalidResponse: invalid,
		truncatedResponse: tlsRequest && c.serverN > 0 && c.serverN < len(c.serverPrefix) && c.readErr != nil,
		readErr:           c.readErr, writeErr: c.writeErr,
	}
}

func (c *tlsLatencyConn) CloseWrite() error { return closeWriteErr(c.Conn) }
func (c *tlsLatencyConn) CloseRead() error  { return closeReadErr(c.Conn) }
