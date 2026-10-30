package proxy

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// tlsLatencyConn observes bounded TLS framing without terminating TLS or
// retaining payloads. TLS latency requires bidirectional progress after the
// ServerHello: a subsequent client flight followed by more upstream data.
// This is not certificate verification or proof of a completed TLS handshake.
type tlsLatencyConn struct {
	net.Conn
	startTime                  atomic.Int64
	reported                   atomic.Bool
	dialElapsed                time.Duration
	onLatency                  func(time.Duration)
	handshakeTimeout           time.Duration
	mu                         sync.Mutex
	clientPrefix, serverPrefix [6]byte
	clientN, serverN           int
	serverRecords              tlsRecordObserver
	serverProgress             uint64
	clientFlight               bool
	handshakeComplete          bool
	readErr, writeErr          error
}

// tlsRecordObserver skips payloads using their record lengths, so a payload
// byte resembling an alert is never mistaken for a new record header.
type tlsRecordObserver struct {
	header    [5]byte
	headerN   int
	remaining int
	alert     bool
	invalid   bool
}

func (o *tlsRecordObserver) observe(p []byte) (progress bool) {
	for len(p) > 0 && !o.invalid && !o.alert {
		if o.remaining > 0 {
			n := min(o.remaining, len(p))
			o.remaining -= n
			progress = true
			p = p[n:]
			continue
		}
		n := copy(o.header[o.headerN:], p)
		o.headerN += n
		p = p[n:]
		if o.headerN > 0 && o.header[0] == 21 {
			o.alert = true
			return
		}
		if o.headerN < len(o.header) {
			return
		}
		o.headerN = 0
		o.remaining = int(o.header[3])<<8 | int(o.header[4])
		o.invalid = o.header[0] < 20 || o.header[0] > 23 || o.header[1] != 3 || o.header[2] > 4 || o.remaining > 18432
	}
	return
}

func newTLSLatencyConn(c net.Conn, dialElapsed time.Duration, onLatency func(time.Duration)) *tlsLatencyConn {
	return &tlsLatencyConn{Conn: c, dialElapsed: dialElapsed, onLatency: onLatency, handshakeTimeout: 5 * time.Second}
}

func appendTunnelPrefix(prefix *[6]byte, n *int, p []byte) {
	*n += copy(prefix[*n:], p)
}

// tlsHelloPrefix checks the TLS record header and first handshake type, not
// the encrypted handshake or certificate. Prefixes may span arbitrary IO calls.
func tlsHelloPrefix(p [6]byte, n int, kind byte) bool {
	return n == len(p) && p[0] == 22 && p[1] == 3 && p[2] <= 4 && (int(p[3])<<8|int(p[4])) >= 4 && p[5] == kind
}

func (c *tlsLatencyConn) invalidTLSResponseLocked() bool {
	return c.serverRecords.alert || c.serverRecords.invalid ||
		(c.serverN > 0 && !tlsHelloPrefix(c.serverPrefix, c.serverN, 2) &&
			(c.serverN == len(c.serverPrefix) || c.serverPrefix[0] != 22))
}

func (c *tlsLatencyConn) completeProgressLocked() bool {
	if c.handshakeComplete || c.invalidTLSResponseLocked() {
		return false
	}
	c.handshakeComplete = true
	_ = c.Conn.SetReadDeadline(time.Time{})
	return true
}

func (c *tlsLatencyConn) reportLatency() {
	if start := c.startTime.Load(); start > 0 && c.reported.CompareAndSwap(false, true) {
		latency := c.dialElapsed + time.Duration(time.Now().UnixNano()-start)
		if c.onLatency != nil {
			c.onLatency(latency)
		}
	}
}

func (c *tlsLatencyConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if len(p) > 0 {
		c.startTime.CompareAndSwap(0, time.Now().UnixNano())
		wasTLS := tlsHelloPrefix(c.clientPrefix, c.clientN, 1)
		appendTunnelPrefix(&c.clientPrefix, &c.clientN, p)
		if !wasTLS && tlsHelloPrefix(c.clientPrefix, c.clientN, 1) {
			_ = c.Conn.SetReadDeadline(time.Now().Add(c.handshakeTimeout))
		}
	}
	followup := len(p) > 0 && tlsHelloPrefix(c.clientPrefix, c.clientN, 1) &&
		tlsHelloPrefix(c.serverPrefix, c.serverN, 2) && !c.handshakeComplete
	serverProgress := c.serverProgress
	c.mu.Unlock()

	n, err := c.Conn.Write(p)
	c.mu.Lock()
	if err != nil {
		c.writeErr = err
	}
	valid := false
	if followup && n > 0 {
		c.clientFlight = true
		// The peer may respond before the underlying Write returns. Count that
		// read only after confirming that this client flight was actually sent.
		if c.serverProgress > serverProgress {
			valid = c.completeProgressLocked()
		}
	}
	c.mu.Unlock()
	if valid {
		c.reportLatency()
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
	valid := classified && c.serverN > 0 && !tlsRequest
	if tlsRequest && !c.handshakeComplete {
		progress := c.serverRecords.observe(p[:n])
		if progress {
			c.serverProgress++
		}
		if progress && c.clientFlight {
			valid = c.completeProgressLocked()
		}
	}
	c.mu.Unlock()
	if valid {
		c.reportLatency()
	}
	return n, err
}

type tunnelObservation struct {
	tlsRequest, invalidResponse, truncatedResponse bool
	handshakeIncomplete                            bool
	readErr, writeErr                              error
}

func (c *tlsLatencyConn) observation() tunnelObservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	tlsRequest := tlsHelloPrefix(c.clientPrefix, c.clientN, 1)
	return tunnelObservation{
		tlsRequest: tlsRequest, invalidResponse: tlsRequest && c.invalidTLSResponseLocked(),
		truncatedResponse:   tlsRequest && c.serverN > 0 && c.serverN < len(c.serverPrefix) && c.readErr != nil,
		handshakeIncomplete: tlsRequest && !c.handshakeComplete,
		readErr:             c.readErr, writeErr: c.writeErr,
	}
}

func (c *tlsLatencyConn) CloseWrite() error { return closeWriteErr(c.Conn) }
func (c *tlsLatencyConn) CloseRead() error  { return closeReadErr(c.Conn) }
