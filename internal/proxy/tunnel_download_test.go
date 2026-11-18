package proxy

import (
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"
)

type downloadEndConn struct {
	net.Conn
	err error
}

func (c downloadEndConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if errors.Is(err, io.EOF) {
		err = c.err
	}
	return n, err
}

func TestTunnelDownloadFeedbackDistinguishesReadErrorFromEOF(t *testing.T) {
	for _, readErr := range []error{io.EOF, syscall.ECONNRESET} {
		t.Run(readErr.Error(), func(t *testing.T) {
			env := newProxyE2EEnv(t)
			const origin = "downloads.example:443"
			route, err := env.router.RouteRequest("plat", "", origin)
			if err != nil {
				t.Fatal(err)
			}
			upstream, peer := net.Pipe()
			defer peer.Close()
			client, proxyClient := net.Pipe()
			defer client.Close()
			_ = client.SetDeadline(time.Now().Add(2 * time.Second))
			_ = peer.SetDeadline(time.Now().Add(2 * time.Second))
			health := &mockHealthRecorder{}
			deps := tunnelDeps{router: env.router, pool: env.pool, health: health}
			prepared := finishConnectTunnel(deps, routedOutbound{Route: route}, downloadEndConn{upstream, readErr}, origin, 0)
			done := make(chan tunnelRelayResult, 1)
			go func() {
				result := pumpPreparedTunnelReader(proxyClient, proxyClient, prepared.session, tunnelPumpOptions{})
				prepared.session.recordResult(result)
				done <- result
			}()
			flight := []byte{23, 3, 3, 0, 1, 0}
			payload := make([]byte, tunnelBulkBytes+32*1024)
			for i := 0; i < len(payload); i += 16 * 1024 {
				copy(payload[i:], []byte{23, 3, 3, 0x3f, 0xfb})
			}
			go func() {
				defer peer.Close()
				_, _ = io.ReadFull(peer, make([]byte, len(testClientHello)))
				_, _ = peer.Write(testServerHello)
				_, _ = io.ReadFull(peer, make([]byte, len(flight)))
				_, _ = peer.Write(payload)
			}()
			writeAll(t, client, testClientHello)
			if _, err := io.ReadFull(client, make([]byte, len(testServerHello))); err != nil {
				t.Fatal(err)
			}
			writeAll(t, client, flight)
			if n, err := io.Copy(io.Discard, client); err != nil || n != int64(len(payload)) {
				t.Fatalf("downloaded %d bytes: %v", n, err)
			}
			select {
			case result := <-done:
				if result.upstreamFailure || result.downloadInterrupted != (readErr != io.EOF) {
					t.Fatalf("unexpected download feedback: %+v", result)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("relay did not finish")
			}
			if health.resultCalls.Load() != 1 || health.lastSuccess.Load() != 1 {
				t.Fatal("bulk transfer teardown should not change established node health")
			}
		})
	}
}

func TestTunnelDownloadProgressRepeatedSlowReads(t *testing.T) {
	for _, tc := range []struct {
		name              string
		gap, downstreamIO time.Duration
		bytes             int
		newRequest        bool
		want              int
	}{
		{name: "repeated stalls", gap: 6 * time.Second, bytes: 32 * 1024, want: 1},
		{name: "slow trickle", gap: time.Second, bytes: 20 * 1024, want: 1},
		{name: "fast transfer", gap: time.Second, bytes: 256 * 1024},
		{name: "wait for new request", gap: 6 * time.Second, bytes: 32 * 1024, newRequest: true},
		{name: "client backpressure", downstreamIO: 6 * time.Second, gap: time.Millisecond, bytes: 32 * 1024},
		{name: "one quiet interval", gap: time.Minute, bytes: 32 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			reports := 0
			p := tunnelDownloadProgress{onSlow: func() { reports++ }}
			p.observe(tunnelBulkBytes, now, now, 0)
			for i := range 15 {
				start := now.Add(tc.downstreamIO)
				elapsed := tc.gap
				if tc.name == "one quiet interval" && i > 0 {
					elapsed = time.Millisecond
				}
				writes := p.clientWrites.Load()
				if tc.newRequest {
					p.clientWrites.Add(1)
				}
				now = start.Add(elapsed)
				p.observe(tc.bytes, start, now, writes)
			}
			if reports != tc.want {
				t.Fatalf("reported %d times, want %d", reports, tc.want)
			}
		})
	}
}
