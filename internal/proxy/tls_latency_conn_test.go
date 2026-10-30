package proxy

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

type latencyReadNotifier struct {
	net.Conn
	started    chan struct{}
	writeDelay time.Duration
}

func (c *latencyReadNotifier) Read(p []byte) (int, error) {
	close(c.started)
	return c.Conn.Read(p)
}

func (c *latencyReadNotifier) Write(p []byte) (int, error) {
	time.Sleep(c.writeDelay)
	return c.Conn.Write(p)
}

func TestTLSLatencyConn_ReadBeforeWrite(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	_ = client.SetDeadline(time.Now().Add(2 * time.Second))
	_ = server.SetDeadline(time.Now().Add(2 * time.Second))

	started := make(chan struct{})
	samples := make(chan time.Duration, 1)
	const dialElapsed = 100 * time.Millisecond
	const writeDelay = 40 * time.Millisecond
	conn := newTLSLatencyConn(&latencyReadNotifier{Conn: client, started: started, writeDelay: writeDelay}, dialElapsed, func(latency time.Duration) {
		samples <- latency
	})
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(conn, make([]byte, 2))
		readDone <- err
	}()
	<-started
	if conn.startTime.Load() != 0 {
		t.Fatal("timer started before the client sent any bytes")
	}
	go func() {
		_, _ = io.ReadFull(server, make([]byte, 2))
		_, _ = server.Write([]byte("ok"))
	}()
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := <-readDone; err != nil {
		t.Fatalf("Read: %v", err)
	}
	select {
	case latency := <-samples:
		if latency < dialElapsed+writeDelay {
			t.Fatalf("latency %v excludes dial or first-write duration", latency)
		}
	case <-time.After(time.Second):
		t.Fatal("missing first-response sample when Read starts before Write")
	}
}

func TestPrepareConnectTunnel_LatencyIncludesDial(t *testing.T) {
	for _, account := range []string{"", "device"} {
		t.Run("account="+account, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			_ = client.SetDeadline(time.Now().Add(2 * time.Second))
			_ = server.SetDeadline(time.Now().Add(2 * time.Second))
			const dialDelay = 40 * time.Millisecond
			setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				time.Sleep(dialDelay)
				return client, nil
			})
			rec := &latencySampleRecorder{samples: make(chan time.Duration, 1)}
			result := prepareConnectTunnel(context.Background(), tunnelDeps{router: env.router, pool: env.pool, health: rec}, "plat", account, "api.github.com:443")
			if result.session == nil {
				t.Fatalf("prepareConnectTunnel: %+v", result)
			}
			go func() {
				_, _ = io.ReadFull(server, make([]byte, 2))
				_, _ = server.Write([]byte("ok"))
			}()
			conn := result.session.upstreamConn
			if _, err := conn.Write([]byte("hi")); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if _, err := io.ReadFull(conn, make([]byte, 2)); err != nil {
				t.Fatalf("Read: %v", err)
			}
			select {
			case latency := <-rec.samples:
				if latency < dialDelay {
					t.Fatalf("latency %v excludes dial delay %v", latency, dialDelay)
				}
			case <-time.After(time.Second):
				t.Fatal("missing latency sample")
			}
		})
	}
}
