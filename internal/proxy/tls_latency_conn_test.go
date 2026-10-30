package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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

func TestTLSLatencyConn_ServerHelloThenEOF(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(time.Second))
	_ = server.SetDeadline(time.Now().Add(time.Second))
	observed := newTLSLatencyConn(client, 0, nil)
	go func() {
		defer server.Close()
		_, _ = io.ReadFull(server, make([]byte, len(testClientHello)))
		_, _ = server.Write(testServerHello)
	}()
	if _, err := observed.Write(testClientHello); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(observed); err != nil {
		t.Fatal(err)
	}
	if observed.reported.Load() {
		t.Fatal("ServerHello followed by EOF generated a positive latency sample")
	}
	if got := observed.observation(); !got.handshakeIncomplete || got.readErr != io.EOF {
		t.Fatalf("incomplete TLS EOF not observed: %+v", got)
	}
}

// Contains alert-looking bytes inside an application record. Only record
// boundaries, never arbitrary payload bytes, may identify a server alert.
var testTLSProgressRecord = []byte{23, 3, 3, 0, 7, 21, 3, 3, 0, 2, 2, 40}
var testTLSClientFlight = []byte{20, 3, 3, 0, 1, 1}

func writeTLSChunks(conn net.Conn, p []byte, fragmented bool) error {
	for len(p) > 0 {
		n := len(p)
		if fragmented {
			n = 1
		}
		if _, err := conn.Write(p[:n]); err != nil {
			return err
		}
		p = p[n:]
	}
	return nil
}

func TestTLSLatencyConn_PostServerHelloProgress(t *testing.T) {
	for _, fragmented := range []bool{false, true} {
		t.Run(fmt.Sprint("fragmented=", fragmented), func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			_ = client.SetDeadline(time.Now().Add(2 * time.Second))
			_ = server.SetDeadline(time.Now().Add(2 * time.Second))
			samples := make(chan time.Duration, 4)
			observed := newTLSLatencyConn(client, 0, func(d time.Duration) { samples <- d })
			serverDone := make(chan error, 1)
			go func() {
				if _, err := io.ReadFull(server, make([]byte, len(testClientHello))); err != nil {
					serverDone <- err
					return
				}
				if err := writeTLSChunks(server, testServerHello, fragmented); err != nil {
					serverDone <- err
					return
				}
				flight := make([]byte, len(testTLSClientFlight))
				if _, err := io.ReadFull(server, flight); err != nil {
					serverDone <- err
					return
				}
				if !bytes.Equal(flight, testTLSClientFlight) {
					serverDone <- fmt.Errorf("client flight changed: %x", flight)
					return
				}
				for i := 0; i < 2; i++ {
					if err := writeTLSChunks(server, testTLSProgressRecord, fragmented); err != nil {
						serverDone <- err
						return
					}
				}
				serverDone <- nil
			}()
			if err := writeTLSChunks(observed, testClientHello, fragmented); err != nil {
				t.Fatal(err)
			}
			hello := make([]byte, len(testServerHello))
			if _, err := io.ReadFull(observed, hello); err != nil || !bytes.Equal(hello, testServerHello) {
				t.Fatalf("ServerHello changed: %x, %v", hello, err)
			}
			if len(samples) != 0 || !observed.observation().handshakeIncomplete {
				t.Fatal("ServerHello alone counted as post-hello progress")
			}
			if err := writeTLSChunks(observed, testTLSClientFlight, fragmented); err != nil {
				t.Fatal(err)
			}
			if len(samples) != 0 {
				t.Fatal("client flight without upstream response counted as progress")
			}
			for i := 0; i < 2; i++ {
				response := make([]byte, len(testTLSProgressRecord))
				if _, err := io.ReadFull(observed, response); err != nil || !bytes.Equal(response, testTLSProgressRecord) {
					t.Fatalf("response changed: %x, %v", response, err)
				}
			}
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
			if len(samples) != 1 {
				t.Fatalf("got %d samples, want exactly one", len(samples))
			}
			if got := observed.observation(); got.handshakeIncomplete || got.invalidResponse {
				t.Fatalf("valid TLS progress not observed: %+v", got)
			}
		})
	}
}

func TestTLSLatencyConn_PostServerHelloAlert(t *testing.T) {
	for _, fragmented := range []bool{false, true} {
		t.Run(fmt.Sprint("fragmented=", fragmented), func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			_ = client.SetDeadline(time.Now().Add(time.Second))
			_ = server.SetDeadline(time.Now().Add(time.Second))
			observed := newTLSLatencyConn(client, 0, nil)
			go func() {
				defer server.Close()
				_, _ = io.ReadFull(server, make([]byte, len(testClientHello)))
				_ = writeTLSChunks(server, testServerHello, fragmented)
				_, _ = io.ReadFull(server, make([]byte, len(testTLSClientFlight)))
				_ = writeTLSChunks(server, []byte{21, 3, 3, 0, 2, 2, 40}, fragmented)
			}()
			if _, err := observed.Write(testClientHello); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(observed, make([]byte, len(testServerHello))); err != nil {
				t.Fatal(err)
			}
			if _, err := observed.Write(testTLSClientFlight); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadAll(observed); err != nil {
				t.Fatal(err)
			}
			if observed.reported.Load() {
				t.Fatal("post-ServerHello alert generated a positive sample")
			}
			if got := observed.observation(); !got.invalidResponse || !got.handshakeIncomplete {
				t.Fatalf("server alert not observed: %+v", got)
			}
		})
	}
}

func TestTLSLatencyConn_HandshakeReadDeadline(t *testing.T) {
	for _, serverHello := range []bool{false, true} {
		t.Run(fmt.Sprint("ServerHello=", serverHello), func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			_ = server.SetDeadline(time.Now().Add(time.Second))
			observed := newTLSLatencyConn(client, 0, nil)
			observed.handshakeTimeout = 30 * time.Millisecond
			go func() {
				_, _ = io.ReadFull(server, make([]byte, len(testClientHello)))
				if serverHello {
					_, _ = server.Write(testServerHello)
				}
			}()
			if _, err := observed.Write(testClientHello); err != nil {
				t.Fatal(err)
			}
			if serverHello {
				if _, err := io.ReadFull(observed, make([]byte, len(testServerHello))); err != nil {
					t.Fatal(err)
				}
			}
			_, err := observed.Read(make([]byte, 1))
			var netErr net.Error
			if !errors.As(err, &netErr) || !netErr.Timeout() {
				t.Fatalf("stalled TLS read should time out: %v", err)
			}
			if got := observed.observation(); !got.handshakeIncomplete || got.readErr != err {
				t.Fatalf("TLS timeout not observed: %+v", got)
			}
			if observed.reported.Load() {
				t.Fatal("stalled handshake generated a positive sample")
			}
		})
	}
}

func TestTLSLatencyConn_ProgressClearsReadDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	_ = server.SetDeadline(time.Now().Add(2 * time.Second))
	observed := newTLSLatencyConn(client, 0, nil)
	observed.handshakeTimeout = 80 * time.Millisecond
	go func() {
		_, _ = io.ReadFull(server, make([]byte, len(testClientHello)))
		_, _ = server.Write(testServerHello)
		_, _ = io.ReadFull(server, make([]byte, len(testTLSClientFlight)))
		_, _ = server.Write(testTLSProgressRecord)
		time.Sleep(2 * observed.handshakeTimeout)
		_, _ = server.Write([]byte("later tunnel bytes"))
	}()
	if _, err := observed.Write(testClientHello); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(observed, make([]byte, len(testServerHello))); err != nil {
		t.Fatal(err)
	}
	if _, err := observed.Write(testTLSClientFlight); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(observed, make([]byte, len(testTLSProgressRecord))); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(observed, make([]byte, len("later tunnel bytes"))); err != nil {
		t.Fatalf("completed progress left a handshake deadline on the tunnel: %v", err)
	}
}

type tlsFragmentedReadConn struct{ net.Conn }

func (c tlsFragmentedReadConn) Read(p []byte) (int, error) {
	return c.Conn.Read(p[:min(len(p), 3)])
}

func TestTLSLatencyConn_RealTLSPassthrough(t *testing.T) {
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		for _, fragmented := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fragmented=%t", tls.VersionName(version), fragmented), func(t *testing.T) {
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					_, _ = io.WriteString(w, "TLS passthrough")
				}))
				server.TLS = &tls.Config{MinVersion: version, MaxVersion: version}
				server.StartTLS()
				defer server.Close()
				raw, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer raw.Close()
				_ = raw.SetDeadline(time.Now().Add(2 * time.Second))
				if fragmented {
					raw = tlsFragmentedReadConn{raw}
				}
				samples := make(chan time.Duration, 4)
				observed := newTLSLatencyConn(raw, 0, func(d time.Duration) { samples <- d })
				roots := x509.NewCertPool()
				roots.AddCert(server.Certificate())
				client := tls.Client(observed, &tls.Config{
					MinVersion: version, MaxVersion: version,
					RootCAs: roots, ServerName: "example.com",
				})
				defer client.Close()
				if err := client.Handshake(); err != nil {
					t.Fatalf("TLS handshake failed: %v", err)
				}
				if _, err := io.WriteString(client, "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"); err != nil {
					t.Fatal(err)
				}
				response, err := http.ReadResponse(bufio.NewReader(client), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if err != nil || string(body) != "TLS passthrough" {
					t.Fatalf("TLS application data changed: %q, %v", body, err)
				}
				if len(samples) != 1 {
					t.Fatalf("got %d samples, want exactly one", len(samples))
				}
				if got := observed.observation(); got.handshakeIncomplete || got.invalidResponse {
					t.Fatalf("normal TLS connection marked incomplete or invalid: %+v", got)
				}
			})
		}
	}
}
