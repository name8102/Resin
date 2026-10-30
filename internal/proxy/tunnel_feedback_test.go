package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/routing"
	M "github.com/sagernet/sing/common/metadata"
)

var testClientHello = []byte{22, 3, 1, 0, 4, 1, 0, 0, 0}
var testServerHello = []byte{22, 3, 3, 0, 4, 2, 0, 0, 0}

func TestTLSLatencyConn_RejectsAlertAndWrongProtocol(t *testing.T) {
	for _, response := range [][]byte{{21, 3, 3, 0, 2, 2, 40}, []byte("HTTP/1.1 403 Forbidden\r\n\r\n")} {
		client, server := net.Pipe()
		_ = client.SetDeadline(time.Now().Add(time.Second))
		_ = server.SetDeadline(time.Now().Add(time.Second))
		samples := make(chan time.Duration, 1)
		observed := newTLSLatencyConn(client, 0, func(d time.Duration) { samples <- d })
		go func() {
			defer server.Close()
			_, _ = io.ReadFull(server, make([]byte, len(testClientHello)))
			_, _ = server.Write(response)
		}()
		if _, err := observed.Write(testClientHello); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(observed); err != nil {
			t.Fatal(err)
		}
		client.Close()
		if observed.reported.Load() {
			t.Fatalf("invalid TLS response generated a positive latency sample: %x", response)
		}
	}
}

func TestSocks5Inbound_TLSInitialFailureCoolsOnlyTarget(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response []byte
		stage    string
	}{
		{"EOF", nil, "connect_no_ingress_traffic"},
		{"alert", []byte{21, 3, 3, 0, 2, 2, 40}, "connect_invalid_tls_response"},
		{"wrong-protocol", []byte("HTTP/1.1 403 Forbidden\r\n\r\n"), "connect_invalid_tls_response"},
		{"truncated-hello", testServerHello[:3], "connect_invalid_tls_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				a, b := net.Pipe()
				go func() {
					defer b.Close()
					_, _ = io.ReadFull(b, make([]byte, len(testClientHello)))
					if len(tc.response) > 0 {
						_, _ = b.Write(tc.response)
					}
				}()
				return a, nil
			})
			emitter := newMockEventEmitter()
			inbound := NewSocks5Inbound(Socks5InboundConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool, Events: emitter})
			client, reader, done := startSocks5Session(t, inbound)
			defer client.Close()
			_ = client.SetDeadline(time.Now().Add(2 * time.Second))
			writeAll(t, client, []byte{socks5Version, 1, socks5MethodUserPass})
			readExactly(t, reader, 2)
			writeAll(t, client, socks5UserPassPacket("plat", "tok"))
			readExactly(t, reader, 2)
			writeAll(t, client, socks5ConnectIPv4Packet("127.0.0.1:443"))
			readExactly(t, reader, 10)
			writeAll(t, client, testClientHello)
			_, _ = io.Copy(io.Discard, reader)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("relay did not close")
			}
			select {
			case e := <-emitter.logCh:
				if e.NetOK || e.UpstreamStage != tc.stage {
					t.Fatalf("TLS EOF was not logged as initial response failure: %+v", e)
				}
			default:
				t.Fatal("missing request log")
			}
			if _, err := env.router.RouteRequest("plat", "", "127.0.0.1:443"); !errors.Is(err, routing.ErrNoAvailableNodes) {
				t.Fatalf("failed TLS target not cooled: %v", err)
			}
			if _, err := env.router.RouteRequest("plat", "", "127.0.0.1:80"); err != nil {
				t.Fatalf("unrelated port cooled: %v", err)
			}

		})
	}
}

func TestTLSLatencyConn_FragmentedHelloRemainsTransparent(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(time.Second))
	_ = server.SetDeadline(time.Now().Add(time.Second))
	samples := make(chan time.Duration, 2)
	observed := newTLSLatencyConn(client, 0, func(d time.Duration) { samples <- d })
	go func() {
		defer server.Close()
		got := make([]byte, len(testClientHello))
		_, _ = io.ReadFull(server, got)
		for _, p := range [][]byte{testServerHello[:3], testServerHello[3:5], testServerHello[5:]} {
			_, _ = server.Write(p)
		}
	}()
	for _, p := range [][]byte{testClientHello[:2], testClientHello[2:5], testClientHello[5:]} {
		if _, err := observed.Write(p); err != nil {
			t.Fatal(err)
		}
	}
	got, err := io.ReadAll(observed)
	if err != nil || string(got) != string(testServerHello) {
		t.Fatalf("fragmented response changed: %x %v", got, err)
	}
	if len(samples) != 1 {
		t.Fatalf("got %d samples, want exactly one", len(samples))
	}
}

func TestPumpPreparedTunnel_ClientAbortDoesNotPenalizeTarget(t *testing.T) {
	for _, partialResponse := range [][]byte{nil, testServerHello[:3]} {
		t.Run(fmt.Sprint(len(partialResponse)), func(t *testing.T) {
			env := newProxyE2EEnv(t)
			route, err := env.router.RouteRequest("plat", "", "api.github.com:443")
			if err != nil {
				t.Fatal(err)
			}
			upstream, peer := net.Pipe()
			defer peer.Close()
			_ = upstream.SetDeadline(time.Now().Add(time.Second))
			client, proxyClient := net.Pipe()
			defer client.Close()
			health := &mockHealthRecorder{}
			prepared := finishConnectTunnel(tunnelDeps{router: env.router, health: health}, routedOutbound{Route: route}, upstream, "api.github.com:443", 0)
			done := make(chan tunnelRelayResult, 1)
			go func() {
				done <- pumpPreparedTunnelReader(proxyClient, proxyClient, prepared.session, tunnelPumpOptions{})
			}()
			received := make(chan struct{})
			go func() {
				_, _ = io.ReadFull(peer, make([]byte, len(testClientHello)))
				close(received)
				if len(partialResponse) > 0 {
					_, _ = peer.Write(partialResponse)
				}
			}()
			if _, err := client.Write(testClientHello); err != nil {
				t.Fatal(err)
			}
			<-received
			if len(partialResponse) > 0 {
				if _, err := io.ReadFull(client, make([]byte, len(partialResponse))); err != nil {
					t.Fatal(err)
				}
			}
			client.Close()
			var result tunnelRelayResult
			select {
			case result = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("client cancellation did not stop relay")
			}
			prepared.session.recordResult(result)
			if result.upstreamFailure {
				t.Fatal("client cancellation attributed to upstream")
			}
			if health.resultCalls.Load() != 0 {
				t.Fatal("client cancellation changed node health")
			}
			if _, err := env.router.RouteRequest("plat", "", "api.github.com:443"); err != nil {
				t.Fatalf("client cancellation cooled target: %v", err)
			}

		})
	}
}
