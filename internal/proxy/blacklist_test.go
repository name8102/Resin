package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
	M "github.com/sagernet/sing/common/metadata"
)

func TestSocks5Inbound_InitialTLSFailureBlacklistsWholeNode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hello bool
		stall bool
	}{{name: "EOF"}, {name: "ServerHello-EOF", hello: true}, {name: "stall", stall: true}} {
		t.Run(tc.name, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				a, b := net.Pipe()
				go func() {
					defer b.Close()
					if _, err := io.ReadFull(b, make([]byte, len(testClientHello))); err != nil {
						return
					}
					if tc.hello {
						_, _ = b.Write(testServerHello)
					}
					if tc.stall {
						_, _ = io.Copy(io.Discard, b)
					}
				}()
				return a, nil
			})
			emitter := newMockEventEmitter()
			inbound := NewSocks5Inbound(Socks5InboundConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool, Health: env.pool, Events: emitter})
			client, reader, done := startSocks5Session(t, inbound)
			defer client.Close()
			_ = client.SetDeadline(time.Now().Add(7 * time.Second))
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
				t.Fatal("TLS failure did not close relay")
			}
			log := <-emitter.logCh
			if log.NetOK {
				t.Fatal("incomplete TLS handshake marked successful")
			}
			if tc.hello && log.UpstreamStage != "connect_tls_handshake_eof" {
				t.Fatalf("handshake EOF misclassified: %s", log.UpstreamStage)
			}
			route, err := env.router.RouteRequest("plat", "", "different.example:80")
			if !errors.Is(err, routing.ErrNoAvailableNodes) {
				t.Fatalf("failed node still available for another target: %+v %v", route, err)
			}
			h, err := node.ParseHex(log.NodeHash)
			if err != nil {
				t.Fatal(err)
			}
			entry, _ := env.pool.GetEntry(h)
			if time.Until(time.Unix(0, entry.BlacklistUntil.Load())) < 59*time.Minute {
				t.Fatal("node did not get hour-long blacklist")
			}
			env.pool.RecordResult(h, true)
			if !entry.IsCircuitOpen() {
				t.Fatal("probe success immediately reopened blacklisted node")
			}
		})
	}
}

func TestPassiveBlacklistIgnoresSyncthingPorts(t *testing.T) {
	for _, target := range []string{"peer.example:22000", "[2001:db8::1]:22000", "peer.example:22067", "web.example:443", "git.example:22", "web.example:80"} {
		t.Run(target, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			route, err := env.router.RouteRequest("plat", "", target)
			if err != nil {
				t.Fatal(err)
			}
			recordPassiveResult(env.pool, route, target, false)
			entry, _ := env.pool.GetEntry(route.NodeHash)
			_, port, _ := net.SplitHostPort(target)
			wantBlocked := port != "22000" && port != "22067"
			if entry.IsCircuitOpen() != wantBlocked {
				t.Fatalf("blacklisted = %v, want %v", entry.IsCircuitOpen(), wantBlocked)
			}
		})
	}
}
