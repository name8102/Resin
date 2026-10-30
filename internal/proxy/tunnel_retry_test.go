package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

func addRetryNode(t *testing.T, env *proxyE2EEnv, id, ip string, dial func(context.Context, string, M.Socksaddr) (net.Conn, error)) node.Hash {
	t.Helper()
	raw := json.RawMessage(`{"type":"stub","server":"` + id + `","server_port":1}`)
	h := node.HashFromRawOptions(raw)
	sub, _ := env.subMgr.Get("sub-1")
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"tag"}})
	env.pool.AddNodeFromSub(h, raw, "sub-1")
	e, _ := env.pool.GetEntry(h)
	var ob adapter.Outbound = &mockOutbound{dialFunc: dial}
	e.Outbound.Store(&ob)
	e.SetEgressIP(netip.MustParseAddr(ip))
	e.LatencyTable.Update("example.com", 20*time.Millisecond, 10*time.Minute)
	env.pool.RecordResult(h, true)
	env.pool.NotifyNodeDirty(h)
	p, _ := env.pool.GetPlatform("plat-id")
	if !p.View().Contains(h) {
		t.Fatal("retry node not routable")
	}
	return h
}

func TestPrepareConnectTunnel_RetriesFailedStickyNode(t *testing.T) {
	env := newProxyE2EEnv(t)
	first, err := env.router.RouteRequest("plat", "device", "api.github.com:443")
	if err != nil {
		t.Fatal(err)
	}
	var failedCalls, goodCalls atomic.Int32
	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		failedCalls.Add(1)
		return nil, errors.New("authentication required")
	})
	good := addRetryNode(t, env, "good", "203.0.113.10", func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		goodCalls.Add(1)
		a, b := net.Pipe()
		t.Cleanup(func() { a.Close(); b.Close() })
		return a, nil
	})
	result := prepareConnectTunnel(context.Background(), tunnelDeps{router: env.router, pool: env.pool}, "plat", "device", "api.github.com:443")
	if result.session == nil {
		t.Fatalf("expected fallback tunnel, got %+v", result)
	}
	if result.route.NodeHash != good || result.route.EgressIP != first.EgressIP || failedCalls.Load() != 1 || goodCalls.Load() != 1 {
		t.Fatalf("wrong fallback: route=%s first=%s failed=%d good=%d", result.route.NodeHash.Hex(), first.NodeHash.Hex(), failedCalls.Load(), goodCalls.Load())
	}
}

func TestPrepareConnectTunnel_KeepsTargetFailureAfterProbeSuccess(t *testing.T) {
	env := newProxyE2EEnv(t)
	var calls atomic.Int32
	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		calls.Add(1)
		return nil, errors.New("authentication required")
	})
	deps := tunnelDeps{router: env.router, pool: env.pool}
	result := prepareConnectTunnel(context.Background(), deps, "plat", "", "api.github.com:443")
	if result.proxyErr == nil {
		t.Fatal("expected dial failure")
	}
	// A successful Google probe must not make this node usable for GitHub again.
	env.pool.RecordResult(result.route.NodeHash, true)
	prepareConnectTunnel(context.Background(), deps, "plat", "", "api.github.com:443")
	if calls.Load() != 1 {
		t.Fatalf("bad node retried after unrelated probe success: calls=%d", calls.Load())
	}
}

func TestPrepareConnectTunnel_ThreeDistinctAttempts(t *testing.T) {
	env := newProxyE2EEnv(t)
	// Explicit exclusions must work even when persistent passive penalties are off.
	p, _ := env.pool.GetPlatform("plat-id")
	p.PassiveCircuitBreakerDisabled = true
	var calls atomic.Int32
	seen := make(map[string]int)
	dial := func(id string) func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("attempt has no deadline")
			}
			seen[id]++
			calls.Add(1)
			return nil, errors.New("authentication required")
		}
	}
	setProxyE2EOutboundDialFunc(t, env, dial("first"))
	for i := 0; i < 4; i++ {
		addRetryNode(t, env, fmt.Sprint(i), fmt.Sprintf("203.0.113.%d", 20+i), dial(fmt.Sprint(i)))
	}
	result := prepareConnectTunnel(context.Background(), tunnelDeps{router: env.router, pool: env.pool}, "plat", "", "api.github.com:443")
	if result.proxyErr != ErrUpstreamConnectFailed || result.upstreamStage != "connect_dial" {
		t.Fatalf("lost upstream failure: %+v", result)
	}
	if calls.Load() != 3 || len(seen) != 3 {
		t.Fatalf("attempts=%d distinct=%d, want 3", calls.Load(), len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("node %s dialed %d times", id, n)
		}
	}
}

func TestPrepareConnectTunnel_AttemptTimeoutCanRetry(t *testing.T) {
	env := newProxyE2EEnv(t)
	_, _ = env.router.RouteRequest("plat", "device", "api.github.com:443")
	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) { return nil, context.DeadlineExceeded })
	good := addRetryNode(t, env, "good", "203.0.113.11", func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		a, b := net.Pipe()
		t.Cleanup(func() { a.Close(); b.Close() })
		return a, nil
	})
	result := prepareConnectTunnel(context.Background(), tunnelDeps{router: env.router, pool: env.pool}, "plat", "device", "api.github.com:443")
	if result.session == nil || result.route.NodeHash != good {
		t.Fatalf("attempt timeout did not fail over: %+v", result)
	}
}

func TestPrepareConnectTunnel_CallerCancellationDoesNotPenalizeNode(t *testing.T) {
	env := newProxyE2EEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		calls.Add(1)
		cancel()
		return nil, context.Canceled
	})
	health := &mockHealthRecorder{}
	result := prepareConnectTunnel(ctx, tunnelDeps{router: env.router, pool: env.pool, health: health}, "plat", "", "api.github.com:443")
	if !result.canceled || calls.Load() != 1 {
		t.Fatalf("cancellation retried: result=%+v calls=%d", result, calls.Load())
	}
	if _, err := env.router.RouteRequest("plat", "", "api.github.com:443"); err != nil {
		t.Fatalf("client cancellation cooled node: %v", err)
	}
	if health.resultCalls.Load() != 0 {
		t.Fatal("client cancellation recorded as node failure")
	}
	// An already-expired caller must not dial at all.
	deadlineCtx, done := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer done()
	result = prepareConnectTunnel(deadlineCtx, tunnelDeps{router: env.router, pool: env.pool}, "plat", "", "api.github.com:443")
	if result.proxyErr != ErrUpstreamTimeout || calls.Load() != 1 {
		t.Fatalf("expired caller dialed: %+v", result)
	}
}

func TestPrepareConnectTunnel_OlderSuccessDoesNotClearNewFailure(t *testing.T) {
	env := newProxyE2EEnv(t)
	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		a, b := net.Pipe()
		t.Cleanup(func() { a.Close(); b.Close() })
		return a, nil
	})
	result := prepareConnectTunnel(context.Background(), tunnelDeps{router: env.router, pool: env.pool, health: env.pool}, "plat", "", "api.github.com:443")
	if result.session == nil {
		t.Fatalf("prepare: %+v", result)
	}
	env.router.ReportTargetFailure(result.route, "api.github.com:443")
	result.session.recordResult(true)
	env.pool.RecordResult(result.route.NodeHash, true)
	if _, err := env.router.RouteRequest("plat", "", "api.github.com:443"); !errors.Is(err, routing.ErrNoAvailableNodes) {
		t.Fatalf("old success cleared new failure: %v", err)
	}
}

func TestSocks5Inbound_RetriesBeforeSuccessReply(t *testing.T) {
	env := newProxyE2EEnv(t)
	if _, err := env.router.RouteRequest("plat", "device", "127.0.0.1:443"); err != nil {
		t.Fatal(err)
	}
	var failedCalls atomic.Int32
	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		failedCalls.Add(1)
		return nil, errors.New("authentication required")
	})
	addRetryNode(t, env, "good", "203.0.113.11", func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		a, b := net.Pipe()
		go func() { defer b.Close(); _, _ = io.Copy(b, b) }()
		return a, nil
	})
	inbound := NewSocks5Inbound(Socks5InboundConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool})
	client, reader, done := startSocks5Session(t, inbound)
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(2 * time.Second))
	writeAll(t, client, []byte{socks5Version, 1, socks5MethodUserPass})
	if got := readExactly(t, reader, 2); got[1] != socks5MethodUserPass {
		t.Fatalf("auth method: %v", got)
	}
	writeAll(t, client, socks5UserPassPacket("plat.device", "tok"))
	if got := readExactly(t, reader, 2); got[1] != socks5UserPassStatusSuccess {
		t.Fatalf("auth: %v", got)
	}
	writeAll(t, client, socks5ConnectIPv4Packet("127.0.0.1:443"))
	if got := readExactly(t, reader, 10); got[1] != socks5ReplySucceeded {
		t.Fatalf("CONNECT: %v", got)
	}
	writeAll(t, client, []byte("ping"))
	if got := string(readExactly(t, reader, 4)); got != "ping" {
		t.Fatalf("relay: %q", got)
	}
	if failedCalls.Load() != 1 {
		t.Fatalf("failed node dialed %d times", failedCalls.Load())
	}
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SOCKS5 relay did not close")
	}
}
