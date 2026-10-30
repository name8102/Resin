package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
	M "github.com/sagernet/sing/common/metadata"
)

type passiveReadConn struct {
	stubHalfCloseConn
}

func (c *passiveReadConn) Read(p []byte) (int, error) { return len(p), nil }

func passiveTestConn(t *testing.T) (*passiveTrafficConn, *node.NodeEntry, *time.Time) {
	t.Helper()
	entry := &node.NodeEntry{}
	conn := newPassiveTrafficConn(&passiveReadConn{}, entry).(*passiveTrafficConn)
	now := time.Now()
	conn.now = func() time.Time { return now }
	return conn, entry, &now
}

func readPassiveBytes(t *testing.T, c net.Conn, count int) {
	t.Helper()
	if n, err := c.Read(make([]byte, count)); err != nil || n != count {
		t.Fatalf("Read = %d, %v", n, err)
	}
}

func TestPassiveTrafficConnReportsBeforeClose(t *testing.T) {
	conn, entry, now := passiveTestConn(t)
	readPassiveBytes(t, conn, 32*1024)
	*now = now.Add(200 * time.Millisecond)
	readPassiveBytes(t, conn, 32*1024)
	if got, ok := entry.Traffic.DownloadSpeed(*now, time.Minute); !ok || got != 327680 {
		t.Fatalf("first active window = %v, measured=%v", got, ok)
	}
	// A second independent download window updates the EWMA while still open.
	*now = now.Add(time.Second)
	readPassiveBytes(t, conn, 64*1024)
	*now = now.Add(200 * time.Millisecond)
	readPassiveBytes(t, conn, 64*1024)
	if got, ok := entry.Traffic.DownloadSpeed(*now, time.Minute); !ok || got != 409600 {
		t.Fatalf("second active window = %v, measured=%v", got, ok)
	}
	*now = now.Add(time.Hour)
	_ = conn.Close()
	if _, ok := entry.Traffic.DownloadSpeed(*now, time.Minute); ok {
		t.Fatal("idle close refreshed an old sample")
	}
}

func TestPassiveTrafficConnDiscardsIdleGap(t *testing.T) {
	conn, entry, now := passiveTestConn(t)
	readPassiveBytes(t, conn, 32*1024)
	*now = now.Add(3 * time.Second)
	readPassiveBytes(t, conn, 32*1024)
	if _, ok := entry.Traffic.DownloadSpeed(*now, time.Minute); ok {
		t.Fatal("bytes on opposite sides of idle gap formed a sample")
	}
	*now = now.Add(200 * time.Millisecond)
	readPassiveBytes(t, conn, 32*1024)
	if got, ok := entry.Traffic.DownloadSpeed(*now, time.Minute); !ok || got != 327680 {
		t.Fatalf("sample after idle gap = %v, measured=%v", got, ok)
	}
}

func TestPassiveTrafficConnIgnoresTinyAndIdleTransfers(t *testing.T) {
	for _, size := range []int{1, 64 * 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			conn, entry, now := passiveTestConn(t)
			readPassiveBytes(t, conn, size)
			*now = now.Add(time.Minute)
			_ = conn.Close()
			if _, ok := entry.Traffic.DownloadSpeed(*now, time.Minute); ok {
				t.Fatal("instant transfer followed by idle close produced a sample")
			}
		})
	}
}

func TestPassiveTrafficConnHalfCloseAndConcurrentShutdown(t *testing.T) {
	base := &passiveReadConn{}
	conn := newPassiveTrafficConn(base, &node.NodeEntry{})
	if err := conn.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := conn.(interface{ CloseRead() error }).CloseRead(); err != nil {
		t.Fatal(err)
	}
	if base.closeWriteCalls.Load() != 1 || base.closeReadCalls.Load() != 1 {
		t.Fatal("half-close was not forwarded")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				_, _ = conn.Read(make([]byte, 1024))
			}
		})
	}
	wg.Go(func() { _ = conn.Close() })
	wg.Wait()
}

func TestPassiveTrafficConnPreservesReadError(t *testing.T) {
	base := &stubConn{}
	conn := newPassiveTrafficConn(base, &node.NodeEntry{})
	if n, err := conn.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read = %d, %v", n, err)
	}
	if got := newPassiveTrafficConn(base, nil); got != base {
		t.Fatal("missing node should leave connection untouched")
	}
}

func TestPassiveTrafficHTTPTransportWrapsSuccessfulProxiedDials(t *testing.T) {
	for _, target := range []string{"example.com:443", "example.com:22000", "[::1]:22067"} {
		t.Run(target, func(t *testing.T) {
			pool := NewOutboundTransportPool(OutboundTransportConfig{})
			entry := &node.NodeEntry{}
			NewForwardProxy(ForwardProxyConfig{Pool: &mockPool{entry: entry}, TransportPool: pool})
			// Reverse construction must preserve the first shared-pool binding.
			NewReverseProxy(ReverseProxyConfig{Pool: &mockPool{entry: &node.NodeEntry{}}, TransportPool: pool})
			base := &passiveReadConn{}
			ob := &mockOutbound{dialFunc: func(context.Context, string, M.Socksaddr) (net.Conn, error) { return base, nil }}
			conn, err := pool.Get(node.Hash{1}, ob, nil).DialContext(context.Background(), "tcp", target)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			wrapped, ok := conn.(*passiveTrafficConn)
			if target == "example.com:443" {
				if !ok || wrapped.entry != entry || wrapped.Conn != base {
					t.Fatal("successful HTTP dial did not bind original node entry")
				}
			} else if conn != base {
				t.Fatal("Syncthing transfer should not be sampled")
			}
		})
	}
	pool := NewOutboundTransportPool(OutboundTransportConfig{})
	pool.WrapConnection = func(node.Hash, net.Conn) net.Conn { t.Fatal("failed dial was wrapped"); return nil }
	_, err := pool.Get(node.Hash{1}, &noopOutbound{}, nil).DialContext(context.Background(), "tcp", "example.com:443")
	if err == nil {
		t.Fatal("expected dial error")
	}
}

func TestPassiveTrafficTunnelWrapsProxiedDials(t *testing.T) {
	entry := &node.NodeEntry{}
	for _, target := range []string{"example.com:443", "example.com:22000", "example.com:22067"} {
		base := &passiveReadConn{}
		prepared := finishConnectTunnel(tunnelDeps{pool: &mockPool{entry: entry}}, routedOutbound{Route: routing.RouteResult{NodeHash: node.Hash{1}}}, base, target, time.Millisecond)
		tlsConn := prepared.session.upstreamConn.(*tlsLatencyConn)
		wrapped, ok := tlsConn.Conn.(*passiveTrafficConn)
		if target == "example.com:443" {
			if !ok || wrapped.entry != entry || wrapped.Conn != base {
				t.Fatal("successful tunnel dial did not bind node entry")
			}
		} else if tlsConn.Conn != base {
			t.Fatal("Syncthing tunnel should not be sampled")
		}
		_ = prepared.session.upstreamConn.Close()
	}
}
