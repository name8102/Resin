package proxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
)

func TestForwardProxy_HTTPRetriesOnlyBeforeConnection(t *testing.T) {
	for _, tc := range []struct {
		name         string
		body         string
		connected    bool
		wantFallback bool
	}{
		{name: "GET dial failure", wantFallback: true},
		{name: "POST body is not replayed", body: "upload"},
		{name: "connected request is not replayed", connected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
			defer upstream.Close()
			if _, err := env.router.RouteRequest("plat", "device", upstream.URL); err != nil {
				t.Fatal(err)
			}
			var failedCalls, goodCalls atomic.Int32
			setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				failedCalls.Add(1)
				if !tc.connected {
					return nil, errors.New("authentication required")
				}
				a, b := net.Pipe()
				go func() {
					defer b.Close()
					req, err := http.ReadRequest(bufio.NewReader(b))
					if err == nil {
						req.Body.Close()
					}
				}()
				return a, nil
			})
			addRetryNode(t, env, "good", "203.0.113.11", func(ctx context.Context, network string, dest M.Socksaddr) (net.Conn, error) {
				goodCalls.Add(1)
				var d net.Dialer
				return d.DialContext(ctx, network, dest.String())
			})
			fp := NewForwardProxy(ForwardProxyConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool})
			defer fp.transportPool.CloseAll()
			method := http.MethodGet
			var body io.Reader
			if tc.body != "" {
				method = http.MethodPost
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(method, upstream.URL, body)
			req.Header.Set("Proxy-Authorization", basicAuth("plat.device", "tok"))
			w := httptest.NewRecorder()
			fp.ServeHTTP(w, req)
			if failedCalls.Load() != 1 {
				t.Fatalf("failed node called %d times", failedCalls.Load())
			}
			if tc.wantFallback {
				if w.Code != http.StatusOK || w.Body.String() != "ok" || goodCalls.Load() != 1 {
					t.Fatalf("fallback failed: status=%d body=%q calls=%d", w.Code, w.Body.String(), goodCalls.Load())
				}
			} else if goodCalls.Load() != 0 || w.Code != http.StatusBadGateway {
				t.Fatalf("request replayed: status=%d fallbackCalls=%d", w.Code, goodCalls.Load())
			}
		})
	}
}
