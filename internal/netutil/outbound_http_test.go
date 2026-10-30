package netutil

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/Resinat/Resin/internal/testutil"
)

func TestHTTPGetViaOutbound_RequireStatusOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	_, _, err = HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: true,
	})
	if err == nil {
		t.Fatal("expected non-200 status to return error")
	}
	if !strings.Contains(err.Error(), "unexpected status 404") {
		t.Fatalf("expected status error, got: %v", err)
	}
}

func TestHTTPGetViaOutbound_AllowNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("probe-body"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	body, _, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: false,
	})
	if err != nil {
		t.Fatalf("expected non-200 response to pass through, got: %v", err)
	}
	if string(body) != "probe-body" {
		t.Fatalf("unexpected body %q", string(body))
	}
}

func TestConnCloseHook_CloseIsIdempotentAndConcurrentSafe(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	var onCloseCount atomic.Int32
	hook := &connCloseHook{
		Conn: client,
		onClose: func() {
			onCloseCount.Add(1)
		},
	}

	const closers = 32
	var wg sync.WaitGroup
	wg.Add(closers)
	for i := 0; i < closers; i++ {
		go func() {
			defer wg.Done()
			_ = hook.Close()
		}()
	}
	wg.Wait()

	if got := onCloseCount.Load(); got != 1 {
		t.Fatalf("onClose called %d times, want 1", got)
	}
}

type delayedDialOutbound struct {
	adapter.Outbound
	delay time.Duration
}

func (o delayedDialOutbound) DialContext(ctx context.Context, network string, dest M.Socksaddr) (net.Conn, error) {
	timer := time.NewTimer(o.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var dialer net.Dialer
	return dialer.DialContext(ctx, network, dest.String())
}

func TestHTTPGetViaOutbound_LatencyIncludesDial(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	const dialDelay = 40 * time.Millisecond
	_, latency, err := HTTPGetViaOutbound(context.Background(), delayedDialOutbound{delay: dialDelay}, srv.URL, OutboundHTTPOptions{})
	if err != nil {
		t.Fatalf("HTTPGetViaOutbound: %v", err)
	}
	if latency < dialDelay {
		t.Fatalf("latency %v excludes dial delay %v", latency, dialDelay)
	}
}

func TestHTTPGetViaOutbound_HTTPSLatencyIncludesDial(t *testing.T) {
	// Run with a fresh root pool so the test trusts only its local HTTPS server
	// without changing process-global TLS settings for other tests.
	if url := os.Getenv("RESIN_TEST_HTTPS_LATENCY_URL"); url != "" {
		const dialDelay = 40 * time.Millisecond
		_, latency, err := HTTPGetViaOutbound(context.Background(), delayedDialOutbound{delay: dialDelay}, url, OutboundHTTPOptions{})
		if err != nil {
			t.Fatalf("HTTPS HTTPGetViaOutbound: %v", err)
		}
		if latency < dialDelay {
			t.Fatalf("TLS latency %v excludes dial delay %v", latency, dialDelay)
		}
		return
	}

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	rootFile := filepath.Join(t.TempDir(), "root.pem")
	rootPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(rootFile, rootPEM, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestHTTPGetViaOutbound_HTTPSLatencyIncludesDial$")
	cmd.Env = append(os.Environ(), "RESIN_TEST_HTTPS_LATENCY_URL="+srv.URL, "SSL_CERT_FILE="+rootFile)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("HTTPS latency regression test: %v\n%s", err, output)
	}
}
