package outbound

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

func startHTTPContextProxy(t *testing.T, serve func(net.Conn, *http.Request)) (string, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	requested := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() {
		listener.Close()
		select {
		case conn := <-accepted:
			conn.Close()
		default:
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("HTTP proxy did not stop")
		}
	})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		request, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		close(requested)
		serve(conn, request)
	}()
	return listener.Addr().String(), requested
}

func TestHTTPContextHandshakeDeadline(t *testing.T) {
	testHTTPContextHandshakeInterrupted(t, true)
}

func TestHTTPContextHandshakeCancel(t *testing.T) {
	testHTTPContextHandshakeInterrupted(t, false)
}

func testHTTPContextHandshakeInterrupted(t *testing.T, deadline bool) {
	closed := make(chan struct{})
	address, requested := startHTTPContextProxy(t, func(conn net.Conn, _ *http.Request) {
		io.Copy(io.Discard, conn)
		close(closed)
	})
	b := newTestSingboxBuilder(t)
	t.Cleanup(func() { b.Close() })
	server := M.ParseSocksaddr(address)
	options, _ := json.Marshal(map[string]any{"type": "http", "server": server.Addr.String(), "server_port": server.Port})
	outbound, err := b.Build(options)
	if err != nil {
		t.Fatal(err)
	}
	var ctx context.Context
	var cancel context.CancelFunc
	wantErr := context.Canceled
	if deadline {
		ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
		wantErr = context.DeadlineExceeded
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()
	result := make(chan error, 1)
	go func() {
		conn, err := outbound.DialContext(ctx, "tcp", M.ParseSocksaddr("example.com:443"))
		if conn != nil {
			conn.Close()
		}
		result <- err
	}()
	select {
	case <-requested:
	case <-time.After(time.Second):
		t.Fatal("proxy did not receive CONNECT")
	}
	if !deadline {
		cancel()
	}
	select {
	case err := <-result:
		if !errors.Is(err, wantErr) {
			t.Fatalf("DialContext error = %v, want %v", err, wantErr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HTTP CONNECT handshake ignored context interruption")
	}
	select {
	case <-closed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("interrupted handshake did not close the proxy connection")
	}
}

func TestHTTPContextHandshakeReleasesConnection(t *testing.T) {
	for _, mode := range []string{"host", "path"} {
		t.Run(mode, func(t *testing.T) {
			requests := make(chan *http.Request, 1)
			address, _ := startHTTPContextProxy(t, func(conn net.Conn, request *http.Request) {
				requests <- request
				if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
					return
				}
				io.Copy(conn, conn)
			})
			b := newTestSingboxBuilder(t)
			defer b.Close()
			server := M.ParseSocksaddr(address)
			headers := map[string]string{"X-Resin-Test": "preserved"}
			options := map[string]any{
				"type": "http", "tag": "context-http", "server": server.Addr.String(), "server_port": server.Port,
				"username": "alice", "password": "secret", "headers": headers,
			}
			if mode == "host" {
				headers["Host"] = "proxy.example"
			} else {
				options["path"] = "/tunnel"
			}
			raw, _ := json.Marshal(options)
			outbound, err := b.Build(raw)
			if err != nil {
				t.Fatal(err)
			}
			if outbound.Type() != "http" || outbound.Tag() != "context-http" || len(outbound.Network()) != 1 || outbound.Network()[0] != "tcp" {
				t.Fatalf("HTTP outbound metadata changed: %v", outbound)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			conn, err := outbound.DialContext(ctx, "tcp", M.ParseSocksaddr("example.com:443"))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			request := <-requests
			if request.Method != http.MethodConnect || request.Header.Get("X-Resin-Test") != "preserved" || request.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:secret")) {
				t.Fatalf("CONNECT options changed: %+v", request)
			}
			if mode == "host" && request.RequestURI != "example.com:443" {
				t.Fatalf("CONNECT destination = %q", request.RequestURI)
			}
			if mode == "path" && request.RequestURI != "/tunnel" {
				t.Fatalf("CONNECT path = %q", request.RequestURI)
			}
			cancel()
			timer := time.NewTimer(150 * time.Millisecond)
			<-timer.C
			if _, err := io.WriteString(conn, "ping"); err != nil {
				t.Fatalf("relay write after cancel/deadline: %v", err)
			}
			var response [4]byte
			if _, err := io.ReadFull(conn, response[:]); err != nil {
				t.Fatalf("relay read after cancel/deadline: %v", err)
			}
			if string(response[:]) != "ping" {
				t.Fatalf("relay response = %q", response)
			}
		})
	}
}
