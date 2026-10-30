package outbound

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/sagernet/sing-box/adapter"
	sbOutbound "github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	sHTTP "github.com/sagernet/sing/protocol/http"
)

// The upstream HTTP client only passes ctx to the transport dialer. Protect
// its CONNECT write/read as well, without changing its protocol implementation.
type httpContextOutbound struct {
	sbOutbound.Adapter
	logger log.ContextLogger
	client *sHTTP.Client
}

func newHTTPContextOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.HTTPOutboundOptions) (adapter.Outbound, error) {
	outboundDialer, err := dialer.New(ctx, options.DialerOptions, options.ServerIsDomain())
	if err != nil {
		return nil, err
	}
	transport, err := tls.NewDialerFromOptions(ctx, router, outboundDialer, options.Server, common.PtrValueOrDefault(options.TLS))
	if err != nil {
		return nil, err
	}
	return &httpContextOutbound{
		Adapter: sbOutbound.NewAdapterWithDialerOptions(C.TypeHTTP, tag, []string{N.NetworkTCP}, options.DialerOptions),
		logger:  logger,
		client: sHTTP.NewClient(sHTTP.Options{
			Dialer: httpHandshakeDialer{transport}, Server: options.ServerOptions.Build(),
			Username: options.Username, Password: options.Password,
			Path: options.Path, Headers: options.Headers.Build(),
		}),
	}, nil
}

func (h *httpContextOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	ctx, metadata := adapter.ExtendContext(ctx)
	metadata.Outbound = h.Tag()
	metadata.Destination = destination
	h.logger.InfoContext(ctx, "outbound connection to ", destination)

	var handshake httpHandshake
	conn, err := h.client.DialContext(context.WithValue(ctx, httpHandshakeKey{}, &handshake), network, destination)
	if handshake.stop != nil {
		if !handshake.stop() {
			// AfterFunc may already be closing the connection. Join it before
			// deciding whether this connection can be handed to the relay.
			<-handshake.done
		}
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	} else if deadline, ok := ctx.Deadline(); err != nil && ok && !time.Now().Before(deadline) {
		// A socket deadline can fire just before ctx's timer publishes Err.
		err = context.DeadlineExceeded
	}
	if err == nil {
		// The dial context owns only connection setup, not the relay lifetime.
		err = handshake.conn.SetDeadline(time.Time{})
	}
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, err
	}
	return conn, nil
}

func (h *httpContextOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}

type httpHandshakeKey struct{}

type httpHandshake struct {
	conn net.Conn
	stop func() bool
	done chan struct{}
}

type httpHandshakeDialer struct{ N.Dialer }

func (d httpHandshakeDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	conn, err := d.Dialer.DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			conn.Close()
			return nil, err
		}
	}
	handshake := ctx.Value(httpHandshakeKey{}).(*httpHandshake)
	handshake.conn = conn
	handshake.done = make(chan struct{})
	handshake.stop = context.AfterFunc(ctx, func() {
		conn.Close()
		close(handshake.done)
	})
	return conn, nil
}
