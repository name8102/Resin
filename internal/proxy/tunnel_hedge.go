package proxy

import (
	"context"
	"net"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
	M "github.com/sagernet/sing/common/metadata"
)

const (
	maxAnonymousConnectAttempts = 8
	anonymousConnectBudget      = 4300 * time.Millisecond
	connectHedgeDelay           = 200 * time.Millisecond
)

type connectCandidate struct {
	routed      routedOutbound
	conn        net.Conn
	err         error
	dialElapsed time.Duration
}

// Anonymous traffic has no lease to preserve. Overlap its connection setup so
// slow-but-working handshakes get a full window while a fast backup can win.
// No application bytes have been sent; only the winning connection is relayed.
func prepareHedgedConnectTunnel(ctx context.Context, deps tunnelDeps, platformName, target string) tunnelPrepareResult {
	budgetCtx, cancel := context.WithTimeout(ctx, anonymousConnectBudget)
	done := make(chan struct{})
	defer func() { cancel(); close(done) }()
	results := make(chan connectCandidate)
	active := make(map[node.Hash]routing.RouteResult)
	var excluded []node.Hash
	var last tunnelPrepareResult
	var lastRoute routing.RouteResult
	exhausted := false
	launch := func() bool {
		routed, routeErr := resolveRoutedOutbound(deps.router, deps.pool, platformName, "", target, excluded...)
		if routeErr != nil {
			exhausted = true
			if len(excluded) == 0 {
				last.proxyErr = routeErr
			}
			return false
		}
		excluded = append(excluded, routed.Route.NodeHash)
		active[routed.Route.NodeHash] = routed.Route
		lastRoute = routed.Route
		if deps.health != nil {
			go deps.health.RecordPassiveLatency(routed.Route.NodeHash, netutil.ExtractDomain(target), nil)
		}
		go func() {
			dialStart := time.Now()
			conn, err := routed.Outbound.DialContext(budgetCtx, "tcp", M.ParseSocksaddr(target))
			candidate := connectCandidate{routed, conn, err, time.Since(dialStart)}
			// An unbuffered handoff lets every losing/late connection observe done and
			// close itself; nothing can be stranded in a result buffer after a winner.
			select {
			case results <- candidate:
			case <-done:
				if conn != nil {
					_ = conn.Close()
				}
			}
		}()
		return true
	}
	if err := ctx.Err(); err != nil {
		return tunnelPrepareResult{proxyErr: classifyConnectError(err), canceled: true}
	}
	if !launch() {
		return last
	}
	timer := time.NewTimer(connectHedgeDelay)
	defer timer.Stop()
	recordFailure := func(route routing.RouteResult) {
		deps.router.ReportTargetFailure(route, target)
		recordPassiveResult(deps.health, route, target, false)
	}
	for len(active) > 0 {
		select {
		case candidate := <-results:
			delete(active, candidate.routed.Route.NodeHash)
			if err := ctx.Err(); err != nil {
				if candidate.conn != nil {
					_ = candidate.conn.Close()
				}
				return tunnelPrepareResult{route: candidate.routed.Route, proxyErr: classifyConnectError(err), canceled: true}
			}
			if candidate.err == nil {
				return finishConnectTunnel(deps, candidate.routed, candidate.conn, target, candidate.dialElapsed)
			}
			if candidate.conn != nil {
				_ = candidate.conn.Close()
			}
			proxyErr := classifyConnectError(candidate.err)
			last = tunnelPrepareResult{route: candidate.routed.Route, proxyErr: proxyErr, upstreamStage: "connect_dial", upstreamErr: candidate.err, canceled: proxyErr == nil}
			if proxyErr != nil {
				recordFailure(candidate.routed.Route)
			}
			if len(excluded) < maxAnonymousConnectAttempts && !exhausted {
				launch()
				timer.Reset(connectHedgeDelay)
			}
		case <-timer.C:
			if len(excluded) < maxAnonymousConnectAttempts && !exhausted {
				launch()
				timer.Reset(connectHedgeDelay)
			}
		case <-budgetCtx.Done():
			if err := ctx.Err(); err != nil {
				return tunnelPrepareResult{route: lastRoute, proxyErr: classifyConnectError(err), canceled: true}
			}
			// Feedback is synchronous even when socket deadlines and the budget fire
			// together. Canceled losers after a success never take this path.
			for _, route := range active {
				recordFailure(route)
			}
			return tunnelPrepareResult{route: lastRoute, proxyErr: ErrUpstreamTimeout, upstreamStage: "connect_dial", upstreamErr: budgetCtx.Err()}
		}
	}
	return last
}
