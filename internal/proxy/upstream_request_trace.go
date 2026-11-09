package proxy

import (
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

// upstreamRequestTrace captures request-progress milestones reported by
// net/http transport so request-log egress bytes can be committed only when
// the request has actually been written to upstream.
type upstreamRequestTrace struct {
	requestStart         time.Time
	firstResponseNs      atomic.Int64
	gotConn              atomic.Bool
	wroteRequest         atomic.Bool
	gotFirstResponseByte func()
}

func newUpstreamRequestTrace(gotFirstResponseByte ...func()) *upstreamRequestTrace {
	trace := &upstreamRequestTrace{requestStart: time.Now()}
	if len(gotFirstResponseByte) > 0 {
		trace.gotFirstResponseByte = gotFirstResponseByte[0]
	}
	return trace
}

func (t *upstreamRequestTrace) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			t.gotConn.Store(true)
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			// Only mark as written when transport reports write success.
			// WroteRequest can also fire with Err!=nil for failed write attempts.
			if info.Err == nil {
				t.wroteRequest.Store(true)
			}
		},
		GotFirstResponseByte: func() {
			t.firstResponseNs.CompareAndSwap(0, int64(time.Since(t.requestStart)))
			if t.gotFirstResponseByte != nil {
				t.gotFirstResponseByte()
			}
		},
	}
}

func (t *upstreamRequestTrace) shouldCommitEgress() bool {
	return t.gotConn.Load() && t.wroteRequest.Load()
}

func (t *upstreamRequestTrace) responseLatency() time.Duration {
	return time.Duration(t.firstResponseNs.Load())
}
