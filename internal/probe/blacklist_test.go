package probe

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/topology"
)

func TestBlacklist_PeriodicVerificationDoesNotWaitForLongBan(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{MaxLatencyTableEntries: 16})
	h := node.HashFromRawOptions([]byte(`{"probe":"blacklist"}`))
	pool.AddNodeFromSub(h, []byte(`{"probe":"blacklist"}`), "sub1")
	e, _ := pool.GetEntry(h)
	storeOutbound(e)
	calls := 0
	m := NewProbeManager(ProbeConfig{Pool: pool, Fetcher: func(node.Hash, string) ([]byte, time.Duration, error) {
		calls++
		return []byte("ip=1.1.1.1\nloc=US\n"), time.Millisecond, nil
	}})
	now := time.Now()
	e.BlacklistUntil.Store(now.Add(6 * time.Hour).UnixNano())
	e.LastLatencyProbeAttempt.Store(now.UnixNano())
	if m.isLatencyProbeDue(e, now, time.Hour, 3*time.Hour, nil, 15*time.Second) {
		t.Fatal("verification retried without spacing")
	}
	m.executeTask(probeTask{key: probeTaskKey{hash: h, kind: probeTaskKindLatency}})
	if calls != 0 {
		t.Fatal("queued task bypassed retry spacing")
	}
	e.LastLatencyProbeAttempt.Store(now.Add(-6 * time.Minute).UnixNano())
	e.CircuitOpenSince.Store(now.Add(-6 * time.Minute).UnixNano())
	if !m.isLatencyProbeDue(e, now, time.Hour, 3*time.Hour, nil, 15*time.Second) {
		t.Fatal("long quarantine prevented timely verification")
	}
	m.executeTask(probeTask{key: probeTaskKey{hash: h, kind: probeTaskKindEgress}})
	if calls != 0 {
		t.Fatal("egress queue duplicated recovery verification")
	}
	m.executeTask(probeTask{key: probeTaskKey{hash: h, kind: probeTaskKindLatency}})
	if calls != 2 || e.BlacklistUntil.Load() != 0 {
		t.Fatal("two-site verification did not recover before blacklist expiry")
	}
	e.BlacklistUntil.Store(now.Add(-time.Second).UnixNano())
	e.LastLatencyProbeAttempt.Store(now.UnixNano())
	if m.isLatencyProbeDue(e, now, time.Hour, 3*time.Hour, nil, 15*time.Second) {
		t.Fatal("expired blacklist created unbounded retry loop")
	}
}

func TestBlacklist_TwoSiteVerificationRecoversBeforeExpiry(t *testing.T) {
	for _, secondFails := range []bool{false, true} {
		t.Run(fmt.Sprint(secondFails), func(t *testing.T) {
			pool := topology.NewGlobalNodePool(topology.PoolConfig{MaxLatencyTableEntries: 16})
			h := node.HashFromRawOptions([]byte(`{"probe":"recovery"}`))
			pool.AddNodeFromSub(h, []byte(`{"probe":"recovery"}`), "sub1")
			e, _ := pool.GetEntry(h)
			storeOutbound(e)
			pool.RecordResult(h, false)
			calls := []string{}
			m := NewProbeManager(ProbeConfig{Pool: pool, Fetcher: func(_ node.Hash, url string) ([]byte, time.Duration, error) {
				calls = append(calls, url)
				if url == egressTraceURL {
					if secondFails {
						return nil, 0, errors.New("second site unavailable")
					}
					return []byte("ip=1.1.1.1\nloc=US\n"), time.Millisecond, nil
				}
				return nil, time.Millisecond, nil
			}})
			err := m.performLatencyProbe(h, defaultLatencyTestURL)
			if len(calls) != 2 || calls[0] == calls[1] {
				t.Fatalf("recovery needs independent HTTPS sites: %v", calls)
			}
			if (err != nil) != secondFails {
				t.Fatalf("err=%v secondFails=%v", err, secondFails)
			}
			if e.IsCircuitOpen() != secondFails {
				t.Fatalf("still blocked=%v, want %v", e.IsCircuitOpen(), secondFails)
			}
		})
	}
}

func TestBlacklist_ConcurrentFailureInvalidatesVerification(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{MaxLatencyTableEntries: 16})
	h := node.HashFromRawOptions([]byte(`{"probe":"stale-recovery"}`))
	pool.AddNodeFromSub(h, []byte(`{}`), "sub1")
	e, _ := pool.GetEntry(h)
	storeOutbound(e)
	pool.RecordResult(h, false)
	m := NewProbeManager(ProbeConfig{Pool: pool, Fetcher: func(_ node.Hash, url string) ([]byte, time.Duration, error) {
		if url == egressTraceURL {
			pool.RecordPassiveResult("", h, false)
		}
		return []byte("ip=1.1.1.1\nloc=US\n"), time.Millisecond, nil
	}})
	if err := m.performLatencyProbe(h, defaultLatencyTestURL); err == nil {
		t.Fatal("stale verification was accepted")
	}
	if !e.IsCircuitOpen() {
		t.Fatal("verification erased concurrent failure")
	}
}
