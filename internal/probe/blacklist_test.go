package probe

import (
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/topology"
)

func TestBlacklist_QueuedProbesWaitUntilExpiry(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{MaxLatencyTableEntries: 16})
	h := node.HashFromRawOptions([]byte(`{"probe":"blacklist"}`))
	pool.AddNodeFromSub(h, []byte(`{"probe":"blacklist"}`), "sub1")
	e, _ := pool.GetEntry(h)
	storeOutbound(e)
	calls := 0
	m := NewProbeManager(ProbeConfig{Pool: pool, Fetcher: func(node.Hash, string) ([]byte, time.Duration, error) {
		calls++
		return []byte("ok"), time.Millisecond, nil
	}})
	now := time.Now()
	e.BlacklistUntil.Store(now.Add(time.Hour).UnixNano())
	if m.isLatencyProbeDue(e, now, time.Hour, 3*time.Hour, nil, 15*time.Second) {
		t.Fatal("quarantined node scheduled before expiry")
	}
	m.executeTask(probeTask{key: probeTaskKey{hash: h, kind: probeTaskKindLatency}})
	if calls != 0 {
		t.Fatal("queued probe bypassed quarantine")
	}
	e.BlacklistUntil.Store(now.Add(-time.Second).UnixNano())
	e.LastLatencyProbeAttempt.Store(now.UnixNano())
	if !m.isLatencyProbeDue(e, now, time.Hour, 3*time.Hour, nil, 15*time.Second) {
		t.Fatal("expired blacklist did not schedule verification")
	}
	m.executeTask(probeTask{key: probeTaskKey{hash: h, kind: probeTaskKindLatency}})
	if calls != 1 || e.BlacklistUntil.Load() != 0 {
		t.Fatal("active verification did not rehabilitate expired node")
	}
}
