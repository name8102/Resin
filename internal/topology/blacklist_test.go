package topology

import (
	"testing"
	"time"
)

func TestBlacklist_FirstPassiveFailureCannotBeReopenedByProbe(t *testing.T) {
	pool, subMgr := newHealthTestPool()
	h := addTestNode(pool, subMgr.Lookup("s1"), `{"type":"ss","blacklist":"first"}`)
	entry, _ := pool.GetEntry(h)
	pool.RecordResult(h, true)
	pool.RecordPassiveResult("", h, false)
	if !entry.IsCircuitOpen() {
		t.Fatal("first upstream failure did not immediately exclude node")
	}
	pool.RecordResult(h, true)
	if !entry.IsCircuitOpen() {
		t.Fatal("successful probe reopened blacklisted node")
	}
}

func TestBlacklist_RecoveryRequiresExpiryAndActiveProbe(t *testing.T) {
	pool, subMgr := newHealthTestPool()
	h := addTestNode(pool, subMgr.Lookup("s1"), `{"type":"ss","blacklist":"recovery"}`)
	entry, _ := pool.GetEntry(h)
	pool.RecordResult(h, true)
	pool.RecordPassiveResult("", h, false)
	entry.BlacklistUntil.Store(time.Now().Add(-time.Second).UnixNano())
	pool.RecordPassiveResult("", h, true)
	if !entry.IsCircuitOpen() || entry.BlacklistUntil.Load() == 0 {
		t.Fatal("old passive connection rehabilitated expired blacklist")
	}
	pool.RecordResult(h, true)
	if entry.IsCircuitOpen() || entry.BlacklistUntil.Load() != 0 {
		t.Fatal("active success after expiry did not rehabilitate node")
	}
}

func TestBlacklist_ProbeFailuresAndBackoff(t *testing.T) {
	pool, subMgr := newHealthTestPool()
	h := addTestNode(pool, subMgr.Lookup("s1"), `{"type":"ss","blacklist":"backoff"}`)
	entry, _ := pool.GetEntry(h)
	pool.RecordResult(h, true)
	for i, want := range []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour, 24 * time.Hour} {
		pool.RecordResult(h, false)
		remaining := time.Until(time.Unix(0, entry.BlacklistUntil.Load()))
		if remaining < want-time.Second || remaining > want {
			t.Fatalf("strike %d: duration=%v want=%v", i+1, remaining, want)
		}
		deadline := entry.BlacklistUntil.Load()
		strikes := entry.BlacklistStrikes.Load()
		pool.RecordResult(h, false)
		if entry.BlacklistUntil.Load() != deadline || entry.BlacklistStrikes.Load() != strikes {
			t.Fatal("in-flight failures continuously extended blacklist")
		}
		pool.RecordResult(h, true)
		if !entry.IsCircuitOpen() {
			t.Fatal("successful probe bypassed quarantine")
		}
		entry.BlacklistUntil.Store(time.Now().Add(-time.Second).UnixNano())
		pool.RecordResult(h, true)
		if entry.IsCircuitOpen() {
			t.Fatal("active verification after quarantine did not recover node")
		}
	}
}
