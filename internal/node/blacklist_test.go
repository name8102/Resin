package node

import (
	"testing"
	"time"
)

func TestBlacklist_UntilMarkerRequiresExplicitRecovery(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)
	for _, until := range []time.Time{time.Now().Add(time.Hour), time.Now().Add(-time.Hour)} {
		e.BlacklistUntil.Store(until.UnixNano())
		if !e.IsCircuitOpen() {
			t.Fatal("blacklist marker ignored without circuit timestamp")
		}
	}
	e.BlacklistUntil.Store(0)
	if e.IsCircuitOpen() {
		t.Fatal("cleared node still circuit open")
	}
}
