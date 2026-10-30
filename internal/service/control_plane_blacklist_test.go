package service

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func TestNodeSummary_Blacklist(t *testing.T) {
	cp := &ControlPlaneService{}
	entry := node.NewNodeEntry(node.Hash{}, nil, time.Now(), 0)
	for _, until := range []int64{time.Now().Add(time.Hour).UnixNano(), time.Now().Add(-time.Hour).UnixNano()} {
		entry.BlacklistUntil.Store(until)
		entry.BlacklistStrikes.Store(3)
		summary := cp.nodeEntryToSummary(entry.Hash, entry)
		want := time.Unix(0, until).UTC().Format(time.RFC3339Nano)
		if summary.BlacklistUntil == nil || *summary.BlacklistUntil != want || summary.BlacklistStrikes != 3 {
			t.Fatalf("blacklist summary: %+v, want until=%s strikes=3", summary, want)
		}
		body, err := json.Marshal(summary)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Fatal(err)
		}
		if fields["blacklist_until"] != want || fields["blacklist_strikes"] != float64(3) {
			t.Fatalf("blacklist JSON: %s", body)
		}
	}
	entry.BlacklistUntil.Store(0)
	entry.BlacklistStrikes.Store(0)
	summary := cp.nodeEntryToSummary(entry.Hash, entry)
	body, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["blacklist_until"]; ok {
		t.Fatalf("cleared blacklist_until should be omitted: %s", body)
	}
	if _, ok := fields["blacklist_strikes"]; ok {
		t.Fatalf("zero blacklist_strikes should be omitted: %s", body)
	}
}

func TestNodeSummary_BlacklistExcludedFromHealthyAggregates(t *testing.T) {
	until := time.Now().Add(-time.Minute).Format(time.RFC3339Nano)
	n := NodeSummary{Enabled: true, HasOutbound: true, BlacklistUntil: &until}
	if n.IsHealthyAndEnabled() {
		t.Fatal("blacklisted node counted as available")
	}
	n.BlacklistUntil = nil
	if !n.IsHealthyAndEnabled() {
		t.Fatal("unblocked ready node not healthy")
	}
}
