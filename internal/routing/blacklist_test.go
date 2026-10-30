package routing

import (
	"errors"
	"testing"
	"time"
)

func TestBlacklist_RejectsStaleViewAcrossTargetsAndLeases(t *testing.T) {
	r, pool, p, h := targetTestRouter(t)
	if _, err := r.RouteRequest(p.Name, "device", "api.github.com:443"); err != nil {
		t.Fatal(err)
	}
	entry, _ := pool.GetEntry(h)
	// The platform's old view still contains this node. Even an expired blacklist
	// is ineligible until active verification explicitly clears its marker.
	entry.BlacklistUntil.Store(time.Now().Add(-time.Second).UnixNano())
	for _, account := range []string{"", "device"} {
		for _, target := range []string{"api.github.com:443", "github.com:443", "example.org:80"} {
			if _, err := r.RouteRequest(p.Name, account, target); !errors.Is(err, ErrNoAvailableNodes) {
				t.Fatalf("blacklist bypass: account=%q target=%s err=%v", account, target, err)
			}
		}
	}
	entry.BlacklistUntil.Store(0)
	if _, err := r.RouteRequest(p.Name, "", "api.github.com:443"); err != nil {
		t.Fatalf("verified node not usable: %v", err)
	}
}
