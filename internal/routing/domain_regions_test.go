package routing

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
)

func TestDomainRegionsConstrainAnonymousStickyAndRetries(t *testing.T) {
	r, pool, p, us := targetTestRouter(t)
	usEntry, _ := pool.GetEntry(us)
	usEntry.SetEgressRegion("us")
	jp, jpEntry := newRoutableEntry(t, `{"region":"jp"}`, "203.0.113.2")
	jpEntry.SetEgressRegion("jp")
	pool.addEntry(jp, jpEntry)
	pool.rebuildPlatformView(p)
	p.DomainRegionRules = []model.DomainRegionRule{
		{Domain: "example.com", Regions: []string{"us"}},
		{Domain: "api.example.com", Regions: []string{"jp"}},
	}
	for _, account := range []string{"", "sticky-user"} {
		for _, tc := range []struct {
			target string
			want   node.Hash
		}{
			{"api.example.com:443", jp}, {"https://cdn.example.com/path", us},
			{"api.example.com:443", jp}, {"EXAMPLE.COM.:80", us},
		} {
			got, err := r.RouteRequest(p.Name, account, tc.target)
			if err != nil || got.NodeHash != tc.want {
				t.Fatalf("%s/%s: route=%v err=%v", account, tc.target, got.NodeHash, err)
			}
		}
		if _, err := r.RouteRequest(p.Name, account, "example.com:443", us); !errors.Is(err, ErrNoAvailableNodes) {
			t.Fatalf("retry escaped region filter: %v", err)
		}
	}
	p.DomainRegionRules = []model.DomainRegionRule{{Domain: "empty.test", Regions: []string{"hk"}}}
	if _, err := r.RouteRequest(p.Name, "", "empty.test:443"); !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("missing region leaked: %v", err)
	}
}

func TestDomainRegionsUseExitGeoIPAndKeepPlatformFilters(t *testing.T) {
	r, pool, p, h := targetTestRouter(t)
	p.DomainRegionRules = []model.DomainRegionRule{{Domain: "example.com", Regions: []string{"us"}}}
	r.geoLookup = func(netip.Addr) string { return "us" }
	if _, err := r.RouteRequest(p.Name, "", "example.com:443"); err != nil {
		t.Fatal(err)
	}
	e, _ := pool.GetEntry(h)
	e.SetEgressRegion("jp")
	if _, err := r.RouteRequest(p.Name, "", "example.com:443"); !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("probe region did not take precedence: %v", err)
	}
	p.RegionFilters = []string{"!jp"}
	pool.rebuildPlatformView(p)
	p.DomainRegionRules = []model.DomainRegionRule{{Domain: "example.com", Regions: []string{"jp"}}}
	if _, err := r.RouteRequest(p.Name, "", "example.com:443"); !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("domain rule bypassed platform: %v", err)
	}
}
