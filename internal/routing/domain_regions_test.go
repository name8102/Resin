package routing

import (
	"errors"
	"fmt"
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

func TestDomainRegionsRequestedPolicy(t *testing.T) {
	r, pool, p, us := targetTestRouter(t)
	usEntry, _ := pool.GetEntry(us)
	usEntry.SetEgressRegion("us")
	for i, region := range []string{"jp", "kr", "cn", ""} {
		h, e := newRoutableEntry(t, `{"region":"`+region+`"}`, fmt.Sprintf("203.0.113.%d", i+2))
		e.SetEgressRegion(region)
		pool.addEntry(h, e)
	}
	p.RegionFilters = []string{"!cn"}
	p.DomainRegionRules = []model.DomainRegionRule{
		{Domain: "dlsite.com", Regions: []string{"jp"}},
		{Domain: "e-hentai.org", Regions: []string{"!jp", "!kr"}},
		{Domain: "exhentai.org", Regions: []string{"!jp", "!kr"}},
		{Domain: "esjzone.cc", Regions: []string{"!jp", "!kr"}},
		{Domain: "*.wild.test", Regions: []string{"!jp", "!kr"}},
	}
	pool.rebuildPlatformView(p)
	for _, account := range []string{"", "same-account"} {
		for _, host := range []string{"dlsite.com", "www.dlsite.com", "e-hentai.org", "forums.e-hentai.org", "exhentai.org", "www.esjzone.cc", "nested.cdn.wild.test", "ordinary.test"} {
			got, err := r.RouteRequest(p.Name, account, host+":443")
			if err != nil {
				t.Fatal(err)
			}
			entry, _ := pool.GetEntry(got.NodeHash)
			region := entry.GetRegion(r.geoLookup)
			want := "us"
			if host == "dlsite.com" || host == "www.dlsite.com" {
				want = "jp"
			}
			if host == "ordinary.test" {
				if region == "" || region == "cn" {
					t.Fatalf("%s: selected excluded region %q", host, region)
				}
			} else if region != want {
				t.Fatalf("%s: got %q want %q", host, region, want)
			}
		}
	}
}
