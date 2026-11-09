package state

import (
	"reflect"
	"testing"

	"github.com/Resinat/Resin/internal/model"
)

func TestPlatformDomainRegionsRoundTrip(t *testing.T) {
	r := newTestStateRepo(t)
	p := model.Platform{ID: "domain-rules", Name: "Domain-Rules", StickyTTLNs: 1000, ReverseProxyMissAction: "TREAT_AS_EMPTY", AllocationPolicy: "BALANCED", DomainRegionRules: []model.DomainRegionRule{{Domain: "EXAMPLE.COM.", Regions: []string{"us", "jp"}}}}
	if err := r.UpsertPlatform(p); err != nil {
		t.Fatal(err)
	}
	want := []model.DomainRegionRule{{Domain: "example.com", Regions: []string{"us", "jp"}}}
	got, err := r.GetPlatform(p.ID)
	if err != nil || !reflect.DeepEqual(got.DomainRegionRules, want) {
		t.Fatalf("get=%+v err=%v", got, err)
	}
	listed, err := r.ListPlatforms()
	if err != nil || len(listed) != 1 || !reflect.DeepEqual(listed[0].DomainRegionRules, want) {
		t.Fatalf("list=%+v err=%v", listed, err)
	}
	p.DomainRegionRules = nil
	if err := r.UpsertPlatform(p); err != nil {
		t.Fatal(err)
	}
	got, err = r.GetPlatform(p.ID)
	if err != nil || len(got.DomainRegionRules) != 0 {
		t.Fatalf("clear=%+v err=%v", got, err)
	}
}
