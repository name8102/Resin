package platform

import (
	"reflect"
	"testing"

	"github.com/Resinat/Resin/internal/model"
)

func TestDomainRegionRulesNormalizeAndMatch(t *testing.T) {
	rules, err := NormalizeDomainRegionRules([]model.DomainRegionRule{
		{Domain: " Example.COM. ", Regions: []string{"us", "!hk"}},
		{Domain: "api.example.com", Regions: []string{"jp"}},
		{Domain: "例子.测试", Regions: []string{"sg"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := &Platform{DomainRegionRules: rules}
	for _, tc := range []struct {
		host string
		want []string
	}{
		{"EXAMPLE.COM.", []string{"us", "!hk"}},
		{"cdn.example.com", []string{"us", "!hk"}},
		{"api.example.com", []string{"jp"}},
		{"v1.api.example.com", []string{"jp"}},
		{"badexample.com", nil},
		{"example.com.evil.test", nil},
		{"例子.测试", []string{"sg"}},
		{"192.0.2.1", nil},
	} {
		if got := p.RegionsForDomain(tc.host); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.host, got, tc.want)
		}
	}
}

func TestDomainRegionRulesRejectInvalid(t *testing.T) {
	for _, rules := range [][]model.DomainRegionRule{
		{{Domain: "https://example.com", Regions: []string{"us"}}},
		{{Domain: "example.com:443", Regions: []string{"us"}}},
		{{Domain: "*.example.com", Regions: []string{"us"}}},
		{{Domain: "192.0.2.1", Regions: []string{"us"}}},
		{{Domain: "bad..example", Regions: []string{"us"}}},
		{{Domain: "example.com", Regions: nil}},
		{{Domain: "example.com", Regions: []string{"USA"}}},
		{{Domain: "example.com", Regions: []string{"us"}}, {Domain: "EXAMPLE.COM.", Regions: []string{"jp"}}},
	} {
		if _, err := NormalizeDomainRegionRules(rules); err == nil {
			t.Errorf("accepted %+v", rules)
		}
	}
}
