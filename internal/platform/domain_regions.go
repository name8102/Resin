package platform

import (
	"fmt"
	"net/netip"
	"strings"

	"github.com/Resinat/Resin/internal/model"
	"golang.org/x/net/idna"
)

func normalizeRuleDomain(raw string) (string, error) {
	domain, err := idna.Lookup.ToASCII(strings.TrimSuffix(strings.ToLower(strings.TrimSpace(raw)), "."))
	if err != nil || domain == "" || len(domain) > 253 || strings.ContainsAny(domain, "/:@*?#") {
		return "", fmt.Errorf("must be a DNS domain without a URL, port or wildcard")
	}
	if _, err := netip.ParseAddr(domain); err == nil {
		return "", fmt.Errorf("must be a DNS domain, not an IP address")
	}
	for _, label := range strings.Split(domain, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", fmt.Errorf("invalid DNS label")
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return "", fmt.Errorf("invalid DNS label")
			}
		}
	}
	return domain, nil
}

func NormalizeDomainRegionRules(rules []model.DomainRegionRule) ([]model.DomainRegionRule, error) {
	normalized := model.CloneDomainRegionRules(rules)
	seen := make(map[string]bool, len(rules))
	for i := range normalized {
		rule := &normalized[i]
		domain, err := normalizeRuleDomain(rule.Domain)
		if err != nil {
			return nil, fmt.Errorf("domain_region_rules[%d].domain: %w", i, err)
		}
		if seen[domain] {
			return nil, fmt.Errorf("domain_region_rules[%d].domain: duplicate domain %q", i, domain)
		}
		seen[domain] = true
		rule.Domain = domain
		if len(rule.Regions) == 0 {
			return nil, fmt.Errorf("domain_region_rules[%d].regions: at least one region filter is required", i)
		}
		if err := ValidateRegionFilters(rule.Regions); err != nil {
			return nil, fmt.Errorf("domain_region_rules[%d].regions: %w", i, err)
		}
	}
	return normalized, nil
}

// RegionsForDomain returns the most specific suffix rule, with DNS label boundaries.
// A nil result leaves the platform's ordinary routing unchanged.
func (p *Platform) RegionsForDomain(host string) []string {
	host, err := normalizeRuleDomain(host)
	if err != nil {
		return nil
	}
	var regions []string
	length := 0
	for _, rule := range p.DomainRegionRules {
		if len(rule.Domain) > length && (host == rule.Domain || strings.HasSuffix(host, "."+rule.Domain)) {
			regions, length = rule.Regions, len(rule.Domain)
		}
	}
	return regions
}
