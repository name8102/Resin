package platform

import (
	"fmt"
	"net/netip"
	"path"
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

// normalizeDomainPattern keeps glob stars in ASCII labels and normalizes other
// labels using the same IDNA rules as ordinary domains.
func normalizeDomainPattern(raw string) (string, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if !strings.Contains(raw, "*") {
		return normalizeRuleDomain(raw)
	}
	raw = strings.TrimSuffix(raw, ".")
	labels := strings.Split(raw, ".")
	for i, label := range labels {
		if strings.Contains(label, "*") {
			if _, err := normalizeRuleDomain(strings.ReplaceAll(label, "*", "x")); err != nil {
				return "", fmt.Errorf("invalid wildcard DNS label")
			}
			for _, c := range label {
				if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '*') {
					return "", fmt.Errorf("wildcard DNS labels must use ASCII characters")
				}
			}
		} else {
			var err error
			labels[i], err = normalizeRuleDomain(label)
			if err != nil {
				return "", err
			}
		}
	}
	domain := strings.Join(labels, ".")
	if len(domain) > 253 {
		return "", fmt.Errorf("DNS domain exceeds 253 characters")
	}
	return domain, nil
}

func NormalizeDomainRegionRules(rules []model.DomainRegionRule) ([]model.DomainRegionRule, error) {
	normalized := model.CloneDomainRegionRules(rules)
	seen := make(map[string]bool, len(rules))
	for i := range normalized {
		rule := &normalized[i]
		domain, err := normalizeDomainPattern(rule.Domain)
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

// RegionsForDomain returns the matching rule with the most literal characters.
// Bare domains include subdomains; globs match the entire host and * spans labels.
// On equal specificity, fewer stars wins, then the first configured rule.
// A nil result leaves the platform's ordinary routing unchanged.
func (p *Platform) RegionsForDomain(host string) []string {
	host, err := normalizeRuleDomain(host)
	if err != nil {
		return nil
	}
	var regions []string
	specificity, stars := -1, 0
	for _, rule := range p.DomainRegionRules {
		nstars := strings.Count(rule.Domain, "*")
		matches := host == rule.Domain || strings.HasSuffix(host, "."+rule.Domain)
		if nstars > 0 {
			matches, _ = path.Match(rule.Domain, host)
		}
		literal := len(rule.Domain) - nstars
		if matches && (literal > specificity || literal == specificity && nstars < stars) {
			regions, specificity, stars = rule.Regions, literal, nstars
		}
	}
	return regions
}
