package api

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/service"
)

func TestPlatformDomainRegionRulesAPI(t *testing.T) {
	srv, cp, _ := newControlPlaneTestServer(t)
	rec := doJSONRequest(t, srv, http.MethodPost, "/api/v1/platforms", map[string]any{
		"name":                "Domain-Regions",
		"domain_region_rules": []model.DomainRegionRule{{Domain: "CHATGPT.COM.", Regions: []string{"us", "jp"}}},
	}, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var created service.PlatformResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	want := []model.DomainRegionRule{{Domain: "chatgpt.com", Regions: []string{"us", "jp"}}}
	if !reflect.DeepEqual(created.DomainRegionRules, want) {
		t.Fatalf("response rules: %+v", created.DomainRegionRules)
	}
	runtime, ok := cp.Pool.GetPlatform(created.ID)
	if !ok || !reflect.DeepEqual(runtime.DomainRegionRules, want) {
		t.Fatal("rules not published to runtime")
	}
	persisted, err := cp.Engine.GetPlatform(created.ID)
	if err != nil || !reflect.DeepEqual(persisted.DomainRegionRules, want) {
		t.Fatalf("persisted=%+v err=%v", persisted, err)
	}
	for _, patch := range []string{
		`{"domain_region_rules":null}`,
		`{"domain_region_rules":{}}`,
		`{"domain_region_rules":[{"domain":"https://chatgpt.com","regions":["us"]}]}`,
		`{"domain_region_rules":[{"domain":"chatgpt.com","regions":[]}]}`,
		`{"domain_region_rules":[{"domain":"chatgpt.com","regions":["us"],"fallback":"any"}]}`,
	} {
		rec = doJSONRequest(t, srv, http.MethodPatch, "/api/v1/platforms/"+created.ID, patch, true)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("invalid patch accepted: %d %s", rec.Code, rec.Body.String())
		}
		runtime, _ = cp.Pool.GetPlatform(created.ID)
		if !reflect.DeepEqual(runtime.DomainRegionRules, want) {
			t.Fatal("invalid patch changed runtime")
		}
	}
	rec = doJSONRequest(t, srv, http.MethodPatch, "/api/v1/platforms/"+created.ID,
		`{"domain_region_rules":[{"domain":"example.com","regions":["sg"]}]}`, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body.String())
	}
	runtime, _ = cp.Pool.GetPlatform(created.ID)
	if got := runtime.RegionsForDomain("www.example.com"); !reflect.DeepEqual(got, []string{"sg"}) {
		t.Fatalf("updated runtime: %v", got)
	}
	rec = doJSONRequest(t, srv, http.MethodPatch, "/api/v1/platforms/"+created.ID, `{"domain_region_rules":[]}`, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("clear: %d %s", rec.Code, rec.Body.String())
	}
	runtime, _ = cp.Pool.GetPlatform(created.ID)
	if len(runtime.DomainRegionRules) != 0 {
		t.Fatal("rules not cleared")
	}
}
