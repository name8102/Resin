package main

import (
	"log"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/requestlog"
	"github.com/Resinat/Resin/internal/routing"
)

func restoreRouteHistory(repo *requestlog.Repo, router *routing.Router) {
	rows, _, _, err := repo.List(requestlog.ListFilter{After: time.Now().Add(-30 * time.Minute).UnixNano(), Limit: 4096})
	if err != nil {
		log.Printf("[routing] request history unavailable: %v", err)
		return
	}
	observations := make([]routing.TargetObservation, 0, len(rows))
	for _, row := range rows {
		h, err := node.ParseHex(row.NodeHash)
		if err != nil || row.PlatformID == "" || row.TargetHost == "" {
			continue
		}
		success := row.NetOK && row.IngressBytes > 0 && row.FirstByteDurationNs > 0
		failure := false
		switch row.UpstreamStage {
		case "connect_dial", "forward_roundtrip", "reverse_roundtrip", "connect_no_ingress_traffic", "connect_invalid_tls_response", "connect_tls_handshake_eof":
			failure = true
		case "connect_upstream_to_client_copy", "connect_client_to_upstream_copy":
			failure = row.IngressBytes == 0 && row.EgressBytes > 0
		}
		if !success && !failure {
			continue
		} // client cancellation/established teardown are neutral
		target := row.TargetURL
		if target == "" {
			target = row.TargetHost
		}
		observed := time.Unix(0, row.TsNs+row.DurationNs)
		if success {
			observed = time.Unix(0, row.TsNs+row.FirstByteDurationNs)
		}
		observations = append(observations, routing.TargetObservation{
			PlatformID: row.PlatformID, NodeHash: h, SelectedAt: time.Unix(0, row.TsNs),
			Target: target, ObservedAt: observed, Success: success,
		})
	}
	router.RestoreTargetHistory(observations)
	log.Printf("[routing] restored %d recent request observations", len(observations))
}
