package proxy

import (
	"net"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// HealthRecorder abstracts passive health feedback reporting.
// topology.GlobalNodePool satisfies this interface.
type HealthRecorder interface {
	RecordResult(hash node.Hash, success bool)
	RecordPassiveLatency(hash node.Hash, rawTarget string, latency *time.Duration)
}

type passiveHealthRecorder interface {
	RecordPassiveResult(platformID string, hash node.Hash, success bool)
}

func recordPassiveResult(health HealthRecorder, route routing.RouteResult, target string, success bool) {
	// Syncthing connectivity is not a signal for web/Git node health.
	_, port, _ := net.SplitHostPort(target)
	if health == nil || port == "22000" || port == "22067" {
		return
	}
	if recorder, ok := health.(passiveHealthRecorder); ok {
		recorder.RecordPassiveResult(route.PlatformID, route.NodeHash, success)
		return
	}
	health.RecordResult(route.NodeHash, success)
}
