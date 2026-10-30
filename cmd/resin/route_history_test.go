package main

import (
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/requestlog"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestRestoreRouteHistory_PrimesKnownRoutesWithoutSpeedOrCancellationPenalty(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{MaxLatencyTableEntries: 16})
	p := platform.NewPlatform(platform.DefaultPlatformID, platform.DefaultPlatformName, nil, nil)
	pool.RegisterPlatform(p)
	hashes := []node.Hash{}
	for _, raw := range []string{`{"id":"known"}`, `{"id":"unproven"}`} {
		h := node.HashFromRawOptions([]byte(raw))
		hashes = append(hashes, h)
		pool.AddNodeFromSub(h, []byte(raw), "sub")
		e, _ := pool.GetEntry(h)
		ob := testutil.NewNoopOutbound()
		e.Outbound.Store(&ob)
		e.SetEgressIP(netip.MustParseAddr("203.0.113.1"))
		e.LatencyTable.LoadEntry("cloudflare.com", node.DomainLatencyStats{Ewma: time.Millisecond, LastUpdated: time.Now()})
		pool.RecordResult(h, true)
	}
	pool.RebuildAllPlatforms()
	repo := requestlog.NewRepo(t.TempDir(), 1<<20, 1)
	if err := repo.Open(); err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	now := time.Now()
	_, err := repo.InsertBatch([]proxy.RequestLogEntry{
		{StartedAtNs: now.Add(-time.Minute).UnixNano(), PlatformID: p.ID, NodeHash: hashes[0].Hex(), TargetHost: "working.example:443", NetOK: true, IngressBytes: 2 << 20, DurationNs: int64(time.Second), FirstByteDurationNs: int64(20 * time.Millisecond)},
		{StartedAtNs: now.Add(-30 * time.Second).UnixNano(), PlatformID: p.ID, NodeHash: hashes[0].Hex(), TargetHost: "working.example:443", NetOK: true, DurationNs: int64(time.Second)}, // client cancellation, not a failure
	})
	if err != nil {
		t.Fatal(err)
	}
	r := routing.NewRouter(routing.RouterConfig{Pool: pool, Authorities: func() []string { return nil }, P2CWindow: func() time.Duration { return 10 * time.Minute }})
	restoreRouteHistory(repo, r)
	for i := 0; i < 32; i++ {
		route, err := r.RouteRequest(p.Name, "", "unvisited.example:443")
		if err != nil || route.NodeHash != hashes[0] {
			t.Fatalf("lost historical working node: %v %v", route, err)
		}
	}
	e, _ := pool.GetEntry(hashes[0])
	if _, measured := e.Traffic.DownloadSpeed(now, time.Hour); measured {
		t.Fatal("request lifetime was misused as active download speed")
	}
}
