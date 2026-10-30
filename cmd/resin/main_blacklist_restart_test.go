package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
)

func TestBootstrapRestart_RecoversBlacklist(t *testing.T) {
	for _, elapsed := range []bool{false, true} {
		name := "pending"
		if elapsed {
			name = "elapsed-awaiting-probe"
		}
		t.Run(name, func(t *testing.T) {
			stateDir, cacheDir := t.TempDir(), t.TempDir()
			engine1, closer1, err := state.PersistenceBootstrap(stateDir, cacheDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = closer1.Close() })
			const subID = "blacklist-sub"
			if err := engine1.UpsertSubscription(model.Subscription{
				ID: subID, Name: subID, URL: "https://example.com/sub", Enabled: true,
				UpdateIntervalNs: int64(time.Hour), CreatedAtNs: 1, UpdatedAtNs: 1,
			}); err != nil {
				t.Fatal(err)
			}
			raw := json.RawMessage(`{"type":"stub","server":"198.51.100.10","server_port":443}`)
			hash := node.HashFromRawOptions(raw)
			runtimeCfg := config.NewDefaultRuntimeConfig()
			subManager1, pool1 := newBootstrapTestRuntime(runtimeCfg)
			sub := subscription.NewSubscription(subID, subID, "https://example.com/sub", true, false)
			subManager1.Register(sub)
			sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"blocked"}})
			pool1.AddNodeFromSub(hash, raw, subID)
			entry1, _ := pool1.GetEntry(hash)
			until := time.Now().Add(time.Hour).UnixNano()
			if elapsed {
				until = time.Now().Add(-time.Hour).UnixNano()
			}
			entry1.BlacklistUntil.Store(until)
			entry1.BlacklistStrikes.Store(5)
			engine1.MarkNodeStatic(hash.Hex())
			engine1.MarkNodeDynamic(hash.Hex())
			engine1.MarkSubscriptionNode(subID, hash.Hex())
			if err := engine1.FlushDirtySets(newFlushReaders(pool1, subManager1, nil)); err != nil {
				t.Fatal(err)
			}
			if err := closer1.Close(); err != nil {
				t.Fatal(err)
			}
			engine2, closer2, err := state.PersistenceBootstrap(stateDir, cacheDir)
			if err != nil {
				t.Fatal(err)
			}
			defer closer2.Close()
			subManager2, pool2 := newBootstrapTestRuntime(runtimeCfg)
			if err := bootstrapTopology(engine2, subManager2, pool2, newDefaultPlatformEnvConfig()); err != nil {
				t.Fatal(err)
			}
			pool2.AddNodeFromSub(hash, raw, subID)
			if err := restoreBootstrapNodeDynamics(engine2, pool2); err != nil {
				t.Fatal(err)
			}
			entry2, _ := pool2.GetEntry(hash)
			if entry2.BlacklistUntil.Load() != until || entry2.BlacklistStrikes.Load() != 5 {
				t.Fatalf("restored blacklist: until=%d strikes=%d, want %d/5", entry2.BlacklistUntil.Load(), entry2.BlacklistStrikes.Load(), until)
			}
		})
	}
}
