package state

import (
	"reflect"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
)

func TestMigrateCacheDB_AddsBlacklistToExistingNodes(t *testing.T) {
	db, err := OpenDB(t.TempDir() + "/cache.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range []string{"000001_cache_base.up.sql", "000002_subscription_nodes_evicted.up.sql"} {
		sql, err := migrationsFS.ReadFile(cacheMigrationsPath + "/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`CREATE TABLE schema_migrations (version uint64 NOT NULL PRIMARY KEY, dirty bool NOT NULL);
		INSERT INTO schema_migrations (version, dirty) VALUES (2, 0);
		INSERT INTO nodes_dynamic (hash, failure_count) VALUES ('existing', 4), ('healthy', 0)`); err != nil {
		t.Fatal(err)
	}
	if err := MigrateCacheDB(db); err != nil {
		t.Fatal(err)
	}
	if err := MigrateCacheDB(db); err != nil {
		t.Fatalf("second migration: %v", err)
	}
	rows, err := newCacheRepo(db).LoadAllNodesDynamic()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("upgraded rows: %+v", rows)
	}
	for _, r := range rows {
		if r.Hash == "healthy" {
			if r.BlacklistUntilNs != 0 || r.BlacklistStrikes != 0 {
				t.Fatalf("healthy node quarantined: %+v", r)
			}
		} else if r.Hash == "existing" {
			remaining := time.Until(time.Unix(0, r.BlacklistUntilNs))
			if r.FailureCount != 4 || r.BlacklistStrikes != 1 || r.CircuitOpenSince == 0 || remaining < 59*time.Minute || remaining > time.Hour {
				t.Fatalf("known failed node not quarantined on upgrade: %+v", r)
			}
		} else {
			t.Fatalf("unexpected row %+v", r)
		}
	}
}

func TestCacheRepo_BlacklistRoundTrip(t *testing.T) {
	for _, path := range []string{"bulk", "flush"} {
		t.Run(path, func(t *testing.T) {
			repo := newTestCacheRepo(t)
			write := repo.BulkUpsertNodesDynamic
			if path == "flush" {
				write = func(nodes []model.NodeDynamic) error {
					return repo.FlushTx(FlushOps{UpsertNodesDynamic: nodes})
				}
			}
			for _, want := range []model.NodeDynamic{
				{Hash: "node", FailureCount: 2, BlacklistUntilNs: 123456789, BlacklistStrikes: 3},
				{Hash: "node", FailureCount: 4, BlacklistUntilNs: 234567890, BlacklistStrikes: 4},
				{Hash: "node", BlacklistStrikes: 4},
			} {
				if err := write([]model.NodeDynamic{want}); err != nil {
					t.Fatal(err)
				}
				got, err := repo.LoadAllNodesDynamic()
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
					t.Fatalf("round trip: got %+v, want %+v", got, want)
				}
			}
		})
	}
}
