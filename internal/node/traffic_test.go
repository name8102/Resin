package node

import (
	"sync"
	"testing"
	"time"
)

func TestTrafficStatsEWMAAndExpiry(t *testing.T) {
	var stats TrafficStats
	now := time.Now()
	if _, measured := stats.DownloadSpeed(now, 30*time.Minute); measured {
		t.Fatal("zero value has a sample")
	}
	stats.ObserveDownload(128*1024, time.Second, now)
	stats.ObserveDownload(256*1024, time.Second, now.Add(time.Second))
	if got, ok := stats.DownloadSpeed(now.Add(time.Second), 30*time.Minute); !ok || got != 160*1024 {
		t.Fatalf("EWMA = %v, measured=%v", got, ok)
	}
	if _, measured := stats.DownloadSpeed(now.Add(31*time.Minute), 30*time.Minute); measured {
		t.Fatal("expired sample remains measured")
	}
	stats.ObserveDownload(512*1024, time.Second, now.Add(32*time.Minute))
	if got, ok := stats.DownloadSpeed(now.Add(32*time.Minute), 30*time.Minute); !ok || got != 512*1024 {
		t.Fatalf("sample after expiry = %v, measured=%v", got, ok)
	}
}

func TestTrafficStatsIgnoresTinyAndInstantWindows(t *testing.T) {
	var stats TrafficStats
	now := time.Now()
	for _, sample := range []struct {
		bytes  int64
		active time.Duration
	}{
		{0, time.Second}, {-1, time.Second}, {64*1024 - 1, time.Second},
		{64 * 1024, 0}, {64 * 1024, -time.Second}, {64 * 1024, 99 * time.Millisecond},
	} {
		stats.ObserveDownload(sample.bytes, sample.active, now)
	}
	if _, measured := stats.DownloadSpeed(now, time.Minute); measured {
		t.Fatal("invalid windows produced a sample")
	}
	stats.ObserveDownload(64*1024, 100*time.Millisecond, now)
	if got, ok := stats.DownloadSpeed(now, time.Minute); !ok || got != 655360 {
		t.Fatalf("threshold sample = %v, measured=%v", got, ok)
	}
}

func TestTrafficStatsConcurrent(t *testing.T) {
	var stats TrafficStats
	now := time.Now()
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 100 {
				stats.ObserveDownload(64*1024, time.Second, now)
				if got, ok := stats.DownloadSpeed(now, time.Minute); !ok || got != 64*1024 {
					t.Errorf("concurrent sample = %v, measured=%v", got, ok)
				}
			}
		})
	}
	wg.Wait()
}
