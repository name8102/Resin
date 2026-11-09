package routing

import (
	"math"
	"time"
)

const (
	defaultResponseLatency = 750 * time.Millisecond
	defaultDownloadSpeed   = 1024 * 1024
	explorationRequestCost = 32
	maxExplorationCredit   = 2 * explorationRequestCost
)

// Reliability and performance expire independently. Recent real responses earn
// confidence; missing latency/rate measurements remain unknown, not zero-cost.
func evidenceFreshness(observed, now time.Time, window time.Duration) float64 {
	if observed.IsZero() || window <= 0 || now.Sub(observed) > window {
		return 0
	}
	return math.Exp2(-2 * max(now.Sub(observed).Seconds(), 0) / window.Seconds())
}

// Response evidence retains the recent balance of real responses and initial
// upstream failures independently of cooldown backoff. A successful recovery
// resets backoff, not this history. The one-response prior fades old failures
// toward neutral; without failures, any number of successes has reliability 1.
type targetResponseEvidence struct {
	successes float64
	failures  float64
	updated   time.Time
}

func (e *targetResponseEvidence) observe(success bool, now time.Time, window time.Duration) {
	weight := evidenceFreshness(e.updated, now, window)
	e.successes *= weight
	e.failures *= weight
	if success {
		e.successes++
	} else {
		e.failures++
	}
	e.updated = now
}

func (e targetResponseEvidence) reliability(now time.Time, window time.Duration) float64 {
	weight := evidenceFreshness(e.updated, now, window)
	responses := 1 + weight*e.successes
	return responses / (responses + weight*e.failures)
}

type targetQualitySample struct {
	value   float64
	updated time.Time
}

func (s targetQualitySample) freshness(now time.Time, window time.Duration) float64 {
	return evidenceFreshness(s.updated, now, window)
}

func (s targetQualitySample) estimate(now time.Time, window time.Duration, prior float64) float64 {
	weight := s.freshness(now, window)
	if weight == 0 {
		return 0
	}
	return prior + weight*(s.value-prior)
}

func (s *targetQualitySample) observe(value float64, now time.Time, window time.Duration) {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	if weight := s.freshness(now, window); weight > 0 {
		s.value += (1 - 0.75*weight) * (value - s.value)
	} else {
		s.value = value
	}
	s.updated = now
}

// ReportTargetQuality consumes observations already made by a real request.
// A zero metric is absent; active-download rate must exclude idle/client time.
// Quality does not establish reachability and never clears a target failure.
func (r *Router) ReportTargetQuality(route RouteResult, target string, responseLatency time.Duration, downloadBytesPerSecond float64) {
	if _, ok := r.pool.GetPlatform(route.PlatformID); !ok {
		return
	}
	now := time.Now()
	window := r.p2cWindow()
	key := targetHealthKey{route.PlatformID, route.NodeHash, normalizeRouteTarget(target)}
	r.targetHealth.mu.Lock()
	defer r.targetHealth.mu.Unlock()
	health := r.targetHealth.get(key, now, window)
	if route.selectedAt.IsZero() || !route.selectedAt.After(health.lastFailure) || now.Before(health.until) {
		return
	}
	health.latency.observe(float64(responseLatency), now, window)
	health.speed.observe(downloadBytesPerSecond, now, passiveSpeedWindow)
	if !health.latency.updated.IsZero() || !health.speed.updated.IsZero() {
		r.targetHealth.put(health)
	}
}

func (c *targetHealthCache) earnExploration(key targetHealthKey, now time.Time, window time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	health := c.get(key, now, window)
	if !isRecent(health.explorationAt, now, knownNodeWindow) {
		health.explorationCredit = 0
	}
	health.explorationAt = now
	health.explorationCredit = min(health.explorationCredit+1, maxExplorationCredit)
	c.put(health)
}

func (c *targetHealthCache) spendExploration(key targetHealthKey, now time.Time, window time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	health := c.get(key, now, window)
	if health.explorationCredit < explorationRequestCost {
		return false
	}
	health.explorationCredit -= explorationRequestCost
	c.put(health)
	return true
}
