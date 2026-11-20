package kolomedia

import (
	"math"
	"slices"
	"time"
)

// The playout delay covers the 95th percentile of arrival lateness over the last 2 s of arrivals,
// counting at most 8192 arrivals.
const (
	estimateWindow   = 2 * time.Second
	estimatePercent  = 95
	estimateCapacity = 8192
)

type sample struct {
	arrival time.Duration
	key     time.Duration
}

// estimator keeps arrival minus media time for the frames of the window. Its minimum is the
// reference: the earliest a frame can arrive for its timestamp.
type estimator struct {
	window []sample
	sorted []time.Duration
}

func (e *estimator) observe(arrival, key time.Duration) {
	for len(e.window) > 0 && (len(e.window) >= estimateCapacity || arrival-e.window[0].arrival > estimateWindow) {
		i, _ := slices.BinarySearch(e.sorted, e.window[0].key)
		e.sorted = slices.Delete(e.sorted, i, i+1)
		e.window = e.window[1:]
	}
	e.window = append(e.window, sample{arrival, key})
	i, _ := slices.BinarySearch(e.sorted, key)
	e.sorted = slices.Insert(e.sorted, i, key)
}

func (e *estimator) reference() time.Duration {
	if len(e.sorted) == 0 {
		return 0
	}
	return e.sorted[0]
}

// jitter returns how much later than the reference the given percentile of frames arrived.
func (e *estimator) jitter() time.Duration {
	n := len(e.sorted)
	if n == 0 {
		return 0
	}
	return satSub(e.sorted[(n*estimatePercent+99)/100-1], e.sorted[0])
}

func satAdd(a, b time.Duration) time.Duration {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	if b < 0 && a < math.MinInt64-b {
		return math.MinInt64
	}
	return a + b
}

func satSub(a, b time.Duration) time.Duration {
	if b == math.MinInt64 {
		if a >= 0 {
			return math.MaxInt64
		}
		return a - b
	}
	return satAdd(a, -b)
}
