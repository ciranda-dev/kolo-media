package kolomedia

import (
	"fmt"
	"math"
	"math/bits"
	"time"
)

// DefaultTimescale is the LOC default: timestamps in microseconds since the Unix epoch.
const DefaultTimescale = 1_000_000

const nanosPerSecond = uint64(time.Second)

// Rescale converts a timestamp from one timescale to another, rounding to the nearest unit.
func Rescale(ts, from, to uint64) (uint64, error) {
	if from == 0 || to == 0 {
		return 0, ErrInvalidTimescale
	}
	hi, lo := bits.Mul64(ts, to)
	lo, carry := bits.Add64(lo, from/2, 0)
	hi += carry
	if hi >= from {
		return 0, fmt.Errorf("%w: %d at %d/s in %d/s", ErrTimestampRange, ts, from, to)
	}
	q, _ := bits.Div64(hi, lo, from)
	return q, nil
}

// ToDuration converts a timestamp in the given timescale to a duration.
func ToDuration(ts, timescale uint64) (time.Duration, error) {
	ns, err := Rescale(ts, timescale, nanosPerSecond)
	if err != nil {
		return 0, err
	}
	if ns > math.MaxInt64 {
		return 0, fmt.Errorf("%w: %d at %d/s", ErrTimestampRange, ts, timescale)
	}
	return time.Duration(ns), nil
}

// FromDuration converts a non-negative duration to a timestamp in the given timescale.
func FromDuration(d time.Duration, timescale uint64) (uint64, error) {
	if d < 0 {
		return 0, fmt.Errorf("%w: negative duration %v", ErrTimestampRange, d)
	}
	return Rescale(uint64(d), nanosPerSecond, timescale)
}

// Clock gives the tracks of one sender one time base: Unix time read once by NewClock, then advanced
// by the monotonic clock. Use NewClock; a Clock is safe for concurrent use.
type Clock struct {
	start time.Time
}

// NewClock returns a clock that starts now.
func NewClock() *Clock {
	return &Clock{start: time.Now()}
}

// Start returns the instant the clock was made.
func (c *Clock) Start() time.Time { return c.start }

// At returns the timestamp of capture instant t in the given timescale.
func (c *Clock) At(t time.Time, timescale uint64) (uint64, error) {
	return c.After(t.Sub(c.start), timescale)
}

// After returns the timestamp of the instant d after the clock's start in the given timescale.
func (c *Clock) After(d time.Duration, timescale uint64) (uint64, error) {
	if timescale == 0 {
		return 0, ErrInvalidTimescale
	}
	if c.start.IsZero() {
		return 0, fmt.Errorf("%w: zero Clock", ErrTimestampRange)
	}
	unix := time.Duration(c.start.UnixNano())
	if d > 0 && unix > math.MaxInt64-d || unix+d < 0 {
		return 0, fmt.Errorf("%w: %v after the clock start", ErrTimestampRange, d)
	}
	return FromDuration(unix+d, timescale)
}
