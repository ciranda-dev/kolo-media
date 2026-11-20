package kolomedia

import (
	"testing"
	"time"

	kolomoq "github.com/ciranda-dev/kolo-moq"
)

// gateSim runs a keyframeGate on a fake time axis, the way requestKeyframes does: one check per keyframeCheck.
type gateSim struct {
	g        keyframeGate
	now      time.Time
	received uint64
	waiting  bool
	asks     []time.Duration
	t0       time.Time
}

func newGateSim() *gateSim {
	t0 := time.Unix(1_800_000_000, 0)
	return &gateSim{g: keyframeGate{moved: t0}, now: t0, t0: t0}
}

// run advances d, calling answer after each request: it returns the frames the publisher sends back, if any.
func (s *gateSim) run(d time.Duration, frames func(since time.Duration) int, answer func() (frames int, waiting bool)) {
	for end := s.now.Add(d); s.now.Before(end); {
		s.now = s.now.Add(keyframeCheck)
		if frames != nil {
			s.received += uint64(frames(s.now.Sub(s.t0)))
		}
		s.g.observe(s.now, s.received, s.waiting)
		if s.g.ready(s.now) {
			s.g.asked(s.now)
			s.asks = append(s.asks, s.now.Sub(s.t0))
			if answer != nil {
				n, waiting := answer()
				s.received += uint64(n)
				s.waiting = waiting
			}
		}
	}
}

// A still source that answers every request with one keyframe is asked less and less often, keyframeMaxQuiet times, and then left alone.
func TestKeyframeGateStillSourceBacksOff(t *testing.T) {
	s := newGateSim()
	s.run(time.Hour, nil, func() (int, bool) { return 1, false })
	if n := len(s.asks); n != keyframeMaxQuiet {
		t.Fatalf("%d requests in an hour of silence, want %d: %v", n, keyframeMaxQuiet, s.asks)
	}
	for i := 2; i < len(s.asks); i++ {
		if gap, prev := s.asks[i]-s.asks[i-1], s.asks[i-1]-s.asks[i-2]; gap < 2*prev-keyframeCheck {
			t.Fatalf("gaps do not double: %v", s.asks)
		}
	}
}

// A camera that resumes after a pause is asked about again if it goes quiet later: frames in a row re-arm the silence trigger.
func TestKeyframeGateFlowRearmsSilence(t *testing.T) {
	s := newGateSim()
	s.run(time.Hour, nil, func() (int, bool) { return 1, false })
	before := len(s.asks)
	s.run(2*time.Second, func(time.Duration) int { return 10 }, nil)
	s.run(time.Second, nil, func() (int, bool) { return 1, false })
	if len(s.asks) != before+1 {
		t.Fatalf("%d requests after the camera ran and went quiet, want %d", len(s.asks), before+1)
	}
	if s.g.quiet != 1 {
		t.Fatalf("quiet %d, want 1", s.g.quiet)
	}
}

// A frame every 3 s, as a still screen share sends: the silence a request needs outgrows the gap after a few requests.
func TestKeyframeGateSlowFramesStop(t *testing.T) {
	s := newGateSim()
	frames := func(at time.Duration) int {
		if at%(3*time.Second) == 0 {
			return 1
		}
		return 0
	}
	s.run(30*time.Second, frames, func() (int, bool) { return 1, false })
	if n := len(s.asks); n < 1 || n > keyframeMaxQuiet {
		t.Fatalf("%d requests in 30 s: %v", n, s.asks)
	}
}

// A publisher that never answers is asked five times, 1 s, 2 s, 4 s and 8 s apart, and then left alone, however long it stays silent.
func TestKeyframeGateDeadTrackStops(t *testing.T) {
	s := newGateSim()
	s.run(time.Hour, nil, nil)
	if len(s.asks) != keyframeMaxUnanswered {
		t.Fatalf("%d requests, want %d: %v", len(s.asks), keyframeMaxUnanswered, s.asks)
	}
	for i := 1; i < len(s.asks); i++ {
		if gap, min := s.asks[i]-s.asks[i-1], kolomoq.NewGroupRequestInterval<<i; gap < min {
			t.Fatalf("request %d came %v after the one before, want at least %v: %v", i, gap, min, s.asks)
		}
	}
}

// A track that waits for a keyframe while frames keep arriving, with a publisher that ignores requests, is asked five times too.
func TestKeyframeGateWaitingStops(t *testing.T) {
	s := newGateSim()
	s.waiting = true
	s.run(time.Hour, func(time.Duration) int { return 1 }, nil)
	if len(s.asks) != keyframeMaxUnanswered {
		t.Fatalf("%d requests, want %d: %v", len(s.asks), keyframeMaxUnanswered, s.asks)
	}
}

// A playable frame ends the count, so a later loss is asked about at once, and a loss that the publisher answers each time never stops.
func TestKeyframeGateRecoveryRearms(t *testing.T) {
	s := newGateSim()
	s.run(10*time.Second, nil, nil)
	if len(s.asks) != 3 {
		t.Fatalf("%d requests in 10 s of silence, want 3: %v", len(s.asks), s.asks)
	}
	s.received++
	s.g.observe(s.now.Add(keyframeCheck), s.received, false)
	if s.g.unanswered != 0 {
		t.Fatalf("a playable frame left %d requests unanswered", s.g.unanswered)
	}
	before := len(s.asks)
	s.g.signal()
	s.run(2*time.Second, func(time.Duration) int { return 1 }, func() (int, bool) { return 1, false })
	if len(s.asks) != before+1 {
		t.Fatalf("no request for a new loss: %v", s.asks)
	}
	for range 50 {
		s.waiting = true
		s.run(1250*time.Millisecond, func(time.Duration) int { return 1 }, func() (int, bool) { return 1, false })
	}
	if got := len(s.asks) - before; got < 40 {
		t.Fatalf("only %d requests for 50 answered losses", got)
	}
}

// A request for silence is not made while the first silence is shorter than keyframeStall, and one real loss restarts the count.
func TestKeyframeGateLossResetsBackoff(t *testing.T) {
	s := newGateSim()
	s.run(keyframeStall-keyframeCheck, nil, nil)
	if len(s.asks) != 0 {
		t.Fatalf("asked after %v of silence: %v", keyframeStall-keyframeCheck, s.asks)
	}
	s.g.quiet = keyframeMaxQuiet
	s.g.signal()
	if s.g.quiet != 0 || !s.g.need || s.g.silent {
		t.Fatalf("signal left %+v", s.g)
	}
}

// Without a video track, or with a new subscription, the count starts over.
func TestKeyframeGateReset(t *testing.T) {
	g := keyframeGate{seen: 7, unanswered: 4, quiet: 3, need: true, silent: true, last: time.Unix(1, 0)}
	now := time.Unix(100, 0)
	g.reset(now)
	if (g != keyframeGate{seen: 7, moved: now}) {
		t.Fatalf("reset left %+v", g)
	}
}

// FuzzKeyframeGate drives the gate with arbitrary time steps, frames and losses: it never asks twice within a second, never
// more than keyframeMaxUnanswered times without a playable frame, and its counters stay within their bounds.
func FuzzKeyframeGate(f *testing.F) {
	f.Add([]byte{1, 2, 3, 0xff, 0x80, 0, 0, 0})
	f.Fuzz(func(t *testing.T, steps []byte) {
		now := time.Unix(1_800_000_000, 0)
		g := keyframeGate{moved: now}
		var received uint64
		var last time.Time
		for _, b := range steps {
			now = now.Add(time.Duration(b&0x0f) * 100 * time.Millisecond)
			received += uint64(b >> 4 & 3)
			waiting := b>>6&1 == 1
			if b>>7 == 1 {
				g.signal()
			}
			g.observe(now, received, waiting)
			if !g.ready(now) {
				continue
			}
			if !last.IsZero() && now.Sub(last) < kolomoq.NewGroupRequestInterval {
				t.Fatalf("asked %v after the last request", now.Sub(last))
			}
			g.asked(now)
			last = now
			if g.unanswered > keyframeMaxUnanswered || g.quiet > keyframeMaxQuiet {
				t.Fatalf("counters out of bounds: %+v", g)
			}
		}
	})
}
