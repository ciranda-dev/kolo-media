package kolomedia

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

type simFrame struct {
	track   string
	f       Frame
	index   int
	capture time.Duration
	arrival time.Duration
}

type simRelease struct {
	sf      simFrame
	release time.Duration
}

// simulate pushes each frame at its arrival and polls every track on the fake clock in 1 ms steps,
// skipping polls of a track while stalled reports true. It returns the released frames in order.
func simulate(t *testing.T, p *Playout, clk *fakeClock, frames []simFrame, end time.Duration, stalled func(track string, now time.Duration) bool) []simRelease {
	t.Helper()
	slices.SortStableFunc(frames, func(a, b simFrame) int { return cmp.Compare(a.arrival, b.arrival) })
	byTS := map[string]map[uint64]simFrame{"mic": {}, "cam": {}}
	for _, sf := range frames {
		byTS[sf.track][sf.f.Timestamp] = sf
	}
	start := clk.now()
	var out []simRelease
	next := 0
	for now := time.Duration(0); now <= end; now += time.Millisecond {
		clk.advance(start.Add(now).Sub(clk.now()))
		for next < len(frames) && frames[next].arrival <= now {
			sf := frames[next]
			if err := p.Push(sf.track, sf.f, start.Add(sf.arrival)); err != nil {
				t.Fatal(err)
			}
			next++
		}
		for _, track := range []string{"mic", "cam"} {
			if stalled != nil && stalled(track, now) {
				continue
			}
			for {
				f, ok := pollNow(p, track)
				if !ok {
					break
				}
				out = append(out, simRelease{byTS[track][f.Timestamp], now})
			}
		}
		if now%time.Second == 0 {
			checkBounds(t, p)
		}
	}
	return out
}

func checkBounds(t *testing.T, p *Playout) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.est.window) > estimateCapacity || len(p.est.window) != len(p.est.sorted) {
		t.Fatalf("estimator holds %d/%d samples", len(p.est.window), len(p.est.sorted))
	}
	if p.delay < p.cfg.MinDelay || p.delay > p.cfg.MaxDelay {
		t.Fatalf("delay %v out of bounds", p.delay)
	}
	for name, ts := range p.tracks {
		if len(ts.frames) > p.cfg.MaxFrames || ts.bytes > p.cfg.MaxBytes {
			t.Fatalf("%s buffers %d frames, %d bytes", name, len(ts.frames), ts.bytes)
		}
	}
}

// broadcast captures 20 ms audio frames and 30 fps video with a keyframe every 2 s on one sender clock.
func broadcast(t *testing.T, c *Clock, length time.Duration, r *rand.Rand, audioNet, videoNet func(capture time.Duration) time.Duration) []simFrame {
	t.Helper()
	var frames []simFrame
	for i := 0; ms(20*i) < length; i++ {
		capture := ms(20 * i)
		ts, err := c.After(capture, 48000)
		if err != nil {
			t.Fatal(err)
		}
		data := make([]byte, 40+r.IntN(80))
		frames = append(frames, simFrame{"mic", Frame{Timestamp: ts, Data: data}, i, capture, capture + audioNet(capture)})
	}
	var last time.Duration
	for i := 0; time.Duration(i)*time.Second/30 < length; i++ {
		capture := time.Duration(i) * time.Second / 30
		ts, err := c.After(capture, 90000)
		if err != nil {
			t.Fatal(err)
		}
		key := i%60 == 0
		data := make([]byte, 500+r.IntN(2000))
		if key {
			data = make([]byte, 20000)
		}
		// One group is one ordered stream: a frame never arrives before the one in front of it.
		arrival := capture + videoNet(capture)
		if !key {
			arrival = max(arrival, last)
		}
		last = arrival
		frames = append(frames, simFrame{"cam", Frame{Timestamp: ts, Keyframe: key, Data: data}, i, capture, arrival})
	}
	return frames
}

// TestAVSync checks that audio and video captured at the same instant play within 5 ms of each
// other, with 5 ms of audio and 30 ms of video send latency and up to 40 ms of network jitter.
func TestAVSync(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{})
	r := rand.New(rand.NewPCG(7, 9))
	jitter := func() time.Duration { return time.Duration(r.Int64N(int64(40 * time.Millisecond))) }
	frames := broadcast(t, &Clock{start: clk.now()}, 10*time.Second, r,
		func(time.Duration) time.Duration { return 5*time.Millisecond + jitter() },
		func(time.Duration) time.Duration { return 30*time.Millisecond + jitter() })
	out := simulate(t, p, clk, frames, 11*time.Second, nil)
	if len(out) != len(frames) {
		t.Fatalf("played %d of %d frames: %+v", len(out), len(frames), p.Stats())
	}
	lo, hi := time.Duration(1<<62), time.Duration(0)
	for _, rel := range out {
		if rel.sf.capture < 2*time.Second {
			continue
		}
		off := rel.release - rel.sf.capture
		lo, hi = min(lo, off), max(hi, off)
	}
	t.Logf("capture to play offsets span %v to %v", lo, hi)
	if hi-lo > 5*time.Millisecond {
		t.Errorf("capture to play offsets span %v to %v", lo, hi)
	}
	if hi > 110*time.Millisecond {
		t.Errorf("latency %v, want about 5 ms of transit plus the 100 ms delay", hi)
	}
}

// TestLongSession runs ten simulated minutes with jitter, lost group tails, bursts, duplicates
// and consumer stalls, then checks the invariants and that sync recovers in the calm last 20 s.
func TestLongSession(t *testing.T) {
	if testing.Short() {
		t.Skip("long simulation")
	}
	length := 10 * time.Minute
	calm := length - 20*time.Second
	var keyframeRequests int
	cfg := PlayoutConfig{MaxFrames: 32, OnKeyframeNeeded: func(string) { keyframeRequests++ }}
	p, clk := newTestPlayout(t, cfg)
	r := rand.New(rand.NewPCG(1, 1))
	net := func(base time.Duration) func(time.Duration) time.Duration {
		return func(capture time.Duration) time.Duration {
			d := base + time.Duration(r.Int64N(int64(30*time.Millisecond)))
			if capture < calm {
				if r.IntN(50) == 0 {
					d += time.Duration(r.Int64N(int64(300 * time.Millisecond)))
				}
				// A 2 s outage every 97 s: everything sent in it arrives at its end.
				if phase := capture % (97 * time.Second); phase > 50*time.Second && phase < 52*time.Second {
					d += 52*time.Second - phase
				}
			}
			return d
		}
	}
	frames := broadcast(t, &Clock{start: clk.now()}, length, r, net(5*time.Millisecond), net(25*time.Millisecond))
	// Lose the tail of one video group in 20 and one audio frame in 50; duplicate one frame in 100.
	var kept []simFrame
	cut := false
	for _, sf := range frames {
		if sf.capture < calm {
			if sf.track == "cam" {
				if sf.f.Keyframe {
					cut = false
				}
				if !cut && !sf.f.Keyframe && r.IntN(1200) == 0 {
					cut = true
				}
				if cut {
					continue
				}
			} else if r.IntN(50) == 0 {
				continue
			}
			if r.IntN(100) == 0 {
				dup := sf
				dup.arrival += time.Duration(r.Int64N(int64(time.Second)))
				kept = append(kept, dup)
			}
		}
		kept = append(kept, sf)
	}
	stalled := func(track string, now time.Duration) bool {
		return now < calm && now%(61*time.Second) > 30*time.Second && now%(61*time.Second) < 30*time.Second+700*time.Millisecond && track == "cam"
	}
	out := simulate(t, p, clk, kept, length+time.Second, stalled)

	last := map[string]simRelease{}
	for _, rel := range out {
		prev, seen := last[rel.sf.track]
		if seen && rel.sf.f.Timestamp <= prev.sf.f.Timestamp {
			t.Fatalf("%s went back from %d to %d", rel.sf.track, prev.sf.f.Timestamp, rel.sf.f.Timestamp)
		}
		if rel.sf.track == "cam" && !rel.sf.f.Keyframe && (!seen || prev.sf.index != rel.sf.index-1) {
			t.Fatalf("video frame %d played after frame %d without a keyframe", rel.sf.index, prev.sf.index)
		}
		last[rel.sf.track] = rel
	}
	lo, hi := time.Duration(1<<62), time.Duration(0)
	for _, rel := range out {
		if rel.sf.capture > calm+5*time.Second {
			off := rel.release - rel.sf.capture
			lo, hi = min(lo, off), max(hi, off)
		}
	}
	if hi-lo > 5*time.Millisecond {
		t.Errorf("after the calm returns, offsets span %v to %v", lo, hi)
	}
	s := p.Stats()
	a, v := s.Tracks["mic"], s.Tracks["cam"]
	t.Logf("delay %v, audio %+v, video %+v, keyframe requests %d", s.Delay, a, v, keyframeRequests)
	if a.Skipped == 0 || v.Skipped == 0 || v.Overflow == 0 || v.NoKeyframe == 0 || keyframeRequests == 0 || a.Duplicate == 0 || a.Late == 0 {
		t.Errorf("the session did not exercise skips, overflow, duplicates and late frames")
	}
	for _, ts := range []TrackStats{a, v} {
		if ts.Played+ts.Late+ts.Duplicate+ts.Overflow+ts.Skipped+ts.NoKeyframe+uint64(ts.Buffered) != ts.Received {
			t.Errorf("frames lost track of: %+v", ts)
		}
	}
}
