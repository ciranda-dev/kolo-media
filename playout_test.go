package kolomedia

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestPlayout(t *testing.T, cfg PlayoutConfig, tracks ...Track) (*Playout, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	if len(tracks) == 0 {
		tracks = []Track{audioTrack(), videoTrack()}
	}
	p, err := newPlayout(tracks, cfg, clk)
	if err != nil {
		t.Fatal(err)
	}
	return p, clk
}

func pollNow(p *Playout, track string) (Frame, bool) {
	p.mu.Lock()
	r := p.poll(p.tracks[track], p.clk.now())
	p.mu.Unlock()
	if r.keyframeNeeded && p.cfg.OnKeyframeNeeded != nil {
		p.cfg.OnKeyframeNeeded(track)
	}
	return r.f, r.ok
}

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func audioAt(d time.Duration) Frame {
	ts, _ := FromDuration(d, 48000)
	return Frame{Timestamp: ts, Keyframe: true, Data: []byte{1, 2}}
}

func videoAt(d time.Duration, key bool) Frame {
	ts, _ := FromDuration(d, 90000)
	return Frame{Timestamp: ts, Keyframe: key, Data: []byte{3, 4, 5}}
}

func push(t *testing.T, p *Playout, track string, f Frame, arrival time.Time) {
	t.Helper()
	if err := p.Push(track, f, arrival); err != nil {
		t.Fatal(err)
	}
}

func TestPlayoutDueTime(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{})
	push(t, p, "mic", audioAt(0), clk.now())
	if _, ok := pollNow(p, "mic"); ok {
		t.Fatal("frame released before its delay")
	}
	clk.advance(ms(99))
	if _, ok := pollNow(p, "mic"); ok {
		t.Fatal("frame released 1 ms early")
	}
	clk.advance(ms(1))
	if f, ok := pollNow(p, "mic"); !ok || f.Timestamp != 0 {
		t.Fatalf("frame not released at reference + 100 ms: %v", ok)
	}
	// The next frame, captured 20 ms later and arriving on time, is due 20 ms later.
	push(t, p, "mic", audioAt(ms(20)), clk.now().Add(ms(-80)))
	clk.advance(ms(19))
	if _, ok := pollNow(p, "mic"); ok {
		t.Fatal("second frame early")
	}
	clk.advance(ms(1))
	if _, ok := pollNow(p, "mic"); !ok {
		t.Fatal("second frame not due")
	}
	if s := p.Stats(); s.Delay != ms(100) || s.Tracks["mic"].Played != 2 {
		t.Errorf("stats %+v", s)
	}
}

func TestPlayoutZeroArrivalAndUnixTimestamps(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{})
	c := &Clock{start: clk.now()}
	ts, err := c.At(clk.now(), 48000)
	if err != nil {
		t.Fatal(err)
	}
	push(t, p, "mic", Frame{Timestamp: ts, Data: []byte{1}}, time.Time{})
	clk.advance(ms(100))
	if f, ok := pollNow(p, "mic"); !ok || f.Timestamp != ts {
		t.Fatal("Unix-based timestamp with arrival now not played after the delay")
	}
}

func TestJitterBufferReorderAndDuplicates(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
	now := clk.now()
	for _, i := range []int{3, 1, 2, 1, 0} {
		push(t, p, "mic", audioAt(ms(20*i)), now)
	}
	clk.advance(time.Second)
	// MaxBehind would skip these, so play them from a fresh reference instead.
	p.mu.Lock()
	p.cfg.MaxBehind = time.Hour
	p.mu.Unlock()
	var got []uint64
	for {
		f, ok := pollNow(p, "mic")
		if !ok {
			break
		}
		got = append(got, f.Timestamp)
	}
	if len(got) != 4 || got[0] != 0 || got[1] != 960 || got[2] != 1920 || got[3] != 2880 {
		t.Fatalf("order %v", got)
	}
	push(t, p, "mic", audioAt(ms(60)), clk.now()) // just played
	push(t, p, "mic", audioAt(ms(40)), clk.now()) // older than the playout point
	s := p.Stats().Tracks["mic"]
	if s.Duplicate != 2 || s.Late != 1 || s.Received != 7 || s.Played != 4 || s.Buffered != 0 {
		t.Errorf("stats %+v", s)
	}
}

func TestJitterBufferBounds(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{MaxFrames: 3, MaxBytes: 7}, audioTrack())
	for i := range 5 {
		push(t, p, "mic", audioAt(ms(20*i)), clk.now())
	}
	s := p.Stats().Tracks["mic"]
	if s.Overflow != 2 || s.Buffered != 3 || s.BufferedBytes != 6 {
		t.Fatalf("frame bound: %+v", s)
	}
	clk.advance(ms(60))
	if f, ok := pollNow(p, "mic"); !ok || f.Timestamp != 1920 {
		t.Fatalf("oldest frames not the ones evicted: %d %v", f.Timestamp, ok)
	}
	push(t, p, "mic", Frame{Timestamp: audioAt(ms(100)).Timestamp, Data: make([]byte, 6)}, clk.now())
	if s := p.Stats().Tracks["mic"]; s.Overflow != 4 || s.Buffered != 1 || s.BufferedBytes != 6 {
		t.Fatalf("byte bound: %+v", s)
	}
	if err := p.Push("mic", Frame{Timestamp: 2_000_000, Data: make([]byte, 8)}, clk.now()); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("frame over MaxBytes: %v", err)
	}
}

func TestVideoWaitsForKeyframe(t *testing.T) {
	var calls atomic.Int32
	p, clk := newTestPlayout(t, PlayoutConfig{OnKeyframeNeeded: func(track string) {
		if track == "cam" {
			calls.Add(1)
		}
	}}, videoTrack())
	if !p.Stats().Tracks["cam"].WaitingKeyframe {
		t.Fatal("a new video track does not wait for a keyframe")
	}
	// Joined mid-group: two delta frames, then a keyframe.
	now := clk.now()
	for i := range 6 {
		push(t, p, "cam", videoAt(ms(33*i), i == 2), now.Add(ms(33*i)))
	}
	var played []int
	for range 400 {
		clk.advance(ms(1))
		for {
			f, ok := pollNow(p, "cam")
			if !ok {
				break
			}
			played = append(played, int(f.Timestamp/2970))
		}
	}
	if len(played) != 4 || played[0] != 2 {
		t.Fatalf("played %v, want frames 2 to 5", played)
	}
	s := p.Stats().Tracks["cam"]
	if s.NoKeyframe != 2 || s.WaitingKeyframe || calls.Load() != 1 {
		t.Fatalf("stats %+v, %d callbacks", s, calls.Load())
	}

	// Losing frames to the buffer bound is a gap: wait for the next keyframe again.
	p.mu.Lock()
	p.cfg.MaxFrames = 2
	p.mu.Unlock()
	base := clk.now()
	for i := 6; i < 12; i++ {
		push(t, p, "cam", videoAt(ms(33*i), i == 10), base)
	}
	s = p.Stats().Tracks["cam"]
	if s.Overflow != 4 || !s.WaitingKeyframe {
		t.Fatalf("after overflow: %+v", s)
	}
	clk.advance(time.Second)
	f, ok := pollNow(p, "cam")
	if !ok || !f.Keyframe || f.Timestamp/2970 != 10 {
		t.Fatalf("after overflow played %+v, %v; want keyframe 10", f, ok)
	}
	if calls.Load() != 1 {
		t.Errorf("callback without a dropped delta frame: %d", calls.Load())
	}
	// A late keyframe-less gap: deltas only, so the callback fires once more.
	p.mu.Lock()
	p.tracks["cam"].needKey = true
	p.mu.Unlock()
	push(t, p, "cam", videoAt(ms(33*13), false), clk.now())
	push(t, p, "cam", videoAt(ms(33*14), false), clk.now())
	clk.advance(time.Second)
	if _, ok := pollNow(p, "cam"); ok || calls.Load() != 2 {
		t.Errorf("deltas while waiting: played %v, %d callbacks", ok, calls.Load())
	}
}

func TestSkipToLiveAudio(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
	start := clk.now()
	for i := range 20 {
		push(t, p, "mic", audioAt(ms(20*i)), start.Add(ms(20*i)))
	}
	// The consumer stalls: frame i is due at 100 + 20i ms. At 400 ms, frames due before 350 ms are too late.
	clk.advance(ms(400))
	f, ok := pollNow(p, "mic")
	if !ok || f.Timestamp != audioAt(ms(260)).Timestamp {
		t.Fatalf("resumed at %d (%v), want the frame captured at 260 ms", f.Timestamp, ok)
	}
	if s := p.Stats().Tracks["mic"]; s.Skipped != 13 {
		t.Errorf("skipped %d, want 13", s.Skipped)
	}
}

func TestSkipToLiveVideo(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, videoTrack())
	start := clk.now()
	for i := range 9 {
		push(t, p, "cam", videoAt(ms(30*i), i%3 == 0), start.Add(ms(30*i)))
	}
	clk.advance(ms(500))
	f, ok := pollNow(p, "cam")
	if !ok || !f.Keyframe || f.Timestamp != videoAt(ms(180), true).Timestamp {
		t.Fatalf("played %+v, want the newest keyframe", f)
	}
	if s := p.Stats().Tracks["cam"]; s.Skipped != 6 || s.WaitingKeyframe {
		t.Errorf("stats %+v", s)
	}
	// No newer keyframe: late deltas play at once to catch up instead of being dropped.
	for _, want := range []time.Duration{ms(210), ms(240)} {
		f, ok := pollNow(p, "cam")
		if !ok || f.Timestamp != videoAt(want, false).Timestamp {
			t.Fatalf("catch-up frame %+v, %v", f, ok)
		}
	}
}

func TestDelayAdaptsToJitter(t *testing.T) {
	for _, tc := range []struct {
		late, want time.Duration
	}{
		{0, ms(100)},
		{ms(50), ms(100)},
		{ms(250), ms(250)},
		{ms(900), ms(400)},
	} {
		p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
		start := clk.now()
		// Every fourth frame arrives late: more than 5% of them.
		for i := range 100 {
			arrival := start.Add(ms(20 * i))
			if i%4 == 0 {
				arrival = arrival.Add(tc.late)
			}
			push(t, p, "mic", audioAt(ms(20*i)), arrival)
		}
		if s := p.Stats(); s.Delay != tc.want || s.Jitter != tc.late {
			t.Errorf("late by %v: delay %v jitter %v, want %v", tc.late, s.Delay, s.Jitter, tc.want)
		}
	}
	// Fewer than 5% of frames late: the outliers do not raise the delay.
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
	for i := range 100 {
		arrival := clk.now().Add(ms(20 * i))
		if i%25 == 0 {
			arrival = arrival.Add(ms(300))
		}
		push(t, p, "mic", audioAt(ms(20*i)), arrival)
	}
	if s := p.Stats(); s.Delay != ms(100) || s.Jitter != 0 {
		t.Errorf("4%% outliers: delay %v jitter %v", s.Delay, s.Jitter)
	}
	p, clk = newTestPlayout(t, PlayoutConfig{MinDelay: ms(20), MaxDelay: ms(30)}, audioTrack())
	push(t, p, "mic", audioAt(0), clk.now())
	push(t, p, "mic", audioAt(ms(20)), clk.now().Add(ms(500)))
	if d := p.Stats().Delay; d != ms(30) {
		t.Errorf("custom bounds: delay %v", d)
	}
}

func TestReferenceFollowsDrift(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
	start := clk.now()
	// The sender's clock runs 0.1% slow: arrivals fall 1 ms per second behind the timestamps.
	var played int
	for i := range 3000 {
		capture := ms(20 * i)
		arrival := start.Add(capture + capture/1000)
		for clk.now().Before(arrival) {
			clk.advance(ms(1))
			for {
				if _, ok := pollNow(p, "mic"); !ok {
					break
				}
				played++
			}
		}
		push(t, p, "mic", audioAt(capture), arrival)
	}
	s := p.Stats()
	if s.Tracks["mic"].Skipped != 0 || s.Delay != ms(100) || played < 2990 {
		t.Errorf("after 60 s of drift: %+v, delay %v, played %d", s.Tracks["mic"], s.Delay, played)
	}
}

func TestOutputLatency(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{})
	if err := p.SetOutputLatency("mic", ms(40)); err != nil {
		t.Fatal(err)
	}
	now := clk.now()
	push(t, p, "mic", audioAt(0), now)
	push(t, p, "cam", videoAt(0, true), now)
	clk.advance(ms(100))
	if _, ok := pollNow(p, "mic"); !ok {
		t.Fatal("audio not due at 100 ms")
	}
	if _, ok := pollNow(p, "cam"); ok {
		t.Fatal("video due before the audio device plays")
	}
	clk.advance(ms(40))
	if _, ok := pollNow(p, "cam"); !ok {
		t.Fatal("video not due 40 ms after the audio")
	}
	if err := p.SetOutputLatency("mic", -1); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("negative: %v", err)
	}
	if err := p.SetOutputLatency("nope", 0); !errors.Is(err, ErrUnknownTrack) {
		t.Errorf("unknown: %v", err)
	}
	p.Close()
	if err := p.SetOutputLatency("mic", 0); !errors.Is(err, ErrClosed) {
		t.Errorf("closed: %v", err)
	}
}

func TestVideoFollowsAudio(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{})
	now := clk.now()
	push(t, p, "mic", audioAt(0), now)
	push(t, p, "cam", videoAt(ms(20), true), now)
	// Audio due at 100 ms is played 30 ms late, so video due at 120 ms waits until 150 ms.
	clk.advance(ms(130))
	if _, ok := pollNow(p, "mic"); !ok {
		t.Fatal("audio")
	}
	clk.advance(ms(19))
	if _, ok := pollNow(p, "cam"); ok {
		t.Fatal("video did not follow the late audio")
	}
	clk.advance(ms(1))
	if _, ok := pollNow(p, "cam"); !ok {
		t.Fatal("video not released with the audio's lag")
	}
}

func TestPlayoutConfigAndErrors(t *testing.T) {
	clk := newFakeClock()
	for _, cfg := range []PlayoutConfig{
		{MinDelay: -1}, {MaxDelay: -1}, {MaxBehind: -1}, {MaxFrames: -1}, {MaxBytes: -1},
		{MinDelay: ms(300), MaxDelay: ms(200)},
	} {
		if _, err := newPlayout([]Track{audioTrack()}, cfg, clk); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%+v: %v", cfg, err)
		}
	}
	if p, err := newPlayout([]Track{audioTrack()}, PlayoutConfig{MinDelay: ms(500)}, clk); err != nil || p.cfg.MaxDelay != ms(500) {
		t.Errorf("MinDelay over the default MaxDelay: %v", err)
	}
	if _, err := NewPlayout(nil, PlayoutConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("no tracks: %v", err)
	}
	if _, err := NewPlayout([]Track{audioTrack(), audioTrack()}, PlayoutConfig{}); !errors.Is(err, ErrInvalidTrack) {
		t.Errorf("same track twice: %v", err)
	}
	bad := videoTrack()
	bad.Width = 0
	if _, err := NewPlayout([]Track{bad}, PlayoutConfig{}); !errors.Is(err, ErrInvalidTrack) {
		t.Errorf("invalid track: %v", err)
	}

	p, _ := newTestPlayout(t, PlayoutConfig{})
	now := clk.now()
	if err := p.Push("nope", audioAt(0), now); !errors.Is(err, ErrUnknownTrack) {
		t.Errorf("unknown track: %v", err)
	}
	if err := p.Push("mic", Frame{}, now); !errors.Is(err, ErrInvalidFrame) {
		t.Errorf("empty frame: %v", err)
	}
	one := audioTrack()
	one.Name, one.Timescale = "one", 1
	p1, _ := newTestPlayout(t, PlayoutConfig{}, one)
	if err := p1.Push("one", Frame{Timestamp: math.MaxInt64/1_000_000_000 + 1, Data: []byte{1}}, now); !errors.Is(err, ErrTimestampRange) {
		t.Errorf("timestamp past the largest duration: %v", err)
	}
	if _, err := p.Next(context.Background(), "nope"); !errors.Is(err, ErrUnknownTrack) {
		t.Errorf("Next unknown: %v", err)
	}
}

func TestNextBlocksUntilDue(t *testing.T) {
	checkGoroutines(t)
	p, clk := newTestPlayout(t, PlayoutConfig{})
	push(t, p, "mic", audioAt(0), clk.now())
	got := make(chan Frame, 1)
	go func() {
		f, err := p.Next(context.Background(), "mic")
		if err != nil {
			t.Error(err)
		}
		got <- f
	}()
	waitFor(t, "Next to wait on a timer", func() bool { return clk.waiting() == 1 })
	clk.advance(ms(99))
	select {
	case <-got:
		t.Fatal("Next returned early")
	case <-time.After(10 * time.Millisecond):
	}
	clk.advance(ms(1))
	select {
	case f := <-got:
		if f.Timestamp != 0 {
			t.Fatalf("frame %+v", f)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Next did not return when due")
	}
}

func TestNextWakesOnPush(t *testing.T) {
	checkGoroutines(t)
	p, clk := newTestPlayout(t, PlayoutConfig{})
	got := make(chan Frame, 1)
	go func() {
		f, err := p.Next(context.Background(), "mic")
		if err != nil {
			t.Error(err)
		}
		got <- f
	}()
	waitBlocked(t, 1)
	// The frame arrived long ago, so it is due at once.
	clk.advance(time.Second)
	push(t, p, "mic", audioAt(0), clk.now().Add(-ms(120)))
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("Push did not wake Next")
	}
}

func TestNextCancel(t *testing.T) {
	checkGoroutines(t)
	p, clk := newTestPlayout(t, PlayoutConfig{})
	for _, withFrame := range []bool{false, true} {
		if withFrame {
			push(t, p, "mic", audioAt(0), clk.now())
		}
		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() {
			_, err := p.Next(ctx, "mic")
			errc <- err
		}()
		if withFrame {
			waitFor(t, "Next to wait on a timer", func() bool { return clk.waiting() == 1 })
		} else {
			waitBlocked(t, 1)
		}
		cancel()
		select {
		case err := <-errc:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Next after cancel: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Next ignored the cancellation")
		}
		if clk.waiting() != 0 {
			t.Error("timer left running after cancel")
		}
	}
	// A frame that is due is returned even with a done context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clk.advance(ms(100))
	if _, err := p.Next(ctx, "mic"); err != nil {
		t.Errorf("due frame with a done context: %v", err)
	}
	if _, err := p.Next(ctx, "mic"); !errors.Is(err, context.Canceled) {
		t.Errorf("empty track with a done context: %v", err)
	}
}

func TestCloseWhileBlocked(t *testing.T) {
	checkGoroutines(t)
	p, clk := newTestPlayout(t, PlayoutConfig{})
	push(t, p, "cam", videoAt(0, true), clk.now())
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, track := range []string{"mic", "cam"} {
		wg.Go(func() {
			_, err := p.Next(context.Background(), track)
			errs <- err
		})
	}
	waitBlocked(t, 2)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	for range 2 {
		if err := <-errs; !errors.Is(err, ErrClosed) {
			t.Errorf("blocked Next after Close: %v", err)
		}
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := p.Push("mic", audioAt(0), clk.now()); !errors.Is(err, ErrClosed) {
		t.Errorf("Push after Close: %v", err)
	}
	if _, err := p.Next(context.Background(), "mic"); !errors.Is(err, ErrClosed) {
		t.Errorf("Next after Close: %v", err)
	}
	if s := p.Stats().Tracks["cam"]; s.Buffered != 0 || s.BufferedBytes != 0 {
		t.Errorf("buffers kept after Close: %+v", s)
	}
}

// TestWaitBlockedOwnGoroutines checks the waitBlocked helper: a goroutine blocked in Next that
// another goroutine started, as one a failed test leaves behind, is not counted, so one failed test
// cannot make every later waitBlocked time out.
func TestWaitBlockedOwnGoroutines(t *testing.T) {
	checkGoroutines(t)
	p, _ := newTestPlayout(t, PlayoutConfig{})
	var wg sync.WaitGroup
	started := make(chan struct{})
	go func() {
		wg.Go(func() { p.Next(context.Background(), "cam") })
		close(started)
	}()
	<-started
	wg.Go(func() { p.Next(context.Background(), "mic") })
	waitFor(t, "both Next calls to block", func() bool {
		blocked := 0
		for _, stack := range goroutines() {
			if strings.Contains(stack, "[select") && strings.Contains(stack, "(*Playout).next(") {
				blocked++
			}
		}
		return blocked == 2
	})
	waitBlocked(t, 1)
	p.Close()
	wg.Wait()
}

// TestPlayoutConcurrent runs producers and consumers on the real clock; run it with -race -count=100.
func TestPlayoutConcurrent(t *testing.T) {
	checkGoroutines(t)
	p, err := NewPlayout([]Track{audioTrack(), videoTrack()}, PlayoutConfig{MinDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond, MaxBehind: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	const n = 200
	var consumers sync.WaitGroup
	for _, track := range []string{"mic", "cam"} {
		consumers.Go(func() {
			var last uint64
			var seen bool
			for {
				f, err := p.Next(context.Background(), track)
				if errors.Is(err, ErrClosed) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				if seen && f.Timestamp <= last {
					t.Errorf("%s went back from %d to %d", track, last, f.Timestamp)
				}
				last, seen = f.Timestamp, true
			}
		})
	}
	var producers sync.WaitGroup
	start := time.Now()
	producers.Go(func() {
		for i := range n {
			p.Push("mic", audioAt(ms(i)), start.Add(ms(i)))
			p.Stats()
		}
	})
	producers.Go(func() {
		for i := range n {
			p.Push("cam", videoAt(ms(i), i%10 == 0), start.Add(ms(i)))
			p.SetOutputLatency("cam", time.Duration(i%3)*time.Microsecond)
		}
	})
	producers.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := p.Stats()
		left := s.Tracks["mic"].Buffered + s.Tracks["cam"].Buffered
		if left == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	p.Close()
	consumers.Wait()
	s := p.Stats()
	for _, name := range []string{"mic", "cam"} {
		ts := s.Tracks[name]
		if ts.Received != n || ts.Played+ts.Late+ts.Duplicate+ts.Overflow+ts.Skipped+ts.NoKeyframe != n {
			t.Errorf("%s: frames lost track of: %+v", name, ts)
		}
	}
}

// FuzzPlayout feeds hostile timestamps, sizes and arrival times, polling between pushes.
func FuzzPlayout(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 2, 9})
	f.Add([]byte{1, 255, 255, 255, 255, 255, 255, 255, 127, 3, 0, 1, 2, 3, 4, 5, 6, 7, 200, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		p, clk := newTestPlayout(t, PlayoutConfig{MaxFrames: 4, MaxBytes: 64})
		last := map[string]uint64{}
		seen := map[string]bool{}
		for len(ops) >= 12 {
			track := "mic"
			if ops[0]&1 == 1 {
				track = "cam"
			}
			var ts uint64
			for _, b := range ops[1:9] {
				ts = ts<<8 | uint64(b)
			}
			size := int(ops[9]) % 80
			arrival := clk.now().Add(time.Duration(int8(ops[10])) * 7 * time.Millisecond)
			clk.advance(time.Duration(ops[11]) * time.Millisecond)
			ops = ops[12:]
			p.Push(track, Frame{Timestamp: ts, Keyframe: size%3 == 0, Data: make([]byte, size)}, arrival)
			for _, name := range []string{"mic", "cam"} {
				for {
					fr, ok := pollNow(p, name)
					if !ok {
						break
					}
					if seen[name] && fr.Timestamp <= last[name] {
						t.Fatalf("%s went back from %d to %d", name, last[name], fr.Timestamp)
					}
					last[name], seen[name] = fr.Timestamp, true
				}
			}
			checkBounds(t, p)
		}
	})
}

func TestFutureTimestampDoesNotMuteTrack(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
	start := clk.now()
	var played int
	run := func(from, to int, bogus int) {
		for i := from; i < to; i++ {
			capture := ms(20 * i)
			for clk.now().Before(start.Add(capture)) {
				clk.advance(ms(1))
				for {
					if _, ok := pollNow(p, "mic"); !ok {
						break
					}
					played++
				}
			}
			f := audioAt(capture)
			if i == bogus {
				f = audioAt(capture + time.Hour)
			}
			push(t, p, "mic", f, start.Add(capture))
		}
	}
	run(0, 500, 200)
	s := p.Stats().Tracks["mic"]
	if s.Ahead != 1 || s.Skipped != 0 || played < 490 {
		t.Fatalf("one frame an hour ahead: %+v, played %d", s, played)
	}
}

func TestAheadThresholdIsTwoSeconds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ahead time.Duration
		want  uint64
	}{
		{"1.9 s ahead plays", 1900 * time.Millisecond, 0},
		{"2 s ahead plays", 2 * time.Second, 0},
		{"2 s and 1 ms ahead is dropped", 2*time.Second + ms(1), 1},
		{"3 s ahead is dropped", 3 * time.Second, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
			start := clk.now()
			for i := range 50 {
				push(t, p, "mic", audioAt(ms(20*i)), start.Add(ms(20*i)))
			}
			at := ms(20 * 50)
			clk.advance(at)
			push(t, p, "mic", audioAt(at+tc.ahead), start.Add(at))
			if s := p.Stats().Tracks["mic"]; s.Ahead != tc.want || s.Received != 51 {
				t.Fatalf("stats %+v, want %d ahead", s, tc.want)
			}
		})
	}
}

func TestBadFirstTimestampRecovers(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
	start := clk.now()
	push(t, p, "mic", audioAt(time.Hour), start)
	var played []uint64
	for i := 1; i < 500; i++ {
		capture := ms(20 * i)
		for clk.now().Before(start.Add(capture)) {
			clk.advance(ms(1))
			for {
				f, ok := pollNow(p, "mic")
				if !ok {
					break
				}
				played = append(played, f.Timestamp)
			}
		}
		push(t, p, "mic", audioAt(capture), start.Add(capture))
	}
	// Within about two windows the real frames play again.
	if len(played) < 250 || played[len(played)-1] < audioAt(ms(9800)).Timestamp {
		t.Fatalf("played %d frames, last %v; stats %+v", len(played), played[len(played)-1:], p.Stats().Tracks["mic"])
	}
}

// A first frame stamped far ahead becomes the reference and mutes the track until the 2 s stale window passes, the figure docs/limits.md gives.
func TestBadFirstTimestampMutesForTwoToThreeSeconds(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
	start := clk.now()
	push(t, p, "mic", audioAt(time.Hour), start)
	var first time.Duration = -1
	for i := 1; i < 500 && first < 0; i++ {
		capture := ms(20 * i)
		for clk.now().Before(start.Add(capture)) {
			clk.advance(ms(1))
			if f, ok := pollNow(p, "mic"); ok && f.Timestamp < audioAt(time.Hour).Timestamp && first < 0 {
				first = clk.now().Sub(start)
			}
		}
		push(t, p, "mic", audioAt(capture), start.Add(capture))
	}
	if first < 2*time.Second || first > 3*time.Second {
		t.Fatalf("the first real frame played at %v, want 2 s to 3 s; stats %+v", first, p.Stats().Tracks["mic"])
	}
}

func TestSkippedFrameArrivingAgainIsLate(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, videoTrack())
	start := clk.now()
	for i := range 6 {
		push(t, p, "cam", videoAt(ms(30*i), i%3 == 0), start.Add(ms(30*i)))
	}
	clk.advance(ms(500))
	if f, ok := pollNow(p, "cam"); !ok || f.Timestamp != videoAt(ms(90), true).Timestamp {
		t.Fatalf("did not skip to the keyframe: %+v", f)
	}
	push(t, p, "cam", videoAt(ms(60), false), start.Add(ms(60)))
	f, ok := pollNow(p, "cam")
	if !ok || f.Timestamp != videoAt(ms(120), false).Timestamp {
		t.Fatalf("after a skipped delta arrived again, played %+v", f)
	}
	if s := p.Stats().Tracks["cam"]; s.Late != 1 {
		t.Errorf("stats %+v", s)
	}
}

func TestSkipTargetsDueKeyframe(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, videoTrack())
	start := clk.now()
	for i := range 4 {
		push(t, p, "cam", videoAt(ms(30*i), i == 0), start.Add(ms(30*i)))
	}
	// A keyframe that is not due yet: the late deltas catch up instead of jumping to it.
	push(t, p, "cam", videoAt(ms(400), true), start.Add(ms(110)))
	clk.advance(ms(250))
	for _, want := range []int{0, 30, 60, 90} {
		if f, ok := pollNow(p, "cam"); !ok || f.Timestamp != videoAt(ms(want), false).Timestamp {
			t.Fatalf("want frame at %d ms, got %+v", want, f)
		}
	}
}

func TestPushCopiesAndRejectsTheOldestWhenFull(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{MaxFrames: 2}, audioTrack())
	data := []byte{1, 2}
	level := &AudioLevel{Level: 5}
	f := audioAt(ms(40))
	f.Data, f.AudioLevel = data, level
	push(t, p, "mic", f, clk.now())
	push(t, p, "mic", audioAt(ms(60)), clk.now())
	data[0], level.Level = 9, 9
	push(t, p, "mic", audioAt(ms(20)), clk.now())
	if s := p.Stats().Tracks["mic"]; s.Overflow != 1 || s.Buffered != 2 {
		t.Fatalf("older frame into a full buffer: %+v", s)
	}
	clk.advance(ms(100))
	got, ok := pollNow(p, "mic")
	if !ok || got.Data[0] != 1 || got.AudioLevel.Level != 5 {
		t.Fatalf("buffered frame shares the caller's memory: %+v", got)
	}
}

func TestOneAudioTrack(t *testing.T) {
	second := audioTrack()
	second.Name = "mic2"
	if _, err := NewPlayout([]Track{audioTrack(), second}, PlayoutConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("two audio tracks: %v", err)
	}
}

func TestAudioLagWakesBlockedVideo(t *testing.T) {
	checkGoroutines(t)
	p, clk := newTestPlayout(t, PlayoutConfig{})
	start := clk.now()
	push(t, p, "mic", audioAt(0), start)
	push(t, p, "mic", audioAt(ms(20)), start.Add(ms(20)))
	push(t, p, "cam", videoAt(ms(40), true), start.Add(ms(40)))
	clk.advance(ms(130))
	if _, ok := pollNow(p, "mic"); !ok { // 30 ms late
		t.Fatal("audio")
	}
	got := make(chan Frame, 1)
	go func() {
		f, err := p.Next(context.Background(), "cam")
		if err != nil {
			t.Error(err)
		}
		got <- f
	}()
	// Video is due at 140 + 30 ms of audio lag.
	waitFor(t, "video to wait", func() bool { return clk.waiting() == 1 })
	if _, ok := pollNow(p, "mic"); !ok { // 10 ms late: video is now due at 150 ms
		t.Fatal("audio")
	}
	clk.advance(ms(20))
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("video Next kept the old audio lag")
	}
}

func TestTimeBaseJumpRestartsReference(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
	start := clk.now()
	var lastPlayed uint64
	for i := range 500 {
		capture := ms(20 * i)
		for clk.now().Before(start.Add(capture)) {
			clk.advance(ms(1))
			for {
				f, ok := pollNow(p, "mic")
				if !ok {
					break
				}
				lastPlayed = f.Timestamp
			}
		}
		ts := capture
		if i >= 200 {
			ts += time.Hour // the sender's clock jumped
		}
		push(t, p, "mic", audioAt(ts), start.Add(capture))
	}
	if lastPlayed < audioAt(time.Hour+ms(9000)).Timestamp {
		t.Fatalf("after the jump, last played %d; stats %+v", lastPlayed, p.Stats().Tracks["mic"])
	}
}

func TestLateArrivalsRaiseTheDelay(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{})
	var frames []simFrame
	for i := range 500 {
		capture := ms(20 * i)
		arrival := capture
		if i%10 == 0 {
			arrival += ms(300)
		}
		frames = append(frames, simFrame{"mic", audioAt(capture), i, capture, arrival})
	}
	simulate(t, p, clk, frames, 11*time.Second, nil)
	s := p.Stats()
	if s.Delay < ms(300) || s.Tracks["mic"].Late == 0 || s.Tracks["mic"].Late > 10 {
		t.Errorf("delay %v, %+v", s.Delay, s.Tracks["mic"])
	}
}

// TestOlderFramesForAWindowRestartOrder shows how an audio frame can play after a later one: when
// only frames older than the last one played arrive for a whole estimate window, as after a delivery
// stall that resumes from an older point, the playout takes the last frame played as the bad one and
// plays the older frames.
func TestOlderFramesForAWindowRestartOrder(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack())
	start := clk.now()
	var played []uint64
	const lag = 2500 * time.Millisecond
	for i := range 300 {
		at := ms(20 * i)
		for clk.now().Before(start.Add(at)) {
			clk.advance(ms(1))
			for {
				f, ok := pollNow(p, "mic")
				if !ok {
					break
				}
				played = append(played, f.Timestamp)
			}
		}
		capture := at
		if at >= 3*time.Second {
			capture -= lag // from here on, frames come 2.5 s late
		}
		push(t, p, "mic", audioAt(capture), start.Add(at))
	}
	back := 0
	for i := 1; i < len(played); i++ {
		if played[i] <= played[i-1] {
			back++
		}
	}
	s := p.Stats().Tracks["mic"]
	if back != 1 || s.Late < 90 {
		t.Fatalf("%d frames played after a later one; stats %+v", back, s)
	}
}

// TestSetTrackKeepsPosition checks that a slot keeps its play position while it is empty and when the
// same track comes back, and starts over for another track.
func TestSetTrackKeepsPosition(t *testing.T) {
	clk := newFakeClock()
	p, err := newSlotPlayout(PlayoutConfig{}, clk)
	if err != nil {
		t.Fatal(err)
	}
	cam, low := videoTrack(), lowVideo()
	start := clk.now()
	p.setTrack("video", &cam)
	p.setTrack("video", nil)
	p.setTrack("video", &cam)
	push(t, p, "video", videoAt(0, false), start)
	if _, ok := pollNow(p, "video"); ok {
		t.Fatal("a delta played before any keyframe")
	}
	for i, key := range []bool{true, false} {
		push(t, p, "video", videoAt(ms(33*(i+1)), key), start)
	}
	clk.advance(time.Second)
	for range 2 {
		if _, ok := pollNow(p, "video"); !ok {
			t.Fatal("nothing played")
		}
	}
	p.setTrack("video", nil)
	p.setTrack("video", &cam)
	for i, key := range []bool{true, false, false} {
		push(t, p, "video", videoAt(ms(33*(i+1)), key), clk.now())
	}
	f, ok := pollNow(p, "video")
	if want := videoAt(ms(99), false).Timestamp; !ok || f.Timestamp != want {
		t.Fatalf("after the re-add played %d (%v), want the next delta %d", f.Timestamp, ok, want)
	}
	if st := p.Stats().Tracks["video"]; st.Late != 1 || st.Duplicate != 1 || st.NoKeyframe != 1 {
		t.Fatalf("stats after the re-add %+v", st)
	}
	p.setTrack("video", &low)
	push(t, p, "video", videoAt(0, true), clk.now())
	if f, ok := pollNow(p, "video"); !ok || f.Timestamp != 0 {
		t.Fatalf("another track did not start over: %d (%v)", f.Timestamp, ok)
	}
}

// After a stall longer than the estimate window, the camera's backlog keeps arriving seconds late while
// the audio arrives on time. The first frame after the stall is a late video one, so the reference is 3 s
// later than the audio's; the audio must not stay dropped as "ahead" while late video keeps arriving.
func TestAudioRecoversWhileLateVideoKeepsArriving(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack(), videoTrack())
	start := clk.now()
	type arrival struct {
		at    time.Duration
		track string
		f     Frame
	}
	var evs []arrival
	const stallFrom, stallTo, lateBy = 3 * time.Second, 6 * time.Second, 3 * time.Second
	for j := 0; j < 600; j++ {
		c := time.Duration(j) * time.Second / 30
		at := c
		if c >= stallFrom {
			at = c + lateBy // the queue of the stall drains in real time: every frame is 3 s late
		}
		evs = append(evs, arrival{at, "cam", videoAt(c, j%60 == 0)})
	}
	for i := 0; i < 1000; i++ {
		if c := ms(20 * i); c < stallFrom || c >= stallTo {
			evs = append(evs, arrival{c, "mic", audioAt(c)})
		}
	}
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].at < evs[j].at })
	played := map[string]map[int]int{"mic": {}, "cam": {}} // frames played in each second
	for _, e := range evs {
		for clk.now().Before(start.Add(e.at)) {
			clk.advance(ms(1))
			for _, tr := range []string{"mic", "cam"} {
				for {
					if _, ok := pollNow(p, tr); !ok {
						break
					}
					played[tr][int(clk.now().Sub(start)/time.Second)]++
				}
			}
		}
		push(t, p, e.track, e.f, start.Add(e.at))
	}
	// The audio comes back after a window of its own ahead frames, by 9 s at the latest, and plays on to the end.
	for sec := 9; sec < 18; sec++ {
		if n := played["mic"][sec]; n < 40 {
			t.Errorf("second %d: %d audio frames played, want about 50 (ahead %d)", sec, n, p.Stats().Tracks["mic"].Ahead)
		}
	}
}

// A sender whose audio timeline falls behind the clock its video is stamped with (a microphone that stalls for a second,
// loses those samples and goes on stamping by count) makes every later audio frame later than the reference the video
// holds. Once that lasts a whole window while the video plays, the audio is shifted to play again, out of sync by what
// it fell behind, and the stats show how long the run of late frames lasted.
func TestAudioRecoversFromATimelineBehindTheVideo(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack(), videoTrack())
	start := clk.now()
	type arrival struct {
		at    time.Duration
		track string
		f     Frame
	}
	var evs []arrival
	for j := 0; j < 300; j++ { // 10 s of video stamped by the clock
		c := time.Duration(j) * time.Second / 30
		evs = append(evs, arrival{c, "cam", videoAt(c, j%60 == 0)})
	}
	for i := range 450 { // 50 frames of 20 ms a second, with nothing from 1 s to 2 s and the same count after it
		c := ms(20 * i)
		if c >= time.Second {
			c += time.Second
		}
		evs = append(evs, arrival{c, "mic", audioAt(ms(20 * i))})
	}
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].at < evs[j].at })
	played := map[string]map[int]int{"mic": {}, "cam": {}}
	var longest time.Duration
	for _, e := range evs {
		for clk.now().Before(start.Add(e.at)) {
			clk.advance(ms(1))
			for _, tr := range []string{"mic", "cam"} {
				for {
					if _, ok := pollNow(p, tr); !ok {
						break
					}
					played[tr][int(clk.now().Sub(start)/time.Second)]++
				}
			}
			longest = max(longest, p.Stats().Tracks["mic"].LateRun)
		}
		push(t, p, e.track, e.f, start.Add(e.at))
	}
	if longest < estimateWindow-ms(50) {
		t.Errorf("the longest run of late audio shown was %v, want about %v", longest, estimateWindow)
	}
	for sec := 5; sec < 10; sec++ {
		if n := played["mic"][sec]; n < 45 {
			t.Errorf("second %d: %d audio frames played, want 50 (stats %+v)", sec, n, p.Stats().Tracks["mic"])
		}
		if n := played["cam"][sec]; n < 25 {
			t.Errorf("second %d: %d pictures played, want 30", sec, n)
		}
	}
}

// Audio that is late on its own, with no other track playing, is the path's delay and not a timeline behind another:
// no shift is added, however long the frames are too late.
func TestLateAudioAloneIsNotShifted(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack(), videoTrack())
	start := clk.now()
	var longest time.Duration
	for i := range 300 { // 6 s of audio over a queue that grows by half a second a second from 1 s on
		c := ms(20 * i)
		at := c
		if c >= time.Second {
			at += (c - time.Second) / 2
		}
		longest = max(longest, p.Stats().Tracks["mic"].LateRun)
		for clk.now().Before(start.Add(at)) {
			clk.advance(ms(1))
			for {
				if _, ok := pollNow(p, "mic"); !ok {
					break
				}
			}
		}
		push(t, p, "mic", audioAt(c), start.Add(at))
	}
	p.mu.Lock()
	shift := p.tracks["mic"].shift
	p.mu.Unlock()
	if longest < estimateWindow || shift != 0 {
		t.Errorf("audio shifted by %v with no other track playing, after a run of %v", shift, longest)
	}
}

// A delay on the path that makes both tracks late, and drains, is not a timeline behind the others: the video plays
// late too, so the audio is not shifted, and nothing is left out of sync once the queue drained.
func TestCommonDelayDoesNotShiftTheAudio(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack(), videoTrack())
	start := clk.now()
	type arrival struct {
		at    time.Duration
		track string
		f     Frame
	}
	lagAt := func(c time.Duration) time.Duration { // a queue that grows by 0.5 s a second from 1 s to 5 s, then is gone
		if c >= time.Second && c < 5*time.Second {
			return (c - time.Second) / 2
		}
		return 0
	}
	var evs []arrival
	for j := range 300 {
		c := time.Duration(j) * time.Second / 30
		evs = append(evs, arrival{c + lagAt(c), "cam", videoAt(c, j%60 == 0)})
	}
	for i := range 500 {
		c := ms(20 * i)
		evs = append(evs, arrival{c + lagAt(c), "mic", audioAt(c)})
	}
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].at < evs[j].at })
	var maxShift time.Duration
	for _, e := range evs {
		for clk.now().Before(start.Add(e.at)) {
			clk.advance(ms(1))
			for _, tr := range []string{"mic", "cam"} {
				for {
					if _, ok := pollNow(p, tr); !ok {
						break
					}
				}
			}
			p.mu.Lock()
			maxShift = max(maxShift, p.tracks["mic"].shift)
			p.mu.Unlock()
		}
		push(t, p, e.track, e.f, start.Add(e.at))
	}
	p.mu.Lock()
	buffered := len(p.tracks["mic"].frames)
	p.mu.Unlock()
	if maxShift != 0 || buffered > 10 {
		t.Errorf("audio shifted by up to %v, %d frames buffered at the end, after a delay both tracks had", maxShift, buffered)
	}
}

// A shift ends once the frames arrive in time without it for a window: a timeline that fell behind and caught up again
// plays in sync again.
func TestShiftEndsWhenTheTimelineCatchesUp(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack(), videoTrack())
	start := clk.now()
	type arrival struct {
		at    time.Duration
		track string
		f     Frame
	}
	var evs []arrival
	for j := range 450 { // 15 s of video stamped by the clock
		c := time.Duration(j) * time.Second / 30
		evs = append(evs, arrival{c, "cam", videoAt(c, j%60 == 0)})
	}
	for i := range 750 { // audio 1 s behind the clock from 1 s to 7 s, then stamped by the clock again
		c := ms(20 * i)
		ts := c
		if c >= time.Second && c < 7*time.Second {
			ts = c - time.Second
		}
		evs = append(evs, arrival{c, "mic", audioAt(ts)})
	}
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].at < evs[j].at })
	var maxShift time.Duration
	for _, e := range evs {
		for clk.now().Before(start.Add(e.at)) {
			clk.advance(ms(1))
			for _, tr := range []string{"mic", "cam"} {
				for {
					if _, ok := pollNow(p, tr); !ok {
						break
					}
				}
			}
			p.mu.Lock()
			maxShift = max(maxShift, p.tracks["mic"].shift)
			p.mu.Unlock()
		}
		push(t, p, e.track, e.f, start.Add(e.at))
	}
	p.mu.Lock()
	shift := p.tracks["mic"].shift
	p.mu.Unlock()
	if maxShift < 500*time.Millisecond || shift != 0 {
		t.Errorf("shift reached %v and is %v at the end, want about 1 s and then 0", maxShift, shift)
	}
}

// A late burst, silence for a window, then one more late frame are two runs, not one that lasted the window.
func TestLateRunEndsWithSilence(t *testing.T) {
	p, clk := newTestPlayout(t, PlayoutConfig{}, audioTrack(), videoTrack())
	start := clk.now()
	push(t, p, "cam", videoAt(0, true), start)
	pollNow(p, "cam")
	for i := range 5 {
		push(t, p, "mic", audioAt(ms(20*i)), start)
	}
	clk.advance(time.Second) // all five are too late
	pollNow(p, "mic")
	clk.advance(2 * time.Second)
	push(t, p, "mic", audioAt(ms(100)), start.Add(ms(100)))
	pollNow(p, "mic")
	if r := p.Stats().Tracks["mic"].LateRun; r != 0 {
		t.Errorf("a run of %v after a new late frame, want a new run", r)
	}
}

// Taking the same track again keeps its shift; another track starts without it.
func TestSetTrackKeepsTheShift(t *testing.T) {
	p, err := newSlotPlayout(PlayoutConfig{}, newFakeClock())
	if err != nil {
		t.Fatal(err)
	}
	a := audioTrack()
	p.setTrack("audio", &a)
	p.tracks["audio"].shift = time.Second
	p.setTrack("audio", &a)
	if s := p.tracks["audio"].shift; s != time.Second {
		t.Errorf("the same track again: shift %v", s)
	}
	b := audioTrack()
	b.Name = "mic2"
	p.setTrack("audio", &b)
	if s := p.tracks["audio"].shift; s != 0 {
		t.Errorf("another track: shift %v", s)
	}
}
