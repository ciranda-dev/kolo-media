package kolomedia

import (
	"context"
	"testing"
	"time"
)

// keyedSource writes 30 frames a second with a keyframe every gop frames and one at once when a watcher asks.
func keyedSource(t *testing.T, p *TrackPublisher, c *Clock, gop int) {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(33 * time.Millisecond)
		defer tick.Stop()
		tr := p.Track()
		force := false
		for i := 0; ; i++ {
			select {
			case <-p.KeyframeRequests():
				force = true
			default:
			}
			now := time.Now()
			ts, err := c.At(now, tr.Timescale)
			if err != nil {
				return
			}
			data := make([]byte, 108)
			putCapture(data, now.Sub(c.Start()))
			key := i%gop == 0 || force
			if err := p.WriteFrame(Frame{Timestamp: ts, Keyframe: key, Data: data}); err == nil && key {
				force = false
			}
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
}

// A watcher whose video waits for a keyframe asks the publisher for one and gets it in well under the 10 s of the group.
func TestWatchRequestsKeyframe(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	keyedSource(t, addTrack(t, b, videoTrack()), c, 300)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps(), RequestKeyframes: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if f, _, err := w.Next(ctx, Video); err != nil || !f.Keyframe {
		t.Fatalf("first frame %+v: %v", f, err)
	}
	// The video loses the rest of its group: it waits for a keyframe.
	w.play.mu.Lock()
	w.play.tracks[Video.String()].needKey = true
	w.play.mu.Unlock()
	start := time.Now()
	for {
		f, _, err := w.Next(ctx, Video)
		if err != nil {
			t.Fatal(err)
		}
		if f.Keyframe {
			break
		}
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the keyframe came after %v", d)
	}
	if n := w.Stats().Video.KeyframeRequests; n == 0 {
		t.Fatalf("no request counted: %+v", w.Stats().Video)
	}
}

// Without the option the watch asks for nothing, and waits for the publisher's own keyframe.
func TestWatchDoesNotRequestByDefault(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	keyedSource(t, addTrack(t, b, videoTrack()), c, 300)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, _, err := w.Next(ctx, Video); err != nil {
		t.Fatal(err)
	}
	w.play.mu.Lock()
	w.play.tracks[Video.String()].needKey = true
	w.play.mu.Unlock()
	short, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	for {
		f, _, err := w.Next(short, Video)
		if err != nil {
			break
		}
		if f.Keyframe {
			t.Fatal("a keyframe came without a request, within 2 s of a 10 s group")
		}
	}
	if n := w.Stats().Video.KeyframeRequests; n != 0 {
		t.Fatalf("%d requests", n)
	}
}

// Requests are spaced: a video that keeps waiting asks again once a second, not on every frame it drops.
func TestWatchRequestsAreSpaced(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	tp := addTrack(t, b, videoTrack())
	keyedSource(t, tp, c, 300)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps(), RequestKeyframes: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, _, err := w.Next(ctx, Video); err != nil {
		t.Fatal(err)
	}
	w.play.mu.Lock()
	w.play.tracks[Video.String()].needKey = true
	w.play.mu.Unlock()
	// hold the playout waiting: the frames are dropped as they come
	short, stop := context.WithTimeout(ctx, 3500*time.Millisecond)
	defer stop()
	for {
		f, _, err := w.Next(short, Video)
		if err != nil {
			break
		}
		if f.Keyframe {
			w.play.mu.Lock()
			w.play.tracks[Video.String()].needKey = true
			w.play.mu.Unlock()
		}
	}
	if n := w.Stats().Video.KeyframeRequests; n < 1 || n > 4 {
		t.Fatalf("%d requests in 3.5 s, want at most one a second", n)
	}
}

// A video that stops delivering frames, as one does that lost a group on the way, is asked about once a second.
func TestWatchRequestsWhenVideoStalls(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	src := startSource(t, addTrack(t, b, videoTrack()), c, 33*time.Millisecond, 300)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps(), RequestKeyframes: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for range 10 {
		if _, _, err := w.Next(ctx, Video); err != nil {
			t.Fatal(err)
		}
	}
	if n := w.Stats().Video.KeyframeRequests; n != 0 {
		t.Fatalf("%d requests while frames flow", n)
	}
	src.Stop()
	deadline := time.Now().Add(10 * time.Second)
	for w.Stats().Video.KeyframeRequests == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := w.Stats().Video.KeyframeRequests; n != 1 {
		t.Fatalf("%d requests after the video went quiet, want 1", n)
	}
}

// A publisher that goes quiet and never answers is asked at growing intervals, not once a second.
func TestWatchQuietVideoIsNotAskedEverySecond(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	src := startSource(t, addTrack(t, b, videoTrack()), NewClock(), 33*time.Millisecond, 300)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps(), RequestKeyframes: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for range 10 {
		if _, _, err := w.Next(ctx, Video); err != nil {
			t.Fatal(err)
		}
	}
	src.Stop()
	time.Sleep(8 * time.Second)
	if n := w.Stats().Video.KeyframeRequests; n < 1 || n > 3 {
		t.Fatalf("%d requests in 8 s of silence, want 1 to 3 (1 s apart would be 8)", n)
	}
}

// With the camera off, the track is gone from the catalog and the watch stops asking.
func TestWatchStopsAskingWhenTheTrackIsRemoved(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	tp := addTrack(t, b, videoTrack())
	src := startSource(t, tp, NewClock(), 33*time.Millisecond, 300)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps(), RequestKeyframes: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, _, err := w.Next(ctx, Video); err != nil {
		t.Fatal(err)
	}
	src.Stop()
	if err := b.RemoveTrack(tp.Track().Name); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for w.Stats().Video.Track != "" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if w.Stats().Video.Track != "" {
		t.Fatal("the watch kept the removed track")
	}
	n := w.Stats().Video.KeyframeRequests
	time.Sleep(3 * time.Second)
	if got := w.Stats().Video.KeyframeRequests; got != n {
		t.Fatalf("%d requests after the track was removed, %d before", got, n)
	}
}
