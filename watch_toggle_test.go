package kolomedia

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	kolomoq "github.com/ciranda-dev/kolo-moq"
)

// playsAfter reports whether w plays a video frame captured at or after since within d.
func playsAfter(w *Watcher, c *Clock, since time.Time, d time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for {
		f, _, err := w.Next(ctx, Video)
		if err != nil {
			return false
		}
		if !captured(c, f).Before(since) {
			return true
		}
	}
}

// TestWatchTrackReAdd removes the camera track and adds it again under the same name, through a
// relay, to 8 viewers: every viewer must play the new track, not keep the old subscription.
func TestWatchTrackReAdd(t *testing.T) {
	for _, between := range []int{0, 1} {
		t.Run(fmt.Sprintf("versions between %d", between), func(t *testing.T) {
			checkGoroutines(t)
			rm := newSecRelay(t)
			pub := newSecNode(t)
			ns := memberNS(pub.pub)
			b, err := NewBroadcast(pub.origin, ns)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { b.Close() })
			rm.mu.Lock()
			rm.members[string(pub.pub)] = true
			rm.mu.Unlock()
			ps := pub.dial(t, rm.addr, rm.pub, pub.origin)
			if _, err := ps.PublishNamespace(testCtx(t), ns); err != nil {
				t.Fatal(err)
			}
			clock := NewClock()
			startSource(t, addTrack(t, b, audioTrack()), clock, 20*time.Millisecond, 1)
			src := startSource(t, addTrack(t, b, videoTrack()), clock, 33*time.Millisecond, 10)
			var ws []*Watcher
			for range 8 {
				v := newSecNode(t)
				rm.mu.Lock()
				rm.members[string(v.pub)] = true
				rm.mu.Unlock()
				w, err := Watch(testCtx(t), v.dial(t, rm.addr, rm.pub, nil), ns, WatchOptions{Capabilities: avCaps()})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { w.Close() })
				ws = append(ws, w)
			}
			const rounds = 10
			stuck := 0
			for round := range rounds {
				src.Stop()
				if err := b.RemoveTrack("cam"); err != nil {
					t.Fatal(err)
				}
				for i := range between {
					// Not selected: the catalog changes, the selection does not.
					pcm := audioTrack()
					pcm.Name, pcm.Codec = fmt.Sprintf("pcm-%d-%d", round, i), PCMS16LE
					addTrack(t, b, pcm)
				}
				t0 := time.Now()
				src = startSource(t, addTrack(t, b, videoTrack()), clock, 33*time.Millisecond, 10)
				var mu sync.Mutex
				var wg sync.WaitGroup
				for i, w := range ws {
					wg.Go(func() {
						if !playsAfter(w, clock, t0, 5*time.Second) {
							mu.Lock()
							stuck++
							mu.Unlock()
							st := w.Stats()
							t.Logf("round %d viewer %d: no video of the new track: catalog %d, video %+v", round, i, st.Catalog, st.Video)
						}
					})
				}
				wg.Wait()
			}
			if stuck > 0 {
				t.Errorf("%d of %d viewer-rounds played no video of the re-added track", stuck, rounds*len(ws))
			}
		})
	}
}

// fakeBroadcast writes catalog groups and track instances itself, so a test controls what a
// watcher sees and when.
type fakeBroadcast struct {
	t       *testing.T
	origin  *kolomoq.Origin
	catalog *kolomoq.TrackWriter
}

func newFakeBroadcast(t *testing.T) *fakeBroadcast {
	o := newOrigin(t, nil)
	w, err := o.CreateTrack(testNS, CatalogTrack, &kolomoq.TrackOptions{Priority: CatalogPriority})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return &fakeBroadcast{t: t, origin: o, catalog: w}
}

// publish writes the next catalog group: c, or an object that is no catalog when c is nil.
func (f *fakeBroadcast) publish(c *Catalog) {
	f.t.Helper()
	g, err := f.catalog.NewGroup()
	if err != nil {
		f.t.Fatal(err)
	}
	data := []byte{0xff}
	if c != nil {
		c.Version = g.ID()
		if data, err = c.MarshalBinary(); err != nil {
			f.t.Fatal(err)
		}
	}
	if _, err := g.WriteObject(data); err != nil {
		f.t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		f.t.Fatal(err)
	}
}

// instance creates a new instance of track tr and writes frames to it, a keyframe every gop, until it ends.
func (f *fakeBroadcast) instance(tr Track, c *Clock, gop int) (*TrackPublisher, *source) {
	f.t.Helper()
	w, err := f.origin.CreateTrack(testNS, tr.Name, &kolomoq.TrackOptions{Priority: VideoPriority})
	if err != nil {
		f.t.Fatal(err)
	}
	pk, err := NewPackager(tr, PackagerOptions{})
	if err != nil {
		f.t.Fatal(err)
	}
	p := &TrackPublisher{b: &Broadcast{}, track: tr, writer: w, packager: pk}
	f.t.Cleanup(func() { p.end() })
	return p, startSource(f.t, p, c, 33*time.Millisecond, gop)
}

func (f *fakeBroadcast) end(p *TrackPublisher, src *source) {
	f.t.Helper()
	src.Stop()
	if err := p.end(); err != nil {
		f.t.Fatal(err)
	}
}

// TestWatchStaleTrackEnd orders by hand the end of a track and the catalog that lists the track's
// next instance under the same name. The removal catalog in between is not a catalog, as when the
// watcher sees the re-add first and skips the older version.
func TestWatchStaleTrackEnd(t *testing.T) {
	cam := videoTrack()
	start := func(t *testing.T) (*fakeBroadcast, *Watcher, *Clock, *TrackPublisher, *source) {
		checkGoroutines(t)
		f := newFakeBroadcast(t)
		c := NewClock()
		p, src := f.instance(cam, c, 10)
		f.publish(&Catalog{Tracks: []Track{cam}})
		sub, _ := publisherPair(t, f.origin)
		w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
		if !playsAfter(w, c, time.Time{}, 10*time.Second) {
			t.Fatal("no video")
		}
		return f, w, c, p, src
	}
	check := func(t *testing.T, w *Watcher, c *Clock, t0 time.Time) {
		t.Helper()
		if !playsAfter(w, c, t0, 10*time.Second) {
			t.Fatalf("no video of the new instance: %+v", w.Stats())
		}
		if st := w.Stats(); st.Catalog != 2 || st.BadCatalogs != 1 || st.Video.Subscriptions != 2 || st.Video.Err != nil {
			t.Errorf("stats %+v", st)
		}
	}

	t.Run("ends after the catalog", func(t *testing.T) {
		f, w, c, p, src := start(t)
		f.publish(nil)
		f.publish(&Catalog{Tracks: []Track{cam}})
		waitFor(t, "catalog 2", func() bool { return w.Stats().Catalog == 2 })
		// Holding applying keeps the watcher from subscribing again before the new instance exists,
		// as a publisher creates it before the catalog that lists it.
		w.applying.Lock()
		f.end(p, src)
		waitFor(t, "the old track to end", func() bool { return w.Stats().Video.Err != nil })
		t0 := time.Now()
		f.instance(cam, c, 10)
		w.applying.Unlock()
		check(t, w, c, t0)
	})

	t.Run("ends before the catalog", func(t *testing.T) {
		f, w, c, p, src := start(t)
		f.end(p, src)
		waitFor(t, "the old track to end", func() bool { return w.Stats().Video.Err != nil })
		t0 := time.Now()
		f.instance(cam, c, 10)
		f.publish(nil)
		f.publish(&Catalog{Tracks: []Track{cam}})
		check(t, w, c, t0)
	})
}

// TestWatchTrackEndsUnderItsCatalog ends the instance a subscription got while the catalog it
// started under still lists the track, and a new instance exists: a relay that served the SUBSCRIBE
// before the old track's end reached it. The watcher subscribes again once; a second end under the
// same catalog is final. A subscription resumed under a newer catalog still gets its one retry.
func TestWatchTrackEndsUnderItsCatalog(t *testing.T) {
	cam := videoTrack()
	start := func(t *testing.T) (*fakeBroadcast, *Watcher, *Clock, *TrackPublisher, *source) {
		checkGoroutines(t)
		f := newFakeBroadcast(t)
		c := NewClock()
		p, src := f.instance(cam, c, 10)
		f.publish(&Catalog{Tracks: []Track{cam}})
		sub, _ := publisherPair(t, f.origin)
		w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
		if !playsAfter(w, c, time.Time{}, 10*time.Second) {
			t.Fatal("no video")
		}
		return f, w, c, p, src
	}
	// next ends the running instance and makes a new one before the watcher may subscribe again,
	// and reports whether the new one plays.
	next := func(t *testing.T, f *fakeBroadcast, w *Watcher, c *Clock, p *TrackPublisher, src *source) (*TrackPublisher, *source, bool) {
		t.Helper()
		// Holding applying keeps the watcher from subscribing again before the new instance exists.
		w.applying.Lock()
		f.end(p, src)
		waitFor(t, "the instance to end", func() bool { return w.Stats().Video.Err != nil })
		t0 := time.Now()
		p, src = f.instance(cam, c, 10)
		w.applying.Unlock()
		return p, src, playsAfter(w, c, t0, 2*time.Second)
	}

	t.Run("same catalog", func(t *testing.T) {
		f, w, c, p, src := start(t)
		p, src, ok := next(t, f, w, c, p, src)
		if !ok {
			t.Fatalf("no video of the second instance: %+v", w.Stats())
		}
		if _, _, ok := next(t, f, w, c, p, src); ok {
			t.Fatal("subscribed a third time under the same catalog")
		}
		if st := w.Stats(); st.Catalog != 0 || st.Video.Subscriptions != 2 || st.Video.Err == nil {
			t.Errorf("after the second end: %+v", st)
		}
	})

	t.Run("resumed under a newer catalog", func(t *testing.T) {
		f, w, c, p, src := start(t)
		f.publish(&Catalog{Tracks: []Track{cam}})
		waitFor(t, "catalog 1", func() bool { return w.Stats().Catalog == 1 })
		p, src, ok := next(t, f, w, c, p, src)
		if !ok {
			t.Fatalf("no video after the newer catalog: %+v", w.Stats())
		}
		if _, _, ok := next(t, f, w, c, p, src); !ok {
			t.Fatalf("no retry under the catalog the resumed subscription started under: %+v", w.Stats())
		}
		if st := w.Stats(); st.Video.Subscriptions != 3 {
			t.Errorf("stats %+v", st)
		}
	})
}

// TestWatchReAddKeepsPlayout applies the removal of the camera, then a catalog that lists it again,
// while the subscription made for the re-add gets the same instance: a watcher that already followed
// the new instance and then took the delayed removal, or a relay that serves the SUBSCRIBE before it
// saw the old instance end. The group it gets again must not play a second time, and the frames after
// the last one played go on without waiting for a keyframe.
func TestWatchReAddKeepsPlayout(t *testing.T) {
	checkGoroutines(t)
	cam := videoTrack()
	f := newFakeBroadcast(t)
	c := NewClock()
	// One keyframe every 60 frames, so the group sent again holds frames already played.
	p, src := f.instance(cam, c, 60)
	f.publish(&Catalog{Tracks: []Track{cam}})
	sub, _ := publisherPair(t, f.origin)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	before := collect(t, w, c, Video, 5)
	last := before[len(before)-1].f.Timestamp
	f.publish(&Catalog{Tracks: []Track{audioTrack()}})
	waitFor(t, "the removal", func() bool { return w.Stats().Video.Track == "" })
	f.publish(&Catalog{Tracks: []Track{audioTrack(), cam}})
	waitFor(t, "the re-add", func() bool { return w.Stats().Video.Subscriptions == 2 })
	for _, pl := range collect(t, w, c, Video, 5) {
		if pl.f.Timestamp <= last {
			t.Fatalf("frame %d played again after the re-add; the last one before was %d", pl.f.Timestamp, last)
		}
		last = pl.f.Timestamp
	}
	if st := w.Stats().Playout.Tracks["video"]; st.NoKeyframe != 0 {
		t.Errorf("%d frames waited for a keyframe after the re-add", st.NoKeyframe)
	}
	f.end(p, src)
}

// TestSubscribeOptionsBoundLateStreams checks that a media subscription asks for both delivery
// timeouts, so after PUBLISH_DONE it waits MaxDelay plus MaxBehind for a counted stream that never
// arrives (reset before its header) instead of the publisher's 30 s group timeout.
func TestSubscribeOptionsBoundLateStreams(t *testing.T) {
	cfg := PlayoutConfig{MaxDelay: 400 * time.Millisecond, MaxBehind: 50 * time.Millisecond}
	for _, kind := range []Kind{Audio, Video} {
		o := subscribeOptions(cfg, kind)
		if o.ObjectDeliveryTimeout != 450*time.Millisecond || o.SubgroupDeliveryTimeout != 450*time.Millisecond {
			t.Errorf("%v: object timeout %v, subgroup timeout %v, want 450ms each", kind, o.ObjectDeliveryTimeout, o.SubgroupDeliveryTimeout)
		}
		if want := map[Kind]kolomoq.Join{Audio: kolomoq.JoinNextObject, Video: kolomoq.JoinCurrentGroup}[kind]; o.Join != want {
			t.Errorf("%v: join %v, want %v", kind, o.Join, want)
		}
	}
}
