package kolomedia

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	kolomoq "github.com/ciranda-dev/kolo-moq"
)

type played struct {
	f      Frame
	track  Track
	offset time.Duration // play time minus capture time
}

func collect(t *testing.T, w *Watcher, c *Clock, kind Kind, n int) []played {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out []played
	for len(out) < n {
		f, tr, err := w.Next(ctx, kind)
		if err != nil {
			t.Errorf("%s after %d frames: %v", kind, len(out), err)
			return out
		}
		out = append(out, played{f, tr, time.Since(captured(c, f))})
	}
	return out
}

func collectAV(t *testing.T, w *Watcher, c *Clock, na, nv int) (audio, video []played) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Go(func() { audio = collect(t, w, c, Audio, na) })
	wg.Go(func() { video = collect(t, w, c, Video, nv) })
	wg.Wait()
	return audio, video
}

func median(ps []played) time.Duration {
	var d []time.Duration
	for _, p := range ps {
		d = append(d, p.offset)
	}
	slices.Sort(d)
	return d[len(d)/2]
}

// syncTolerance bounds the difference between the audio and video capture-to-play offsets end to
// end: the 5 ms of the playout itself plus timer and scheduling delays on a loaded test machine.
const syncTolerance = 15 * time.Millisecond

func TestEndToEnd(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
	startSource(t, addTrack(t, b, videoTrack()), c, 33*time.Millisecond, 30)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	if s := w.Tracks(); s.Audio == nil || s.Video == nil || s.Audio.Name != "mic" || s.Video.Name != "cam" {
		t.Fatalf("selected %+v", s)
	}
	audio, video := collectAV(t, w, c, 100, 60)
	if len(video) == 0 || !video[0].f.Keyframe {
		t.Fatal("video did not start on a keyframe")
	}
	for i := 1; i < len(video); i++ {
		if video[i].f.Timestamp <= video[i-1].f.Timestamp || video[i].track.Name != "cam" {
			t.Fatalf("video frame %d out of order or from %q", i, video[i].track.Name)
		}
	}
	ma, mv := median(audio[20:]), median(video[10:])
	t.Logf("capture to play: audio %v, video %v; %+v", ma, mv, w.Stats())
	if (ma - mv).Abs() > syncTolerance {
		t.Errorf("audio and video play %v apart", ma-mv)
	}
	if ma > 300*time.Millisecond {
		t.Errorf("latency %v on loopback", ma)
	}
	s := w.Stats()
	if s.Video.Gaps != 0 || s.Video.Malformed != 0 || s.Audio.Malformed != 0 || s.Catalog != 2 {
		t.Errorf("stats %+v", s)
	}
}

func TestLateJoinerStartsAtKeyframe(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	v := addTrack(t, b, videoTrack())
	startSource(t, v, c, 33*time.Millisecond, 45)
	time.Sleep(1200 * time.Millisecond) // the broadcast runs into its second group
	sub, _ := publisherPair(t, o)
	joined := time.Now()
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	video := collect(t, w, c, Video, 20)
	if len(video) == 0 || !video[0].f.Keyframe {
		t.Fatal("late joiner did not start on a keyframe")
	}
	if !captured(c, video[0].f).Before(joined) {
		t.Error("the current group's keyframe did not come from the cache")
	}
	if d := w.Stats().Playout.Delay; d != 100*time.Millisecond {
		t.Errorf("cached frames raised the delay to %v", d)
	}
}

func TestCatalogUpdateSwitchesRendition(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
	startSource(t, addTrack(t, b, lowVideo()), c, 33*time.Millisecond, 15)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	waitTrack := func(name string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		first := true
		for {
			f, tr, err := w.Next(ctx, Video)
			if err != nil {
				t.Fatalf("waiting for %s: %v", name, err)
			}
			if tr.Name == name {
				if first && !f.Keyframe {
					t.Fatalf("%s started on a delta frame", name)
				}
				if !first {
					return
				}
				first = false
			}
		}
	}
	waitTrack("cam-low")
	high := startSource(t, addTrack(t, b, videoTrack()), c, 33*time.Millisecond, 15)
	waitTrack("cam")
	if s := w.Tracks(); s.Video.Name != "cam" || w.Catalog().Version != 3 {
		t.Fatalf("after the better rendition: %+v", s)
	}
	high.Stop()
	if err := b.RemoveTrack("cam"); err != nil {
		t.Fatal(err)
	}
	waitTrack("cam-low")
	if s := w.Stats(); s.Video.Subscriptions != 3 || s.Audio.Subscriptions != 1 || s.Audio.Err != nil {
		t.Errorf("stats %+v", s)
	}
	collect(t, w, c, Audio, 5)
}

func TestRelay(t *testing.T) {
	checkGoroutines(t)
	relay, err := kolomoq.NewRelay(nil)
	if err != nil {
		t.Fatal(err)
	}
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
	startSource(t, addTrack(t, b, videoTrack()), c, 33*time.Millisecond, 15)
	pub, _ := dialPair(t,
		func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
			return relay.NewServer(ctx, conn)
		},
		func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
			return kolomoq.NewClient(ctx, conn, &kolomoq.Config{Origin: o})
		})
	if _, err := pub.PublishNamespace(testCtx(t), testNS); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 2 {
		sub, _ := dialPair(t,
			func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
				return relay.NewServer(ctx, conn)
			},
			func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
				return kolomoq.NewClient(ctx, conn, nil)
			})
		w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
		wg.Go(func() {
			audio, video := collectAV(t, w, c, 50, 30)
			if len(video) == 0 || !video[0].f.Keyframe {
				t.Error("relayed video did not start on a keyframe")
				return
			}
			if d := (median(audio[10:]) - median(video[5:])).Abs(); d > syncTolerance {
				t.Errorf("relayed audio and video play %v apart", d)
			}
		})
	}
	wg.Wait()
}

func TestPartialFailure(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
	v := addTrack(t, b, videoTrack())
	vs := startSource(t, v, c, 33*time.Millisecond, 15)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	collectAV(t, w, c, 10, 5)

	// The video track ends without a catalog change: video reports it, audio plays on.
	vs.Stop()
	if err := v.end(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the video error", func() bool { return errors.Is(w.Stats().Video.Err, ErrTrackEnded) })
	collect(t, w, c, Audio, 20)

	// A track removed from the catalog stops; Next for its kind blocks.
	v2 := videoTrack()
	v2.Name, v2.Width, v2.Height = "cam2", 1920, 1080
	p2 := addTrack(t, b, v2)
	startSource(t, p2, c, 33*time.Millisecond, 15)
	waitFor(t, "cam2", func() bool { s := w.Tracks(); return s.Video != nil && s.Video.Name == "cam2" })
	collect(t, w, c, Video, 3)
	if err := p2.Close(); err != nil && !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	waitFor(t, "no video", func() bool { return w.Tracks().Video == nil })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, _, err := w.Next(ctx, Video); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Next with no video track: %v", err)
	}
	collect(t, w, c, Audio, 20)
}

func TestWriteFrameFailure(t *testing.T) {
	o := newOrigin(t, &kolomoq.OriginConfig{MaxObjectSize: 1024})
	b := newBroadcast(t, o)
	v := addTrack(t, b, videoTrack())
	a := addTrack(t, b, audioTrack())
	if err := v.WriteFrame(Frame{Timestamp: 1, Keyframe: true, Data: make([]byte, 2000)}); !errors.Is(err, kolomoq.ErrObjectTooLarge) {
		t.Fatalf("oversized keyframe: %v", err)
	}
	if err := v.WriteFrame(Frame{Timestamp: 2, Data: []byte{1}}); !errors.Is(err, ErrNeedKeyframe) {
		t.Fatalf("delta after a failed keyframe: %v", err)
	}
	if err := v.WriteFrame(Frame{Timestamp: 1, Keyframe: true, Data: []byte{1}}); err != nil {
		t.Fatalf("the failed frame's timestamp again: %v", err)
	}
	if err := v.WriteFrame(Frame{Timestamp: 2, Data: make([]byte, 2000)}); !errors.Is(err, kolomoq.ErrObjectTooLarge) {
		t.Fatalf("oversized delta: %v", err)
	}
	if err := a.WriteFrame(Frame{Timestamp: 2, Data: []byte{1}}); err != nil {
		t.Fatalf("audio after a video failure: %v", err)
	}
	for i, key := range []bool{true, false} {
		if err := v.WriteFrame(Frame{Timestamp: uint64(3 + i), Keyframe: key, Data: []byte{1}}); err != nil {
			t.Fatalf("frame %d after recovery: %v", i, err)
		}
	}
	if err := v.WriteFrame(Frame{Timestamp: 4, Data: []byte{1}}); !errors.Is(err, ErrTimestampOrder) {
		t.Errorf("packager errors pass through: %v", err)
	}
}

func TestBroadcastCatalog(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	sub, _ := publisherPair(t, o)
	ctx := testCtx(t)
	cs, err := sub.Subscribe(ctx, testNS, CatalogTrack, &kolomoq.SubscribeOptions{Join: kolomoq.JoinCurrentGroup})
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	next := func() *Catalog {
		t.Helper()
		for {
			obj, err := cs.ReadObject(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if obj.Status != kolomoq.StatusNormal {
				continue
			}
			c, err := ParseCatalog(obj.Payload, CatalogLimits{})
			if err != nil || c.Version != obj.Group || obj.ID != 0 {
				t.Fatalf("catalog object %d/%d: %+v, %v", obj.Group, obj.ID, c, err)
			}
			if obj.Priority != CatalogPriority {
				t.Errorf("catalog priority %d", obj.Priority)
			}
			return c
		}
	}
	if c := next(); c.Version != 0 || len(c.Tracks) != 0 {
		t.Fatalf("first catalog %+v", c)
	}
	a := audioTrack()
	a.Priority = 0
	addTrack(t, b, a)
	if c := next(); c.Version != 1 || c.Tracks[0].Priority != AudioPriority {
		t.Fatalf("after AddTrack %+v", c)
	}
	v := videoTrack()
	v.Priority = 0
	addTrack(t, b, v)
	if c := next(); c.Version != 2 || len(c.Tracks) != 2 || c.Tracks[1].Priority != VideoPriority {
		t.Fatalf("after the second AddTrack %+v", c)
	}
	if got := b.Catalog(); got.Version != 2 || len(got.Tracks) != 2 {
		t.Errorf("Catalog() = %+v", got)
	}
	named := audioTrack()
	named.Name = CatalogTrack
	for _, bad := range []Track{audioTrack(), named, {Name: CatalogTrack, Kind: Audio}, {Name: "x"}} {
		if _, err := b.AddTrack(bad, PackagerOptions{}); !errors.Is(err, ErrInvalidTrack) && !errors.Is(err, ErrInvalidCatalog) {
			t.Errorf("AddTrack(%q): %v", bad.Name, err)
		}
	}
	if err := b.RemoveTrack("nope"); !errors.Is(err, ErrUnknownTrack) {
		t.Errorf("RemoveTrack unknown: %v", err)
	}
	if err := b.RemoveTrack("mic"); err != nil {
		t.Fatal(err)
	}
	if c := next(); c.Version != 3 || len(c.Tracks) != 1 || c.Tracks[0].Name != "cam" {
		t.Fatalf("after RemoveTrack %+v", c)
	}
	cam := b.tracks["cam"]
	if err := cam.Close(); err != nil {
		t.Fatal(err)
	}
	next()
	again := addTrack(t, b, videoTrack())
	if err := cam.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if c := next(); c.Version != 5 || len(c.Tracks) != 1 || b.tracks["cam"] != again {
		t.Fatalf("closing a removed track removed its replacement: %+v", c)
	}
	if err := cam.WriteFrame(Frame{Timestamp: 1, Keyframe: true, Data: []byte{1}}); !errors.Is(err, ErrClosed) {
		t.Errorf("WriteFrame after Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if _, err := b.AddTrack(audioTrack(), PackagerOptions{}); !errors.Is(err, ErrClosed) {
		t.Errorf("AddTrack after Close: %v", err)
	}
	if err := b.RemoveTrack("cam"); !errors.Is(err, ErrClosed) {
		t.Errorf("RemoveTrack after Close: %v", err)
	}
	if err := again.Close(); err != nil {
		t.Errorf("track Close after the broadcast's: %v", err)
	}
	if err := again.WriteFrame(Frame{Timestamp: 1, Keyframe: true, Data: []byte{1}}); !errors.Is(err, ErrClosed) {
		t.Errorf("WriteFrame after the broadcast closed: %v", err)
	}
	if _, err := NewBroadcast(o, testNS); err != nil {
		t.Errorf("a new broadcast after Close: %v", err)
	}
}

func TestPublisherEnds(t *testing.T) {
	checkGoroutines(t)
	for _, how := range []string{"broadcast", "session"} {
		o := newOrigin(t, nil)
		b := newBroadcast(t, o)
		c := NewClock()
		startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
		sub, pub := publisherPair(t, o)
		w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
		collect(t, w, c, Audio, 3)
		errc := make(chan error, 1)
		go func() {
			for {
				if _, _, err := w.Next(context.Background(), Video); err != nil {
					errc <- err
					return
				}
			}
		}()
		want := ErrBroadcastEnded
		if how == "broadcast" {
			b.Close()
		} else {
			pub.Close()
			want = ErrSessionEnded
		}
		select {
		case err := <-errc:
			if !errors.Is(err, want) {
				t.Errorf("%s ended: %v", how, err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("%s ended: Next still blocked", how)
		}
		if _, _, err := w.Next(context.Background(), Audio); !errors.Is(err, want) {
			t.Errorf("%s ended, later Next: %v", how, err)
		}
	}
}

func TestWatchCancelAndClose(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, _, err := w.Next(ctx, Video)
		errc <- err
	}()
	waitBlocked(t, 1)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Errorf("Next after cancel: %v", err)
	}

	go func() {
		_, _, err := w.Next(context.Background(), Video)
		errc <- err
	}()
	waitBlocked(t, 1)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; !errors.Is(err, ErrClosed) {
		t.Errorf("Next blocked across Close: %v", err)
	}
	if _, _, err := w.Next(context.Background(), Audio); !errors.Is(err, ErrClosed) {
		t.Errorf("Next after Close: %v", err)
	}
	if _, _, err := w.Next(context.Background(), Kind(7)); !errors.Is(err, ErrClosed) {
		t.Errorf("Next of an unknown kind after Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	// The session stays usable after the watcher closed.
	w2 := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	collect(t, w2, c, Audio, 3)
}

func TestWatchErrors(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	sub, _ := publisherPair(t, o)
	var re *kolomoq.RequestError
	if _, err := Watch(testCtx(t), sub, testNS, WatchOptions{}); !errors.As(err, &re) {
		t.Errorf("no broadcast: %v", err)
	}
	if _, err := Watch(testCtx(t), sub, testNS, WatchOptions{Playout: PlayoutConfig{MinDelay: -1}}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("bad playout config: %v", err)
	}
	newBroadcast(t, o)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w, err := Watch(ctx, sub, testNS, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if s := w.Tracks(); s.Audio != nil || s.Video != nil || w.Catalog().Version != 0 {
		t.Errorf("empty broadcast: %+v", s)
	}
}

// TestMalformedObjects publishes a catalog and a video track by hand, with one object that has
// no timestamp in the middle of a group: the rest of that group must not play.
func TestMalformedObjects(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	cw, err := o.CreateTrack(testNS, CatalogTrack, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cw.Close()
	vt := videoTrack()
	vw, err := o.CreateTrack(testNS, vt.Name, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer vw.Close()
	cat, err := (&Catalog{Tracks: []Track{vt}}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	g, _ := cw.NewGroup()
	if _, err := g.WriteObject(cat); err != nil {
		t.Fatal(err)
	}
	g.WriteObject([]byte("not a catalog"))
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})

	c := NewClock()
	frame := func(capture time.Time, malformed bool) ([]byte, []kolomoq.Property) {
		ts, _ := c.At(capture, 90000)
		data := make([]byte, 8)
		putCapture(data, capture.Sub(c.Start()))
		props := []kolomoq.Property{{Type: PropertyTimescale, Value: 90000}, {Type: PropertyTimestamp, Value: ts}}
		if malformed {
			props = props[:1]
		}
		return data, props
	}
	for group := range 2 {
		gw, err := vw.NewGroup()
		if err != nil {
			t.Fatal(err)
		}
		for i := range 4 {
			data, props := frame(time.Now(), group == 0 && i == 1)
			if _, err := gw.WriteObject(data, props...); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	got := collect(t, w, c, Video, 5)
	if !got[0].f.Keyframe || !got[1].f.Keyframe || got[2].f.Keyframe {
		t.Errorf("played group 0 past its malformed object: keyframes %v %v %v", got[0].f.Keyframe, got[1].f.Keyframe, got[2].f.Keyframe)
	}
	s := w.Stats()
	if s.Video.Gaps != 1 || s.Video.Malformed != 1 || s.BadCatalogs != 1 {
		t.Errorf("stats %+v", s)
	}
}

func TestGroupAssembler(t *testing.T) {
	obj := func(g, id uint64) kolomoq.Object { return kolomoq.Object{Group: g, ID: id} }
	ids := func(objs []kolomoq.Object) (out [][2]uint64) {
		for _, o := range objs {
			out = append(out, [2]uint64{o.Group, o.ID})
		}
		return out
	}
	a := groupAssembler{maxFrames: 3, maxBytes: 1 << 20}
	steps := []struct {
		in    kolomoq.Object
		ready [][2]uint64
		gaps  int
	}{
		{obj(5, 2), nil, 0},                         // joined mid-group: wait for the cache
		{obj(5, 0), [][2]uint64{{5, 0}}, 0},         // the cache fills in
		{obj(5, 1), [][2]uint64{{5, 1}, {5, 2}}, 0}, // and releases what was held
		{obj(5, 1), nil, 0},                         // duplicate
		{obj(5, 4), nil, 0},                         // object 3 is missing
		{obj(6, 0), [][2]uint64{{6, 0}}, 1},         // a newer group cuts group 5
		{obj(5, 3), nil, 0},                         // too late for a cut group
		{obj(6, 1), [][2]uint64{{6, 1}}, 0},         //
		{obj(4, 0), nil, 0},                         // older than the previous group
		{obj(8, 1), nil, 0},                         // group 7 lost, 8 lost its keyframe
		{obj(8, 2), nil, 0},                         //
		{obj(8, 3), nil, 0},                         //
		{obj(8, 4), nil, 1},                         // over the pending limit: cut
		{obj(8, 0), nil, 0},                         // cut groups stay cut
		{obj(9, 0), [][2]uint64{{9, 0}}, 0},         // the next keyframe plays
		{obj(9, 2), nil, 0},                         //
		{obj(9, 1), [][2]uint64{{9, 1}, {9, 2}}, 0}, //
	}
	for i, s := range steps {
		ready, gaps := a.add(s.in)
		if !slices.Equal(ids(ready), s.ready) || gaps != s.gaps {
			t.Fatalf("step %d: add(%v) = %v, %d gaps; want %v, %d", i, s.in.ID, ids(ready), gaps, s.ready, s.gaps)
		}
	}
	if len(a.groups) > 2 {
		t.Errorf("assembler keeps %d groups", len(a.groups))
	}
	a.cut(9)
	if ready, _ := a.add(obj(9, 3)); ready != nil {
		t.Error("cut group released an object")
	}
	a.cut(100)
	if a.frames != 0 || a.bytes != 0 {
		t.Errorf("assembler counts %d frames, %d bytes with nothing held", a.frames, a.bytes)
	}
}

func TestGroupAssemblerBytes(t *testing.T) {
	big := func(g, id uint64, n int) kolomoq.Object {
		return kolomoq.Object{Group: g, ID: id, Payload: make([]byte, n), Properties: []kolomoq.Property{{Type: 1, Bytes: make([]byte, 10)}}}
	}
	a := groupAssembler{maxFrames: 100, maxBytes: 100}
	if r, gaps := a.add(big(1, 1, 40)); r != nil || gaps != 0 || a.bytes != 50 {
		t.Fatalf("held %d bytes, %d gaps", a.bytes, gaps)
	}
	if r, gaps := a.add(big(1, 2, 40)); r != nil || gaps != 0 || a.bytes != 100 || a.frames != 2 {
		t.Fatalf("held %d bytes in %d frames, %d gaps", a.bytes, a.frames, gaps)
	}
	if r, gaps := a.add(big(1, 3, 0)); r != nil || gaps != 1 || a.bytes != 0 || a.frames != 0 {
		t.Fatalf("over the byte bound: held %d bytes in %d frames, %d gaps", a.bytes, a.frames, gaps)
	}
	if r, _ := a.add(big(2, 1, 30)); r != nil || a.bytes != 40 {
		t.Fatalf("next group held %d bytes", a.bytes)
	}
	if r, _ := a.add(big(2, 0, 200)); len(r) != 2 || a.bytes != 0 || a.frames != 0 {
		t.Fatalf("released %d objects, still held %d bytes in %d frames", len(r), a.bytes, a.frames)
	}
	if r, gaps := a.add(big(3, 1, 200)); r != nil || gaps != 1 {
		t.Fatalf("an object over the whole bound: %d gaps", gaps)
	}
}

// TestLongNetworkSession runs a broadcast for 20 s of real time and checks that nothing is lost and that
// sync holds throughout.
func TestLongNetworkSession(t *testing.T) {
	if testing.Short() {
		t.Skip("long real-time session")
	}
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
	startSource(t, addTrack(t, b, videoTrack()), c, 33*time.Millisecond, 60)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	audio, video := collectAV(t, w, c, 1000, 600)
	for i := 100; i+100 <= len(audio) && i/5*3+60 <= len(video); i += 100 {
		ma, mv := median(audio[i:i+100]), median(video[i/5*3:i/5*3+60])
		if (ma - mv).Abs() > syncTolerance {
			t.Errorf("at audio frame %d: audio %v, video %v", i, ma, mv)
		}
	}
	s := w.Stats()
	t.Logf("%+v", s)
	if s.Video.Gaps != 0 || s.Playout.Tracks["video"].NoKeyframe != 0 || s.Video.Err != nil || s.Audio.Err != nil {
		t.Errorf("stats %+v", s)
	}
}
