package kolomedia

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	kolomoq "github.com/ciranda-dev/kolo-moq"
)

func drain(t *testing.T, w *Watcher, kind Kind, wait time.Duration) {
	t.Helper()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		_, _, err := w.Next(ctx, kind)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
}

func TestAudioWriteFailureStartsGroup(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, &kolomoq.OriginConfig{MaxObjectSize: 1024})
	b := newBroadcast(t, o)
	tr := audioTrack()
	tr.Config = []byte{1, 2, 3}
	p, err := b.AddTrack(tr, PackagerOptions{AudioGroupDuration: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	sub, _ := publisherPair(t, o)
	ctx := testCtx(t)
	rs, err := sub.Subscribe(ctx, testNS, tr.Name, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	write := func(i, size int) error {
		return p.WriteFrame(Frame{Timestamp: uint64(i) * 960, Data: make([]byte, size)})
	}
	for i := range 3 {
		if err := write(i, 10); err != nil {
			t.Fatal(err)
		}
	}
	if err := write(3, 2000); !errors.Is(err, kolomoq.ErrObjectTooLarge) {
		t.Fatalf("oversized frame: %v", err)
	}
	for i := 3; i < 5; i++ {
		if err := write(i, 10); err != nil {
			t.Fatalf("frame %d after the failure: %v", i, err)
		}
	}
	u, err := NewUnpackager(p.Track())
	if err != nil {
		t.Fatal(err)
	}
	want := []kolomoq.Location{{Group: 0, Object: 0}, {Group: 0, Object: 1}, {Group: 0, Object: 2}, {Group: 1, Object: 0}, {Group: 1, Object: 1}}
	var got []kolomoq.Object
	for len(got) < len(want) {
		obj, err := rs.ReadObject(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if obj.Status == kolomoq.StatusNormal {
			got = append(got, obj)
		}
	}
	// Each group has its own stream, so groups arrive in any order.
	slices.SortFunc(got, func(a, b kolomoq.Object) int { return cmp.Or(cmp.Compare(a.Group, b.Group), cmp.Compare(a.ID, b.ID)) })
	for i, obj := range got {
		if loc := (kolomoq.Location{Group: obj.Group, Object: obj.ID}); loc != want[i] {
			t.Fatalf("object %d at %+v, want %+v", i, loc, want[i])
		}
		props := make([]Property, len(obj.Properties))
		for j, pr := range obj.Properties {
			props[j] = Property{Type: pr.Type, Value: pr.Value, Bytes: pr.Bytes}
		}
		f, err := u.Unpack(obj.ID, props, obj.Payload)
		if err != nil {
			t.Fatalf("object %+v: %v", want[i], err)
		}
		if ts := uint64(i) * 960; f.Timestamp != ts {
			t.Errorf("object %d timestamp %d, want %d", i, f.Timestamp, ts)
		}
	}
}

func TestSlowWatcher(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
	startSource(t, addTrack(t, b, videoTrack()), c, 33*time.Millisecond, 15)
	sub, _ := publisherPair(t, o)
	const limit = 16
	w := watch(t, sub, WatchOptions{Capabilities: avCaps(), Playout: PlayoutConfig{MaxFrames: limit}})

	// Nobody calls Next: each buffer stays within its bound.
	waitFor(t, "both buffers to overflow", func() bool {
		s := w.Stats().Playout.Tracks
		return s["audio"].Overflow > 0 && s["video"].Overflow > 0
	})
	for kind, s := range w.Stats().Playout.Tracks {
		if s.Buffered > limit {
			t.Errorf("%s buffers %d frames, limit %d", kind, s.Buffered, limit)
		}
	}

	// A late reader catches up with live and plays in sync.
	audio, video := collectAV(t, w, c, 60, 30)
	ma, mv := median(audio[30:]), median(video[15:])
	if ma > 300*time.Millisecond || (ma-mv).Abs() > syncTolerance {
		t.Errorf("after catching up: audio %v, video %v behind capture", ma, mv)
	}
	if s := w.Stats(); s.Audio.Err != nil || s.Video.Err != nil {
		t.Errorf("stats %+v", s)
	}
}

func TestPublisherStall(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	a, v := addTrack(t, b, audioTrack()), addTrack(t, b, videoTrack())
	as, vs := startSource(t, a, c, 20*time.Millisecond, 1), startSource(t, v, c, 33*time.Millisecond, 15)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	collectAV(t, w, c, 25, 15)

	// The publisher stops sending for a second: Next blocks without an error.
	as.Stop()
	vs.Stop()
	drain(t, w, Audio, 500*time.Millisecond)
	drain(t, w, Video, 100*time.Millisecond)
	time.Sleep(time.Second)

	startSource(t, a, c, 20*time.Millisecond, 1)
	startSource(t, v, c, 33*time.Millisecond, 15)
	audio, video := collectAV(t, w, c, 60, 30)
	if !video[0].f.Keyframe {
		t.Error("video resumed on a delta frame")
	}
	ma, mv := median(audio[30:]), median(video[15:])
	if ma > 300*time.Millisecond || (ma-mv).Abs() > syncTolerance {
		t.Errorf("after the stall: audio %v, video %v behind capture", ma, mv)
	}
	if s := w.Stats(); s.Audio.Err != nil || s.Video.Err != nil || s.Audio.Subscriptions != 1 || s.Video.Subscriptions != 1 {
		t.Errorf("stats %+v", s)
	}
}

func TestSessionDiesMidStream(t *testing.T) {
	checkGoroutines(t)
	for _, end := range []string{"subscriber", "relay"} {
		t.Run(end, func(t *testing.T) {
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
			sub, relaySide := dialPair(t,
				func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
					return relay.NewServer(ctx, conn)
				},
				func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
					return kolomoq.NewClient(ctx, conn, nil)
				})
			w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
			collectAV(t, w, c, 10, 5)

			errs := make(chan error, 2)
			for _, kind := range []Kind{Audio, Video} {
				go func() {
					for {
						if _, _, err := w.Next(context.Background(), kind); err != nil {
							errs <- err
							return
						}
					}
				}()
			}
			if end == "subscriber" {
				sub.Close()
			} else {
				relaySide.Close()
			}
			for range 2 {
				select {
				case err := <-errs:
					if !errors.Is(err, ErrSessionEnded) {
						t.Errorf("Next after the session died: %v", err)
					}
				case <-time.After(30 * time.Second):
					t.Fatal("Next still blocked after the session died")
				}
			}
		})
	}
}

func TestRelayPublisherDies(t *testing.T) {
	checkGoroutines(t)
	relay, err := kolomoq.NewRelay(nil)
	if err != nil {
		t.Fatal(err)
	}
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
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
	sub, _ := dialPair(t,
		func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
			return relay.NewServer(ctx, conn)
		},
		func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
			return kolomoq.NewClient(ctx, conn, nil)
		})
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	collect(t, w, c, Audio, 5)

	// The relay keeps the watcher's session; the broadcast ends with the publisher's.
	pub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		_, _, err := w.Next(ctx, Audio)
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrBroadcastEnded) {
			t.Fatalf("Next after the publisher died: %v", err)
		}
		break
	}
	select {
	case <-sub.Done():
		t.Error("the relay closed the watcher's session")
	default:
	}
}

func TestWriteFrameNeverBlocks(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	a := addTrack(t, b, audioTrack())
	sub, _ := publisherPair(t, o)
	ctx := testCtx(t)
	rs, err := sub.Subscribe(ctx, testNS, a.Track().Name, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	const limit = 64
	w := watch(t, sub, WatchOptions{Capabilities: avCaps(), Playout: PlayoutConfig{MaxFrames: limit}})

	// Neither the raw subscription nor the watcher reads: the writer never waits for them.
	start := time.Now()
	for i := range 3000 {
		if err := a.WriteFrame(Frame{Timestamp: uint64(i) * 960, Data: make([]byte, 1000)}); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("3000 writes took %v", d)
	}
	waitFor(t, "the unread subscription to drop groups", func() bool { return rs.Dropped() > 0 })
	waitFor(t, "the watcher to receive", func() bool { return w.Stats().Playout.Tracks["audio"].Received > 0 })
	if s := w.Stats().Playout.Tracks["audio"]; s.Buffered > limit {
		t.Errorf("watcher buffers %d frames, limit %d", s.Buffered, limit)
	}
}

func TestWatchCancelBeforeCatalog(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	cw, err := o.CreateTrack(testNS, CatalogTrack, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cw.Close()
	sub, _ := publisherPair(t, o)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := Watch(ctx, sub, testNS, WatchOptions{})
		errc <- err
	}()
	waitFor(t, "the catalog subscription", func() bool { return cw.Subscribers() == 1 })
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Errorf("Watch canceled while waiting for a catalog: %v", err)
	}
	waitFor(t, "the catalog subscription to end", func() bool { return cw.Subscribers() == 0 })
	if _, err := Watch(ctx, sub, testNS, WatchOptions{}); err == nil {
		t.Error("Watch with a canceled context succeeded")
	}
}

func TestBroadcastConcurrent(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	stop := make(chan struct{})
	var readers, writers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				w.Stats()
				w.Catalog()
				w.Tracks()
				b.Catalog()
			}
		})
	}
	// Video renditions come and go while audio plays on.
	for g := range 4 {
		writers.Go(func() {
			for i := range 25 {
				tr := videoTrack()
				tr.Name, tr.Width = fmt.Sprintf("cam%d", g), uint32(320*(g+1))
				p, err := b.AddTrack(tr, PackagerOptions{})
				if err != nil {
					t.Error(err)
					return
				}
				for j := range 5 {
					if err := p.WriteFrame(Frame{Timestamp: uint64(j) * 3000, Keyframe: j == 0, Data: []byte{byte(j)}}); err != nil && !isClosed(err) {
						t.Error(err)
						return
					}
				}
				if i%2 == 0 {
					err = p.Close()
				} else {
					err = b.RemoveTrack(tr.Name)
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	writers.Wait()
	close(stop)
	readers.Wait()
	waitFor(t, "the last catalog", func() bool { return w.Stats().Catalog == 201 })
	collect(t, w, c, Audio, 10)
	if s := w.Stats(); s.Audio.Subscriptions != 1 || s.Audio.Err != nil {
		t.Errorf("after the churn: %+v", s)
	}
	closed := time.Now()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		_, _, err := w.Next(ctx, Audio)
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrBroadcastEnded) {
			t.Fatalf("Next after the broadcast closed: %v", err)
		}
		break
	}
	// The catalog's End of Track object ends the watch; it does not wait for the request timeout.
	if d := time.Since(closed); d > 5*time.Second {
		t.Errorf("the watcher saw the end %v after Close", d)
	}
}

// TestHostileCatalogs publishes a catalog track by hand with malformed, mismatched and stale
// catalogs, then a burst of versions that switch tracks: the watcher ends on the last version.
func TestHostileCatalogs(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	cw, err := o.CreateTrack(testNS, CatalogTrack, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cw.Close()
	at, vt := audioTrack(), videoTrack()
	for _, name := range []string{at.Name, vt.Name} {
		w, err := o.CreateTrack(testNS, name, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
	}
	publish := func(objects ...[]byte) {
		t.Helper()
		g, err := cw.NewGroup()
		if err != nil {
			t.Fatal(err)
		}
		for _, obj := range objects {
			if _, err := g.WriteObject(obj); err != nil {
				t.Fatal(err)
			}
		}
	}
	encode := func(version uint64, tracks ...Track) []byte {
		t.Helper()
		data, err := (&Catalog{Version: version, Tracks: tracks}).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	publish(encode(0, at))
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})

	publish(encode(7, at, vt))                // version is not the group ID
	publish([]byte{1, 2, 3})                  // not a catalog
	publish(encode(3, at), encode(3, at, vt)) // a second object in the group
	publish(make([]byte, 0))                  // empty
	waitFor(t, "the bad catalogs", func() bool { return w.Stats().BadCatalogs == 4 })
	if s := w.Tracks(); w.Catalog().Version != 3 || s.Video != nil {
		t.Fatalf("catalog %d, video %+v", w.Catalog().Version, s.Video)
	}

	var last uint64
	for i := range 100 {
		tracks := []Track{at}
		if i%2 == 1 {
			tracks = append(tracks, vt)
		}
		last = uint64(5 + i)
		publish(encode(last, tracks...))
	}
	waitFor(t, "the last catalog", func() bool { return w.Catalog().Version == last })
	if s := w.Tracks(); s.Audio == nil || s.Video == nil || s.Video.Name != vt.Name {
		t.Errorf("after the burst: %+v", s)
	}
	if s := w.Stats(); s.Audio.Subscriptions != 1 || s.BadCatalogs != 4 {
		t.Errorf("stats %+v", s)
	}
}

// relayChain connects a publisher serving o to a relay and returns a subscriber session of the relay
// and the publisher's own session.
func relayChain(t *testing.T, relay *kolomoq.Relay, o *kolomoq.Origin) (sub, pub *kolomoq.Session) {
	t.Helper()
	pub, _ = dialPair(t,
		func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
			return relay.NewServer(ctx, conn)
		},
		func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
			return kolomoq.NewClient(ctx, conn, &kolomoq.Config{Origin: o})
		})
	if _, err := pub.PublishNamespace(testCtx(t), testNS); err != nil {
		t.Fatal(err)
	}
	sub, _ = dialPair(t,
		func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
			return relay.NewServer(ctx, conn)
		},
		func(ctx context.Context, conn kolomoq.Conn) (*kolomoq.Session, error) {
			return kolomoq.NewClient(ctx, conn, nil)
		})
	return sub, pub
}

// TestRelayStatusErrors checks the errors a watcher reports when a relay ends a subscription with a
// status instead of a clean end of track.
func TestRelayStatusErrors(t *testing.T) {
	checkGoroutines(t)
	// Draft-22 section 12.4: a relay sends GOING_AWAY only when a GOAWAY was sent or received on the
	// publisher's session; a session that ends without one gives INTERNAL_ERROR.
	for _, tc := range []struct {
		name   string
		goAway bool
		code   kolomoq.DoneCode
	}{
		{"publisher session ends without GOAWAY", false, kolomoq.DoneInternalError},
		{"publisher session ends after GOAWAY", true, kolomoq.DoneGoingAway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			relay, err := kolomoq.NewRelay(nil)
			if err != nil {
				t.Fatal(err)
			}
			o := newOrigin(t, nil)
			b := newBroadcast(t, o)
			c := NewClock()
			startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
			sub, pub := relayChain(t, relay, o)
			w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
			collect(t, w, c, Audio, 3)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if tc.goAway {
				if err := pub.GoAway(ctx, 100*time.Millisecond); err != nil {
					t.Fatal(err)
				}
			}
			pub.Close()
			var err2 error
			for err2 == nil {
				_, _, err2 = w.Next(ctx, Audio)
			}
			var pd *kolomoq.PublishDoneError
			if !errors.Is(err2, ErrBroadcastEnded) || !errors.As(err2, &pd) || pd.Code != tc.code {
				t.Fatalf("Next after the publisher's session ended: %v, want %v", err2, tc.code)
			}
		})
	}
	t.Run("track fails behind the relay", func(t *testing.T) {
		relay, err := kolomoq.NewRelay(&kolomoq.RelayConfig{Session: kolomoq.Config{MaxObjectSize: 1 << 10}})
		if err != nil {
			t.Fatal(err)
		}
		o := newOrigin(t, nil)
		b := newBroadcast(t, o)
		a := addTrack(t, b, audioTrack())
		sub, _ := relayChain(t, relay, o)
		w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
		// The relay refuses the object, so it ends the audio track with INTERNAL_ERROR; the catalog goes on.
		for i := 0; w.Stats().Audio.Err == nil; i++ {
			if i > 2000 {
				t.Fatal("the audio track did not end")
			}
			if err := a.WriteFrame(Frame{Timestamp: uint64(i+1) * 960, Data: make([]byte, 4<<10)}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(5 * time.Millisecond)
		}
		err = w.Stats().Audio.Err
		var pd *kolomoq.PublishDoneError
		if !errors.Is(err, ErrTrackEnded) || !errors.As(err, &pd) || pd.Code != kolomoq.DoneInternalError {
			t.Fatalf("audio error %v", err)
		}
		select {
		case <-sub.Done():
			t.Error("the relay closed the watcher's session")
		default:
		}
	})
}

// worstProps are the properties of a group's first object when each takes its longest encoding.
func worstProps(config []byte) []kolomoq.Property {
	return []kolomoq.Property{
		{Type: PropertyTimescale, Value: math.MaxUint64},
		{Type: PropertyAudioLevel, Value: 0xFF},
		{Type: PropertyAudioConfig, Bytes: config},
		{Type: PropertyTimestamp, Value: math.MaxUint64},
	}
}

func TestMaxConfigFitsOneObject(t *testing.T) {
	checkGoroutines(t)
	limit := CatalogLimits{}.withDefaults().MaxConfigLen

	t.Run("limit is exact", func(t *testing.T) {
		o := newOrigin(t, nil)
		w, err := o.CreateTrack(testNS, "raw", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		g, err := w.NewGroup()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.WriteObject([]byte{1}, worstProps(make([]byte, limit))...); err != nil {
			t.Errorf("config of %d bytes: %v", limit, err)
		}
		if _, err := g.WriteObject([]byte{1}, worstProps(make([]byte, limit+1))...); !errors.Is(err, kolomoq.ErrInvalidProperty) {
			t.Errorf("config of %d bytes: %v", limit+1, err)
		}
	})

	for _, tr := range []Track{audioTrack(), videoTrack()} {
		t.Run("publish "+tr.Kind.String(), func(t *testing.T) {
			tr.Timescale = math.MaxUint64
			tr.Config = make([]byte, limit)
			b := newBroadcast(t, newOrigin(t, nil))
			p := addTrack(t, b, tr)
			f := Frame{Timestamp: math.MaxUint64, Keyframe: true, Data: []byte{1}}
			if tr.Kind == Audio {
				f.AudioLevel = &AudioLevel{Voice: true, Level: 1}
			}
			if err := p.WriteFrame(f); err != nil {
				t.Fatalf("WriteFrame with a config of %d bytes: %v", limit, err)
			}
			tr.Name, tr.Config = "more", make([]byte, limit+1)
			if _, err := b.AddTrack(tr, PackagerOptions{}); !errors.Is(err, ErrInvalidTrack) {
				t.Errorf("AddTrack with a config of %d bytes: %v", limit+1, err)
			}
		})
	}

	t.Run("watch", func(t *testing.T) {
		o := newOrigin(t, nil)
		b := newBroadcast(t, o)
		c := NewClock()
		at, vt := audioTrack(), videoTrack()
		at.Config, vt.Config = bytes.Repeat([]byte{0xA5}, limit), bytes.Repeat([]byte{0x5A}, limit)
		startSource(t, addTrack(t, b, at), c, 20*time.Millisecond, 1)
		startSource(t, addTrack(t, b, vt), c, 33*time.Millisecond, 15)
		sub, _ := publisherPair(t, o)
		w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
		audio, video := collectAV(t, w, c, 5, 5)
		if len(audio) != 5 || len(video) != 5 {
			t.Fatalf("played %d audio and %d video frames", len(audio), len(video))
		}
		if !bytes.Equal(audio[0].track.Config, at.Config) || !bytes.Equal(video[0].track.Config, vt.Config) {
			t.Error("the played tracks lost their config")
		}
		if s := w.Stats(); s.Audio.Malformed != 0 || s.Video.Malformed != 0 || s.BadCatalogs != 0 {
			t.Errorf("stats %+v", s)
		}
	})
}

func TestCatalogOverObjectSize(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, &kolomoq.OriginConfig{MaxObjectSize: 1 << 10})
	b := newBroadcast(t, o)
	at := audioTrack()
	at.Config = make([]byte, 300)
	addTrack(t, b, at)
	vt := videoTrack()
	vt.Config = make([]byte, 900)
	_, err := b.AddTrack(vt, PackagerOptions{})
	if !errors.Is(err, ErrInvalidCatalog) || !errors.Is(err, kolomoq.ErrObjectTooLarge) {
		t.Fatalf("AddTrack past the object size: %v", err)
	}
	if c := b.Catalog(); len(c.Tracks) != 1 || c.Tracks[0].Name != at.Name {
		t.Fatalf("catalog after the failure: %+v", c)
	}

	// A subscriber joining now finds a catalog in the current group, not an empty one.
	sub, _ := publisherPair(t, o)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w, err := Watch(ctx, sub, testNS, WatchOptions{Capabilities: avCaps()})
	if err != nil {
		t.Fatalf("Watch after the failed AddTrack: %v", err)
	}
	defer w.Close()
	if c := w.Catalog(); c.Version != b.Catalog().Version || len(c.Tracks) != 1 {
		t.Errorf("watched catalog %+v, broadcast catalog %+v", c, b.Catalog())
	}
	// The broadcast goes on.
	vt.Config = nil
	if _, err := b.AddTrack(vt, PackagerOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the new catalog", func() bool { return w.Catalog().Version == b.Catalog().Version && len(w.Catalog().Tracks) == 2 })
}

// TestCatalogFailureAtVersionBoundary fails an AddTrack whose catalog is over the object size when the
// catalog in use is exactly at it and the next version needs one more byte (127 to 128): the failed
// AddTrack must not leave an empty catalog group where a late joiner would land.
func TestCatalogFailureAtVersionBoundary(t *testing.T) {
	checkGoroutines(t)
	const max = 1 << 10
	o := newOrigin(t, &kolomoq.OriginConfig{MaxObjectSize: max})
	b := newBroadcast(t, o)
	for b.Catalog().Version < 126 {
		addTrack(t, b, lowVideo()).Close()
	}
	// The next AddTrack publishes version 127, one byte; its catalog is exactly max bytes.
	tr := audioTrack()
	enc, err := (&Catalog{Version: 127, Tracks: []Track{tr}}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	tr.Config = make([]byte, max-len(enc)-1) // the config length grows from 1 to 2 bytes
	addTrack(t, b, tr)
	if v := b.Catalog().Version; v != 127 {
		t.Fatalf("version %d, want 127", v)
	}
	if enc, _ := b.Catalog().MarshalBinary(); len(enc) != max {
		t.Fatalf("catalog of %d bytes, want %d", len(enc), max)
	}
	if _, err := b.AddTrack(lowVideo(), PackagerOptions{}); !errors.Is(err, ErrInvalidCatalog) || !errors.Is(err, kolomoq.ErrObjectTooLarge) {
		t.Fatalf("AddTrack past the object size: %v", err)
	}
	if v := b.Catalog().Version; v != 127 {
		t.Errorf("version %d after the failure, want 127", v)
	}
	sub, _ := publisherPair(t, o)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w, err := Watch(ctx, sub, testNS, WatchOptions{Capabilities: avCaps()})
	if err != nil {
		t.Fatalf("Watch after the failed AddTrack: %v", err)
	}
	defer w.Close()
	if c := w.Catalog(); c.Version != 127 || len(c.Tracks) != 1 {
		t.Errorf("watched catalog %+v", c)
	}
	// Removing the track still works and the version goes on.
	if err := b.RemoveTrack(tr.Name); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the catalog after the removal", func() bool { return w.Catalog().Version == b.Catalog().Version && len(w.Catalog().Tracks) == 0 })
}

// TestMoqErrorsHaveKolomediaPrefix checks that every error of kolo-moq that kolo-media returns starts
// with kolomedia:, keeps its cause for errors.Is, and has a kolomedia sentinel where a caller needs one.
func TestMoqErrorsHaveKolomediaPrefix(t *testing.T) {
	checkGoroutines(t)
	check := func(what string, err error, want error, cause error) {
		t.Helper()
		if err == nil || !strings.HasPrefix(err.Error(), "kolomedia: ") {
			t.Errorf("%s: %v", what, err)
			return
		}
		if want != nil && !errors.Is(err, want) {
			t.Errorf("%s: %v does not wrap %v", what, err, want)
		}
		if cause != nil && !errors.Is(err, cause) {
			t.Errorf("%s: %v does not wrap %v", what, err, cause)
		}
	}
	o := newOrigin(t, &kolomoq.OriginConfig{MaxObjectSize: 1 << 10})
	b := newBroadcast(t, o)
	p := addTrack(t, b, audioTrack())
	err := p.WriteFrame(Frame{Timestamp: 1, Data: make([]byte, 2<<10)})
	check("WriteFrame over MaxObjectSize", err, ErrFrameTooLarge, kolomoq.ErrObjectTooLarge)
	_, err = NewBroadcast(o, testNS)
	check("NewBroadcast twice", err, ErrBroadcastExists, kolomoq.ErrTrackExists)

	sub, _ := publisherPair(t, o)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = Watch(ctx, sub, kolomoq.Namespace{"no", "such"}, WatchOptions{Capabilities: avCaps()})
	var re *kolomoq.RequestError
	check("Watch of a missing broadcast", err, ErrBroadcastNotFound, nil)
	if !errors.As(err, &re) || re.Code != kolomoq.ReqDoesNotExist {
		t.Errorf("Watch of a missing broadcast: %v does not wrap the request error", err)
	}

	small := newOrigin(t, &kolomoq.OriginConfig{MaxTracks: 2})
	sb := newBroadcast(t, small)
	addTrack(t, sb, audioTrack())
	_, err = sb.AddTrack(lowVideo(), PackagerOptions{})
	check("AddTrack past the origin's MaxTracks", err, ErrTooManyTracks, kolomoq.ErrTooManyTracks)
	if _, err = NewBroadcast(small, kolomoq.Namespace{"other"}); !errors.Is(err, ErrTooManyTracks) {
		t.Errorf("NewBroadcast past the origin's MaxTracks: %v", err)
	}

	p.Close()
	b.Close()
	check("WriteFrame after the broadcast closed", p.WriteFrame(Frame{Timestamp: 2, Data: []byte{1}}), ErrClosed, nil)
}

// TestOriginTrackLimit checks that the catalog's 1024 tracks are not all usable on a default origin: the
// origin's MaxTracks (1000) also counts the catalog track.
func TestOriginTrackLimit(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	var err error
	n := 0
	for ; err == nil && n < 1024; n++ {
		tr := audioTrack()
		tr.Name = fmt.Sprintf("a%04d", n)
		_, err = b.AddTrack(tr, PackagerOptions{})
	}
	if !errors.Is(err, ErrTooManyTracks) || !errors.Is(err, kolomoq.ErrTooManyTracks) {
		t.Fatalf("AddTrack %d: %v", n, err)
	}
	if got := len(b.Catalog().Tracks); got != 999 {
		t.Errorf("%d media tracks, want 999", got)
	}
}

func TestHugeDelays(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	b := newBroadcast(t, o)
	c := NewClock()
	startSource(t, addTrack(t, b, audioTrack()), c, 20*time.Millisecond, 1)
	sub, _ := publisherPair(t, o)
	w := watch(t, sub, WatchOptions{
		Capabilities: avCaps(),
		Playout:      PlayoutConfig{MaxDelay: math.MaxInt64, MaxBehind: math.MaxInt64},
	})
	if s := w.Stats(); s.Audio.Err != nil || s.Audio.Subscriptions != 1 {
		t.Fatalf("stats %+v", s)
	}
	collect(t, w, c, Audio, 5)
}

func TestWatchReportsFailedSubscriptions(t *testing.T) {
	checkGoroutines(t)
	o := newOrigin(t, nil)
	cw, err := o.CreateTrack(testNS, CatalogTrack, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cw.Close()
	at, vt := audioTrack(), videoTrack()
	aw, err := o.CreateTrack(testNS, at.Name, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer aw.Close()
	publish := func(tracks ...Track) {
		t.Helper()
		g, err := cw.NewGroup()
		if err != nil {
			t.Fatal(err)
		}
		data, err := (&Catalog{Version: g.ID(), Tracks: tracks}).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.WriteObject(data); err != nil {
			t.Fatal(err)
		}
	}
	sub, _ := publisherPair(t, o)

	// The catalog lists a video track the origin does not have: nothing can start.
	publish(vt)
	var re *kolomoq.RequestError
	if _, err := Watch(testCtx(t), sub, testNS, WatchOptions{Capabilities: avCaps()}); !errors.As(err, &re) || errors.Unwrap(err) == nil {
		t.Fatalf("Watch with a track that cannot be subscribed: %v", err)
	}

	// With the audio track present, the video failure shows in Stats and audio plays on.
	publish(at, vt)
	w := watch(t, sub, WatchOptions{Capabilities: avCaps()})
	if s := w.Stats(); !errors.As(s.Video.Err, &re) || s.Audio.Err != nil || s.Audio.Subscriptions != 1 {
		t.Errorf("stats %+v", s)
	}
}

// FuzzGroupAssembler feeds arbitrary object sequences to the assembler and checks that it releases
// each group's objects once, in order from 0, and holds no more than its bounds.
func FuzzGroupAssembler(f *testing.F) {
	f.Add([]byte{1, 0, 10, 1, 2, 10, 1, 1, 10, 2, 0, 5})
	f.Add([]byte{3, 5, 200, 3, 6, 200, 0, 1, 1, 255, 0, 0})
	f.Fuzz(func(t *testing.T, ops []byte) {
		const maxFrames, maxBytes = 8, 600
		a := groupAssembler{maxFrames: maxFrames, maxBytes: maxBytes}
		next := map[uint64]uint64{}
		var group uint64 = 10
		for len(ops) >= 3 {
			op, id, size := ops[0], uint64(ops[1]%16), int(ops[2])
			ops = ops[3:]
			if op == 255 {
				a.cut(group)
				continue
			}
			group = group + uint64(op%4) - 1
			obj := kolomoq.Object{Group: group, ID: id, Payload: make([]byte, size)}
			ready, gaps := a.add(obj)
			if gaps < 0 {
				t.Fatalf("%d gaps", gaps)
			}
			for _, o := range ready {
				if o.ID != next[o.Group] {
					t.Fatalf("released %d/%d, want object %d", o.Group, o.ID, next[o.Group])
				}
				next[o.Group]++
			}
			frames, bytes := 0, 0
			for _, g := range a.groups {
				frames += len(g.pending)
				for _, o := range g.pending {
					bytes += len(o.Payload)
				}
				if g.cut && len(g.pending) > 0 {
					t.Fatal("a cut group holds objects")
				}
			}
			if frames != a.frames || bytes != a.bytes || frames > maxFrames || bytes > maxBytes {
				t.Fatalf("holds %d frames, %d bytes; counts %d, %d", frames, bytes, a.frames, a.bytes)
			}
			if len(a.groups) > 2 {
				t.Fatalf("keeps %d groups", len(a.groups))
			}
		}
	})
}

func TestBroadcastOriginErrors(t *testing.T) {
	o := newOrigin(t, &kolomoq.OriginConfig{MaxTracks: 2})
	b := newBroadcast(t, o)
	if _, err := NewBroadcast(o, testNS); err == nil {
		t.Error("a second broadcast under the same namespace")
	}
	addTrack(t, b, audioTrack())
	if _, err := b.AddTrack(videoTrack(), PackagerOptions{}); err == nil {
		t.Fatal("AddTrack past the origin's track limit")
	}
	if c := b.Catalog(); c.Version != 1 || len(c.Tracks) != 1 {
		t.Errorf("a failed AddTrack changed the catalog: %+v", c)
	}
}
