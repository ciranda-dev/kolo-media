package kolomedia

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	kolomoq "github.com/ciranda-dev/kolo-moq"
)

// hostile is how a relay that holds no key rewrites what it forwards. Nil fields forward as is.
type hostile struct {
	// objects returns the objects to write for o, which arrived for the downstream track.
	objects func(track string, o kolomoq.Object) []kolomoq.Object
	// priority returns the publisher priority of the downstream track.
	priority func(track string, p uint8) uint8
	// props are track properties added to every downstream track.
	props []kolomoq.Property
}

type fwdTrack struct {
	w *kolomoq.TrackWriter
	g *kolomoq.GroupWriter
}

// write publishes objs in their groups; a group before the open one is skipped, skipped group IDs
// are opened and closed empty, and object IDs are the writer's, so an extra object shifts the next.
func (f *fwdTrack) write(objs []kolomoq.Object) {
	for _, o := range objs {
		for f.g == nil || f.g.ID() < o.Group {
			g, err := f.w.NewGroup()
			if err != nil {
				return
			}
			f.g = g
		}
		if f.g.ID() == o.Group {
			f.g.WriteObject(o.Payload, o.Properties...)
		}
	}
}

// forward subscribes on up to the tracks of upNS named by the values of routes and republishes
// them on origin under downNS, each as the track named by its key, as a relay that serves its own
// origin. It returns once every track got its first object.
func forward(t *testing.T, up *kolomoq.Session, upNS kolomoq.Namespace, origin *kolomoq.Origin, downNS kolomoq.Namespace, routes map[string]string, h *hostile) {
	t.Helper()
	if h == nil {
		h = &hostile{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	for down, name := range routes {
		sub, err := up.Subscribe(testCtx(t), upNS, name, &kolomoq.SubscribeOptions{Join: kolomoq.JoinCurrentGroup})
		if err != nil {
			t.Fatal(err)
		}
		first, err := nextNormal(t, sub, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		prio := first.Priority
		if h.priority != nil {
			prio = h.priority(down, prio)
		}
		w, err := origin.CreateTrack(downNS, down, &kolomoq.TrackOptions{Priority: prio, Properties: h.props})
		if err != nil {
			t.Fatal(err)
		}
		f := &fwdTrack{w: w}
		emit := func(o kolomoq.Object) {
			if h.objects == nil {
				f.write([]kolomoq.Object{o})
			} else {
				f.write(h.objects(down, o))
			}
		}
		emit(first)
		wg.Go(func() {
			defer sub.Close()
			defer w.Close()
			for {
				o, err := sub.ReadObject(ctx)
				if err != nil {
					return
				}
				if o.Status == kolomoq.StatusNormal {
					emit(o)
				}
			}
		})
	}
}

// hostileSetup runs a's protected audio broadcast behind a hostile relay that wp lists as its
// relay, and returns wp's session to the relay.
func hostileSetup(t *testing.T, h *hostile, routes map[string]string) (a, wp *secParty, down *kolomoq.Session, clock *Clock) {
	t.Helper()
	hr := newSecNode(t)
	a, wp = newSecParty(t), newSecParty(t, hr.pub)
	if err := a.sender.SetMembers([]ed25519.PublicKey{wp.pub}); err != nil {
		t.Fatal(err)
	}
	wp.receive(t, wp.pairwise(t, a), a)
	clock = NewClock()
	startSource(t, addTrack(t, a.b, audioTrack()), clock, 20*time.Millisecond, 1)
	startSource(t, addTrack(t, a.b, videoTrack()), clock, 33*time.Millisecond, 10)
	if routes == nil {
		routes = map[string]string{CatalogTrack: CatalogTrack, "mic": "mic"}
	}
	forward(t, hr.dial(t, a.addr, a.pub, nil), a.ns, hr.origin, a.ns, routes, h)
	return a, wp, wp.dial(t, hr.addr, hr.pub, nil), clock
}

func onTrack(track string, fn func(o kolomoq.Object) []kolomoq.Object) func(string, kolomoq.Object) []kolomoq.Object {
	return func(name string, o kolomoq.Object) []kolomoq.Object {
		if name != track {
			return []kolomoq.Object{o}
		}
		return fn(o)
	}
}

// TestSecureHostileRelay checks that a relay without keys cannot make a watcher play anything it
// changed, forged, moved or sent in clear, and that it counts what it refused.
func TestSecureHostileRelay(t *testing.T) {
	audioOnly := Capabilities{Codecs: []Codec{Opus}}
	plainAudio := func() func(kolomoq.Object) []kolomoq.Object {
		pk, err := NewPackager(audioTrack(), PackagerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		i := 0
		return func(o kolomoq.Object) []kolomoq.Object {
			mu.Lock()
			defer mu.Unlock()
			i++
			obj, err := pk.Pack(audioFrame(i, nil))
			if err != nil {
				t.Error(err)
				return nil
			}
			var props []kolomoq.Property
			for _, p := range obj.Properties {
				props = append(props, kolomoq.Property{Type: p.Type, Value: p.Value, Bytes: p.Bytes})
			}
			return []kolomoq.Object{{Group: o.Group, Properties: props, Payload: obj.Payload}}
		}
	}
	refused := []struct {
		name   string
		h      *hostile
		routes map[string]string
	}{
		{"flipped payload bit", &hostile{objects: onTrack("mic", func(o kolomoq.Object) []kolomoq.Object {
			o.Payload = slices.Clone(o.Payload)
			o.Payload[len(o.Payload)/2] ^= 0x10
			return []kolomoq.Object{o}
		})}, nil},
		{"Key ID stripped", &hostile{objects: onTrack("mic", func(o kolomoq.Object) []kolomoq.Object {
			o.Properties = nil
			return []kolomoq.Object{o}
		})}, nil},
		{"Key ID replaced", &hostile{objects: onTrack("mic", func(o kolomoq.Object) []kolomoq.Object {
			o.Properties = []kolomoq.Property{{Type: 0x0B, Bytes: []byte{0x78, 0}}}
			return []kolomoq.Object{o}
		})}, nil},
		{"object ID shifted", &hostile{objects: onTrack("mic", func(o kolomoq.Object) []kolomoq.Object {
			filler := kolomoq.Object{Group: o.Group, Properties: o.Properties, Payload: make([]byte, len(o.Payload))}
			return []kolomoq.Object{filler, o}
		})}, nil},
		{"group ID shifted", &hostile{objects: onTrack("mic", func(o kolomoq.Object) []kolomoq.Object {
			o.Group++
			return []kolomoq.Object{o}
		})}, nil},
		{"priority changed", &hostile{priority: func(track string, p uint8) uint8 {
			if track == "mic" {
				return p + 1
			}
			return p
		}}, nil},
		{"video moved to the audio track", nil, map[string]string{CatalogTrack: CatalogTrack, "mic": "cam"}},
		{"forged objects", &hostile{objects: onTrack("mic", func(o kolomoq.Object) []kolomoq.Object {
			out := make([]kolomoq.Object, 50)
			for i := range out {
				out[i] = kolomoq.Object{Group: o.Group, Properties: o.Properties, Payload: make([]byte, len(o.Payload))}
				rand.Read(out[i].Payload)
			}
			return out
		})}, nil},
		{"unprotected media", &hostile{objects: onTrack("mic", plainAudio())}, nil},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			checkGoroutines(t)
			a, wp, down, _ := hostileSetup(t, tc.h, tc.routes)
			w := secWatch(t, down, a.ns, WatchOptions{Capabilities: audioOnly, Keys: wp.keys})
			// A replaced Key ID names a key the watcher does not hold (NoKey or Retired); the rest fail to open.
			waitFor(t, "refused objects", func() bool { st := w.Stats().Audio; return st.Unauthenticated+st.NoKey+st.Retired >= 20 })
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if f, _, err := w.Next(ctx, Audio); err == nil {
				t.Errorf("played a frame of %d bytes", len(f.Data))
			}
			if st := w.Stats().Audio; st.Malformed != 0 {
				t.Errorf("stats %+v", st)
			}
		})
	}

	t.Run("moved from another sender", func(t *testing.T) {
		checkGoroutines(t)
		hr := newSecNode(t)
		a, a2, wp := newSecParty(t), newSecParty(t), newSecParty(t, hr.pub)
		for _, p := range []*secParty{a, a2} {
			if err := p.sender.SetMembers([]ed25519.PublicKey{wp.pub}); err != nil {
				t.Fatal(err)
			}
			wp.receive(t, wp.pairwise(t, p), p)
			startSource(t, addTrack(t, p.b, audioTrack()), NewClock(), 20*time.Millisecond, 1)
		}
		forward(t, hr.dial(t, a.addr, a.pub, nil), a.ns, hr.origin, a.ns, map[string]string{CatalogTrack: CatalogTrack}, nil)
		forward(t, hr.dial(t, a2.addr, a2.pub, nil), a2.ns, hr.origin, a.ns, map[string]string{"mic": "mic"}, nil)
		w := secWatch(t, wp.dial(t, hr.addr, hr.pub, nil), a.ns, WatchOptions{Capabilities: audioOnly, Keys: wp.keys})
		waitFor(t, "refused objects", func() bool { return w.Stats().Audio.Unauthenticated >= 20 })
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if _, _, err := w.Next(ctx, Audio); err == nil {
			t.Error("played a frame of another sender")
		}
	})

	t.Run("visible and track LOC properties", func(t *testing.T) {
		checkGoroutines(t)
		h := &hostile{
			objects: onTrack("mic", func(o kolomoq.Object) []kolomoq.Object {
				o.Properties = []kolomoq.Property{
					{Type: PropertyTimescale, Value: 1}, o.Properties[0],
					{Type: PropertyAudioLevel, Value: 0x85}, {Type: PropertyTimestamp, Value: 7},
				}
				return []kolomoq.Object{o}
			}),
			props: []kolomoq.Property{{Type: PropertyTimescale, Value: 1}, {Type: PropertyAudioConfig, Bytes: []byte("x")}},
		}
		a, wp, down, clock := hostileSetup(t, h, nil)
		w := secWatch(t, down, a.ns, WatchOptions{Capabilities: audioOnly, Keys: wp.keys})
		for _, p := range collect(t, w, clock, Audio, 20) {
			if p.f.Timestamp < 1<<40 || p.f.AudioLevel != nil {
				t.Fatalf("a relay property reached the frame: timestamp %d, level %+v", p.f.Timestamp, p.f.AudioLevel)
			}
		}
		if st := w.Stats().Audio; st.Malformed+st.Unauthenticated != 0 {
			t.Errorf("stats %+v", st)
		}
	})

	t.Run("catalog in clear", func(t *testing.T) {
		checkGoroutines(t)
		h := &hostile{objects: onTrack(CatalogTrack, func(o kolomoq.Object) []kolomoq.Object {
			c := Catalog{Version: o.Group, Tracks: []Track{audioTrack()}}
			data, err := c.MarshalBinary()
			if err != nil {
				t.Error(err)
				return nil
			}
			return []kolomoq.Object{{Group: o.Group, Payload: data}}
		})}
		a, wp, down, _ := hostileSetup(t, h, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if w, err := Watch(ctx, down, a.ns, WatchOptions{Capabilities: audioOnly, Keys: wp.keys}); !errors.Is(err, context.DeadlineExceeded) {
			if err == nil {
				w.Close()
			}
			t.Fatalf("watch of a catalog in clear: %v", err)
		}
	})
}
