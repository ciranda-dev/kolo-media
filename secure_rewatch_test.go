package kolomedia

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	kolomoq "github.com/ciranda-dev/kolo-moq"
)

// TestSecureRewatch checks that a viewer that watches again with the same Keyring, on the same
// session, after a relay reconnect or twice at once, reads the current catalog at once and plays.
func TestSecureRewatch(t *testing.T) {
	checkGoroutines(t)
	rm := newSecRelay(t)
	a, b := newSecParty(t, rm.pub), newSecParty(t, rm.pub)
	a.join(t, rm)
	b.join(t, rm)
	if err := a.sender.SetMembers([]ed25519.PublicKey{b.pub}); err != nil {
		t.Fatal(err)
	}
	b.receive(t, b.pairwise(t, a), a)
	clock := NewClock()
	startSource(t, addTrack(t, a.b, audioTrack()), clock, 20*time.Millisecond, 1)
	opts := WatchOptions{Capabilities: avCaps(), Keys: b.keys}
	first := secWatch(t, b.relay, a.ns, opts)
	collect(t, first, clock, Audio, 10)
	first.Close()

	rewatch := func(name string, n int) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s := b.relay
		if name == "after a reconnect" {
			s = b.dial(t, rm.addr, rm.pub, b.origin)
		}
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() {
				w, err := Watch(ctx, s, a.ns, opts)
				if err != nil {
					t.Errorf("%s: %v", name, err)
					return
				}
				defer w.Close()
				collect(t, w, clock, Audio, 10)
				if st := w.Stats(); st.Audio.Unauthenticated != 0 || st.BadCatalogs != 0 {
					t.Errorf("%s: stats %+v", name, st)
				}
			})
		}
		wg.Wait()
	}
	rewatch("same session", 1)
	rewatch("after a reconnect", 1)
	rewatch("two at once", 2)
}

// TestSecureJoinerIdleSender checks that a member who joins a broadcast whose sender writes no frame
// reads the catalog at once: a key change publishes the catalog again without waiting for a frame.
func TestSecureJoinerIdleSender(t *testing.T) {
	checkGoroutines(t)
	a, j := newSecParty(t), newSecParty(t)
	addTrack(t, a.b, audioTrack())
	if err := a.sender.SetMembers([]ed25519.PublicKey{j.pub}); err != nil {
		t.Fatal(err)
	}
	s := j.pairwise(t, a)
	j.receive(t, s, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w, err := Watch(ctx, s, a.ns, WatchOptions{Capabilities: avCaps(), Keys: j.keys})
	if err != nil {
		t.Fatalf("joiner of an idle sender: %v", err)
	}
	defer w.Close()
	if c := w.Catalog(); len(c.Tracks) != 1 || c.Tracks[0].Name != "mic" {
		t.Errorf("catalog %+v", c)
	}
}

// TestSecureCatalogRepublishFailure checks that a frame written while the catalog cannot be
// published again under a new key still reports success, and that the next frame publishes it once
// that works again, so a joiner reads it. The key change follower is stopped first, so only the
// retry at the frame can publish it.
func TestSecureCatalogRepublishFailure(t *testing.T) {
	checkGoroutines(t)
	a, j := newSecParty(t), newSecParty(t)
	mic := addTrack(t, a.b, audioTrack())
	a.b.mu.Lock()
	close(a.b.done)
	a.b.mu.Unlock()
	a.b.wg.Wait()
	a.b.done = make(chan struct{})
	broken, err := a.origin.CreateTrack(a.ns, "broken", nil)
	if err != nil {
		t.Fatal(err)
	}
	broken.Close()
	a.b.mu.Lock()
	catalog := a.b.catalog
	a.b.catalog = broken
	a.b.mu.Unlock()
	if err := a.sender.SetMembers([]ed25519.PublicKey{j.pub}); err != nil {
		t.Fatal(err)
	}
	if err := mic.WriteFrame(audioFrame(0, nil)); err != nil {
		t.Errorf("a written frame reported the catalog failure: %v", err)
	}
	a.b.mu.Lock()
	a.b.catalog = catalog
	a.b.mu.Unlock()
	if err := mic.WriteFrame(audioFrame(1, nil)); err != nil {
		t.Fatal(err)
	}
	s := j.pairwise(t, a)
	j.receive(t, s, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w, err := Watch(ctx, s, a.ns, WatchOptions{Capabilities: avCaps(), Keys: j.keys})
	if err != nil {
		t.Fatalf("joiner after a failed republish: %v", err)
	}
	w.Close()
}

// TestSecureStatusesWaitForHeldObjects checks that object statuses and the end of a subscription
// do not overtake objects waiting for their key: they come after them, in order, once the key
// arrives, and without a key the end comes once the held objects expire.
func TestSecureStatusesWaitForHeldObjects(t *testing.T) {
	for _, keyArrives := range []bool{true, false} {
		t.Run(map[bool]string{true: "key arrives", false: "no key"}[keyArrives], func(t *testing.T) {
			checkGoroutines(t)
			a, wp := newSecParty(t), newSecParty(t)
			if err := a.sender.SetMembers([]ed25519.PublicKey{wp.pub}); err != nil {
				t.Fatal(err)
			}
			s := wp.pairwise(t, a)
			stop := wp.receive(t, s, a)
			stop()
			if err := a.sender.Rotate(); err != nil {
				t.Fatal(err)
			}
			tw, err := a.origin.CreateTrack(a.ns, "data", &kolomoq.TrackOptions{Priority: 90})
			if err != nil {
				t.Fatal(err)
			}
			sub := rawSub(t, s, a.ns, "data", kolomoq.JoinNextObject)
			waitFor(t, "subscriber", func() bool { return tw.Subscribers() > 0 })
			op, err := wp.keys.Opener(s, a.ns, "data")
			if err != nil {
				t.Fatal(err)
			}
			var drops int
			o := &opener{keys: wp.keys, op: op, maxAge: time.Second, maxFrames: 16, maxBytes: 1 << 20, drop: func(error) { drops++ }}
			for _, payload := range []string{"one", "two"} {
				g, err := tw.NewGroup()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := a.sender.Write(g, "data", []byte(payload)); err != nil {
					t.Fatal(err)
				}
			}
			tw.Close()

			type got struct {
				obj kolomoq.Object
				err error
			}
			out := make(chan got, 16)
			go func() {
				for {
					obj, _, err := o.next(testCtx(t), sub)
					out <- got{obj, err}
					if err != nil {
						return
					}
				}
			}()
			select {
			case g := <-out:
				t.Fatalf("delivered before the key: status %d %q %v", g.obj.Status, g.obj.Payload, g.err)
			case <-time.After(300 * time.Millisecond):
			}
			if keyArrives {
				wp.receive(t, s, a)
			}
			var seq []string
			for g := range out {
				if g.err != nil {
					break
				}
				if g.obj.Status == kolomoq.StatusNormal {
					seq = append(seq, string(g.obj.Payload))
				} else {
					seq = append(seq, fmt.Sprintf("status %d", g.obj.Status))
				}
			}
			// The two groups travel on two streams; within each, the status follows the object.
			endGroup, endTrack := fmt.Sprintf("status %d", kolomoq.StatusEndOfGroup), fmt.Sprintf("status %d", kolomoq.StatusEndOfTrack)
			before := func(x, y string) bool {
				i, j := slices.Index(seq, x), slices.Index(seq, y)
				return i >= 0 && i < j
			}
			ok := len(seq) == 4 && before("one", endGroup) && before("two", endTrack) && drops == 0
			if !keyArrives {
				ok = len(seq) == 2 && slices.Contains(seq, endGroup) && slices.Contains(seq, endTrack) && drops == 2
			}
			if !ok {
				t.Errorf("delivered %q with %d drops", seq, drops)
			}
		})
	}
}

// readPackets reads a testdata file of Opus packets, each a 2-byte big-endian length and its bytes.
// The files hold 2 s of speech-like bursts, then silence, encoded by kolo-audio's opus package at
// 32 kbit/s, mono, 20 ms, with Config.CBR set and not set.
func readPackets(t *testing.T, name string) [][]byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	for len(b) >= 2 {
		n := int(binary.BigEndian.Uint16(b))
		if len(b) < 2+n {
			t.Fatalf("%s: truncated packet", name)
		}
		out = append(out, b[2:2+n])
		b = b[2+n:]
	}
	return out
}

// TestSecureCBROpusSizes runs real Opus packets through the protected path: constant bitrate
// packets give ciphertexts of one size through speech and silence, variable bitrate ones do not.
func TestSecureCBROpusSizes(t *testing.T) {
	checkGoroutines(t)
	a, w := newSecParty(t), newSecParty(t)
	if err := a.sender.SetMembers([]ed25519.PublicKey{w.pub}); err != nil {
		t.Fatal(err)
	}
	s := w.pairwise(t, a)
	w.receive(t, s, a)
	mic := addTrack(t, a.b, audioTrack())
	sub := rawSub(t, s, a.ns, "mic", kolomoq.JoinNextObject)
	waitFor(t, "subscriber", func() bool { return mic.writer.Subscribers() > 0 })
	op, err := w.keys.Opener(s, a.ns, "mic")
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for _, name := range []string{"opus-cbr-32k-20ms.bin", "opus-vbr-32k-20ms.bin"} {
		packets := readPackets(t, name)
		sizes := map[int]bool{}
		for j, pkt := range packets {
			// Speech for the first second, silence after: the levels a capture would report.
			level := &AudioLevel{Voice: true, Level: 30}
			if j >= len(packets)/2 {
				level = &AudioLevel{Level: 127}
			}
			f := Frame{Timestamp: 1_000_000_000 + uint64(i)*960, Keyframe: true, Data: pkt, AudioLevel: level}
			i++
			if err := mic.WriteFrame(f); err != nil {
				t.Fatal(err)
			}
			o, err := nextNormal(t, sub, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			pt, _, err := op.Open(o)
			if err != nil || !bytes.Equal(pt, pkt) {
				t.Fatalf("%s: opened %d bytes, %v", name, len(pt), err)
			}
			sizes[len(o.Payload)] = true
		}
		if cbr := name == "opus-cbr-32k-20ms.bin"; cbr != (len(sizes) == 1) {
			t.Errorf("%s: %d packets gave %d ciphertext sizes", name, len(packets), len(sizes))
		}
	}
}

// TestSecureHeldStatusBytes checks that held statuses count no bytes, even when a relay attaches
// properties to them, so they never push held objects out.
func TestSecureHeldStatusBytes(t *testing.T) {
	var drops int
	o := &opener{maxAge: time.Second, maxFrames: 8, maxBytes: 100, drop: func(error) { drops++ }}
	o.hold(kolomoq.Object{Group: 1, Payload: make([]byte, 10)}, time.Now(), nil)
	for _, n := range []int{95, 1000} {
		o.hold(kolomoq.Object{Group: 1, ID: 1, Status: kolomoq.StatusEndOfGroup, Properties: []kolomoq.Property{{Type: 0x21, Bytes: make([]byte, n)}}}, time.Now(), nil)
	}
	if drops != 0 || len(o.held) != 3 || o.bytes != 10 {
		t.Fatalf("drops %d, held %d, bytes %d", drops, len(o.held), o.bytes)
	}
}

// TestSecureHeldStatusesKeepObjects checks that statuses held behind an object never push it out:
// beyond MaxFrames held statuses, the oldest status is dropped instead.
func TestSecureHeldStatusesKeepObjects(t *testing.T) {
	var drops int
	o := &opener{maxAge: time.Second, maxFrames: 2, maxBytes: 100, drop: func(error) { drops++ }}
	o.hold(kolomoq.Object{Group: 8, Payload: make([]byte, 10)}, time.Now(), nil)
	for i := range 5 {
		o.hold(kolomoq.Object{Group: 8, ID: uint64(1 + i), Status: kolomoq.StatusEndOfGroup}, time.Now(), nil)
	}
	if drops != 0 || len(o.held) != 3 || o.held[0].obj.Status != kolomoq.StatusNormal || o.held[2].obj.ID != 5 {
		t.Fatalf("drops %d, held %+v", drops, o.held)
	}
	o.hold(kolomoq.Object{Group: 9, Payload: make([]byte, 10)}, time.Now(), nil)
	if drops != 0 || len(o.held) != 4 {
		t.Fatalf("a second object within MaxFrames: drops %d, held %d", drops, len(o.held))
	}
	o.hold(kolomoq.Object{Group: 10, Payload: make([]byte, 10)}, time.Now(), nil)
	// The oldest object goes, and the statuses behind it are released in order.
	if drops != 1 || len(o.held) != 2 || o.held[0].obj.Group != 9 || len(o.ready) != 2 || o.ready[1].obj.ID != 5 {
		t.Fatalf("drops %d, held %+v, ready %+v", drops, o.held, o.ready)
	}
}

// TestWatchRestartKeepsPlayout checks that a subscription restarted on the same track, which gets
// the current group again from its keyframe, plays none of the frames played before the restart.
func TestWatchRestartKeepsPlayout(t *testing.T) {
	for _, protected := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "protected"}[protected], func(t *testing.T) {
			checkGoroutines(t)
			a, v := newSecParty(t), newSecParty(t)
			b, opts := a.b, WatchOptions{Capabilities: avCaps()}
			if protected {
				if err := a.sender.SetMembers([]ed25519.PublicKey{v.pub}); err != nil {
					t.Fatal(err)
				}
				opts.Keys = v.keys
			} else {
				var err error
				if b, err = NewBroadcast(a.origin, kolomoq.Namespace{"plain"}); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { b.Close() })
			}
			s := v.pairwise(t, a)
			if protected {
				v.receive(t, s, a)
			}
			clock := NewClock()
			// One keyframe every 60 frames, so the redelivered group holds frames already played.
			startSource(t, addTrack(t, b, videoTrack()), clock, 33*time.Millisecond, 60)
			ns := a.ns
			if !protected {
				ns = kolomoq.Namespace{"plain"}
			}
			w := secWatch(t, s, ns, opts)
			before := collect(t, w, clock, Video, 10)
			w.applying.Lock()
			wt := w.kinds[1]
			err := w.switchTo(w.ctx, 1, &wt.track, w.catalog.Version)
			w.applying.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			last := before[len(before)-1].f.Timestamp
			for _, p := range collect(t, w, clock, Video, 10) {
				if p.f.Timestamp <= last {
					t.Fatalf("frame %d played again after the restart; the last one before was %d", p.f.Timestamp, last)
				}
				last = p.f.Timestamp
			}
		})
	}
}
