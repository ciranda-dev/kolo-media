package kolomedia

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	kolomoq "github.com/ciranda-dev/kolo-moq"
	"github.com/ciranda-dev/kolo-moq/secobj"
)

// heldTrack is a protected data track whose watcher holds the first key of the sender but not
// the one it rotated to after.
type heldTrack struct {
	a, w *secParty
	s    *kolomoq.Session
	tw   *kolomoq.TrackWriter
	sub  *kolomoq.Subscription
	op   *secobj.Opener
}

func newHeldTrack(t *testing.T) *heldTrack {
	t.Helper()
	h := &heldTrack{a: newSecParty(t), w: newSecParty(t)}
	if err := h.a.sender.SetMembers([]ed25519.PublicKey{h.w.pub}); err != nil {
		t.Fatal(err)
	}
	h.s = h.w.pairwise(t, h.a)
	h.w.receive(t, h.s, h.a)()
	if err := h.a.sender.Rotate(); err != nil {
		t.Fatal(err)
	}
	var err error
	if h.tw, err = h.a.origin.CreateTrack(h.a.ns, "data", &kolomoq.TrackOptions{Priority: 90}); err != nil {
		t.Fatal(err)
	}
	h.sub = rawSub(t, h.s, h.a.ns, "data", kolomoq.JoinNextObject)
	waitFor(t, "subscriber", func() bool { return h.tw.Subscribers() > 0 })
	if h.op, err = h.w.keys.Opener(h.s, h.a.ns, "data"); err != nil {
		t.Fatal(err)
	}
	return h
}

// write seals payload under the sender's current key in a new group and returns it as received.
func (h *heldTrack) write(t *testing.T, payload string) kolomoq.Object {
	t.Helper()
	g, err := h.tw.NewGroup()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.a.sender.Write(g, "data", []byte(payload)); err != nil {
		t.Fatal(err)
	}
	obj, err := nextNormal(t, h.sub, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

func (h *heldTrack) opener(maxAge time.Duration, drops *int) *opener {
	return &opener{keys: h.w.keys, op: h.op, maxAge: maxAge, maxFrames: 16, maxBytes: 1 << 20, drop: func(error) { *drops++ }}
}

// TestSecureHeldKeyBeforeLateRetry checks that a held object whose key arrived opens even when the
// retry runs after the hold expired (a reader woken late, or the expiry and the key wake together).
func TestSecureHeldKeyBeforeLateRetry(t *testing.T) {
	checkGoroutines(t)
	h := newHeldTrack(t)
	obj := h.write(t, "one")
	var drops int
	o := h.opener(400*time.Millisecond, &drops)
	if _, err := o.open(obj); !errors.Is(err, secobj.ErrNoKey) {
		t.Fatalf("open before the key: %v", err)
	}
	// Arrived a second ago, so the hold of 400ms has passed when the retry runs.
	o.hold(obj, time.Now().Add(-time.Second), h.w.keys.Changed())
	h.w.receive(t, h.s, h.a)
	o.retry()
	if drops != 0 || len(o.ready) != 1 || string(o.ready[0].obj.Payload) != "one" {
		t.Fatalf("drops %d, ready %d", drops, len(o.ready))
	}
	// Still no key: the expired object is dropped.
	if err := h.a.sender.Rotate(); err != nil {
		t.Fatal(err)
	}
	stale := h.write(t, "two")
	o.ready = nil
	h.w.keys.Remove(h.a.ns)
	o.hold(stale, time.Now().Add(-time.Second), h.w.keys.Changed())
	o.retry()
	if drops != 1 || len(o.ready) != 0 || len(o.held) != 0 || o.bytes != 0 {
		t.Fatalf("without the key: drops %d, ready %d, held %d, bytes %d", drops, len(o.ready), len(o.held), o.bytes)
	}
}

// TestSecureHeldKeyBeforeHold checks the wake when the key is installed between the failed open and
// the hold: the change taken before the open wakes the reader at once.
func TestSecureHeldKeyBeforeHold(t *testing.T) {
	checkGoroutines(t)
	h := newHeldTrack(t)
	obj := h.write(t, "one")
	var drops int
	o := h.opener(10*time.Second, &drops)
	changed := h.w.keys.Changed()
	if _, err := o.open(obj); !errors.Is(err, secobj.ErrNoKey) {
		t.Fatalf("open before the key: %v", err)
	}
	h.w.receive(t, h.s, h.a)
	o.hold(obj, time.Now(), changed)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, _, err := o.next(ctx, h.sub)
	if err != nil || string(got.Payload) != "one" || drops != 0 {
		t.Fatalf("next: %q, %v, drops %d", got.Payload, err, drops)
	}
}

// TestSecureHeldConcurrentInstall rotates the key while objects sealed under the new key race their
// key message to the watcher (separate streams): every object opens, none is dropped.
func TestSecureHeldConcurrentInstall(t *testing.T) {
	checkGoroutines(t)
	h := newHeldTrack(t)
	h.w.receive(t, h.s, h.a)
	var drops int
	o := h.opener(5*time.Second, &drops)
	type got struct {
		payload string
		err     error
	}
	const rounds, per = 30, 4
	out := make(chan got, per)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() {
		for n := 0; n < rounds*per; {
			obj, _, err := o.next(ctx, h.sub)
			if err == nil && obj.Status != kolomoq.StatusNormal {
				continue
			}
			n++
			out <- got{string(obj.Payload), err}
			if err != nil {
				return
			}
		}
	}()
	for r := range rounds {
		if err := h.a.sender.Rotate(); err != nil {
			t.Fatal(err)
		}
		for i := range per {
			g, err := h.tw.NewGroup()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.a.sender.Write(g, "data", fmt.Appendf(nil, "%d/%d", r, i)); err != nil {
				t.Fatal(err)
			}
			if i == rand.IntN(per) {
				time.Sleep(time.Duration(rand.IntN(3)) * time.Millisecond)
			}
		}
		// Each round drains before the next rotation, so no object outlives the previous key.
		seen := map[string]bool{}
		for range per {
			g := <-out
			if g.err != nil {
				t.Fatalf("round %d: %v", r, g.err)
			}
			seen[g.payload] = true
		}
		if len(seen) != per || drops != 0 {
			t.Fatalf("round %d: %d of %d objects, %d drops", r, len(seen), per, drops)
		}
	}
}

// TestSecureRetiredKeyCounted checks that objects under a Key ID older than the keys held (a
// joiner's view of the group cached before it joined) count as Retired, not as NoKey.
func TestSecureRetiredKeyCounted(t *testing.T) {
	checkGoroutines(t)
	a, j := newSecParty(t), newSecParty(t)
	clock := NewClock()
	vp := addTrack(t, a.b, videoTrack())
	for i := range 6 {
		ts, err := clock.At(time.Now().Add(time.Duration(i)*33*time.Millisecond), vp.Track().Timescale)
		if err != nil {
			t.Fatal(err)
		}
		if err := vp.WriteFrame(Frame{Timestamp: ts, Keyframe: i == 0, Data: make([]byte, 100)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.sender.SetMembers([]ed25519.PublicKey{j.pub}); err != nil {
		t.Fatal(err)
	}
	s := j.pairwise(t, a)
	j.receive(t, s, a)
	w := secWatch(t, s, a.ns, WatchOptions{Capabilities: avCaps(), Keys: j.keys})
	waitFor(t, "the cached group dropped", func() bool { return w.Stats().Video.Retired == 6 })
	if st := w.Stats().Video; st.NoKey != 0 || st.Unauthenticated != 0 {
		t.Errorf("NoKey %d, Unauthenticated %d", st.NoKey, st.Unauthenticated)
	}
}

// TestSecureHeldRetryOutcomes checks each outcome of a retry: without its key a held object stays
// (no time bound when maxAge is 0) and the reader waits for the next change, not the one that woke
// it; an object that fails to open once its key is there is dropped at once.
func TestSecureHeldRetryOutcomes(t *testing.T) {
	checkGoroutines(t)
	h := newHeldTrack(t)
	obj := h.write(t, "one")
	var drops int
	o := h.opener(0, &drops)
	o.hold(obj, time.Now().Add(-time.Hour), h.w.keys.Changed())
	// Another sender's key wakes the reader but does not open the object.
	b := newSecParty(t)
	if err := b.sender.SetMembers([]ed25519.PublicKey{h.w.pub}); err != nil {
		t.Fatal(err)
	}
	h.w.receive(t, h.w.pairwise(t, b), b)
	select {
	case <-o.wake:
	default:
		t.Fatal("the other key did not wake the reader")
	}
	o.retry()
	if drops != 0 || len(o.held) != 1 || len(o.ready) != 0 {
		t.Fatalf("without its key, maxAge 0: drops %d, held %d, ready %d", drops, len(o.held), len(o.ready))
	}
	select {
	case <-o.wake:
		t.Fatal("the reader would wake again on the change already handled")
	default:
	}

	tampered := h.write(t, "two")
	tampered.Payload = slices.Clone(tampered.Payload)
	tampered.Payload[0] ^= 1
	o = h.opener(time.Hour, &drops)
	o.hold(tampered, time.Now(), h.w.keys.Changed())
	h.w.receive(t, h.s, h.a)
	o.retry()
	if drops != 1 || len(o.held) != 0 || len(o.ready) != 0 || o.bytes != 0 {
		t.Fatalf("tampered: drops %d, held %d, ready %d, bytes %d", drops, len(o.held), len(o.ready), o.bytes)
	}
}

// installOnChange installs a key the first time the opener asks for the keyring's change channel,
// so the key arrives between that call and the open that follows it.
type installOnChange struct {
	keys    *secobj.Keyring
	install func()
	done    bool
}

func (c *installOnChange) Changed() <-chan struct{} {
	if !c.done {
		c.done = true
		c.install()
	}
	return c.keys.Changed()
}

// TestSecureNextTakesChangeBeforeOpen checks that the reader takes the keyring's change channel
// before it opens an object: a key installed after that is seen by the open, or wakes the hold.
func TestSecureNextTakesChangeBeforeOpen(t *testing.T) {
	checkGoroutines(t)
	h := newHeldTrack(t)
	g, err := h.tw.NewGroup()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.a.sender.Write(g, "data", []byte("one")); err != nil {
		t.Fatal(err)
	}
	var drops int
	o := h.opener(10*time.Second, &drops)
	o.keys = &installOnChange{keys: h.w.keys, install: func() { h.w.receive(t, h.s, h.a) }}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, _, err := o.next(ctx, h.sub)
	if err != nil || string(got.Payload) != "one" || drops != 0 {
		t.Fatalf("next: %q, %v, drops %d", got.Payload, err, drops)
	}
}

// TestCatalogHoldKeepsNewestVersion checks that the catalog opener holds the newest version (the
// highest group) whatever the arrival order: groups travel on separate streams, so a relay can
// deliver version 4 before version 3.
func TestCatalogHoldKeepsNewestVersion(t *testing.T) {
	for _, order := range [][]uint64{{4, 3}, {3, 4}} {
		var drops int
		o := catalogOpener(nil, nil, func(error) { drops++ })
		for _, g := range order {
			o.hold(kolomoq.Object{Group: g, Payload: make([]byte, 10)}, time.Now(), nil)
		}
		if len(o.held) != 1 || o.held[0].obj.Group != 4 || drops != 1 || o.bytes != 10 {
			t.Fatalf("arrival %v: held %+v, drops %d, bytes %d", order, o.held, drops, o.bytes)
		}
	}
}

// TestSecureCatalogNewestOpensAfterKey holds two catalog groups under a key in flight, the newer
// first; once the key arrives the newer one opens.
func TestSecureCatalogNewestOpensAfterKey(t *testing.T) {
	checkGoroutines(t)
	h := newHeldTrack(t)
	older := h.write(t, "v1")
	newer := h.write(t, "v2")
	var drops int
	o := catalogOpener(h.w.keys, h.op, func(error) { drops++ })
	changed := h.w.keys.Changed()
	o.hold(newer, time.Now(), changed)
	o.hold(older, time.Now(), changed)
	h.w.receive(t, h.s, h.a)
	o.retry()
	if drops != 1 || len(o.ready) != 1 || string(o.ready[0].obj.Payload) != "v2" {
		t.Fatalf("drops %d, ready %+v", drops, o.ready)
	}
}
