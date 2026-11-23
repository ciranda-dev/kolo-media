package kolomedia

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolomoq "github.com/ciranda-dev/kolo-moq"
	"github.com/ciranda-dev/kolo-moq/secobj"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolotransport "github.com/ciranda-dev/kolo-transport"
)

// secRoom is the room of the protected tests; a member's media namespace is {room, hex(key)}.
var secRoom = kolomoq.Namespace{"sec-room"}

// secTransport lets every participant on 127.0.0.1 reach one listener.
var secTransport = &kolotransport.Config{MaxIncomingStreams: 1000, MaxConnsPerIP: 100}

func memberNS(pub ed25519.PublicKey) kolomoq.Namespace {
	return append(slices.Clone(secRoom), hex.EncodeToString(pub))
}

// ownerAuth is the meeting rule: a peer owns {room, hex(peer)}.
func ownerAuth(peer ed25519.PublicKey, ns kolomoq.Namespace) error {
	if len(peer) == ed25519.PublicKeySize && slices.Equal(ns, memberNS(peer)) {
		return nil
	}
	return errors.New("not the owner")
}

type secNode struct {
	id     *koloid.Identity
	pub    ed25519.PublicKey
	origin *kolomoq.Origin
	addr   string // serves origin to every peer that dials it
}

func newSecNode(t *testing.T) *secNode {
	t.Helper()
	id, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	n := &secNode{id: id, pub: id.PublicKey(), origin: newOrigin(t, nil)}
	n.addr = secListen(t, id, func(ctx context.Context, c kolomoq.Conn) (*kolomoq.Session, error) {
		return kolomoq.NewServer(ctx, c, &kolomoq.Config{Origin: n.origin})
	})
	return n
}

// secListen accepts kolo-secure connections on 127.0.0.1 with id and starts a session on each.
func secListen(t *testing.T, id *koloid.Identity, serve func(ctx context.Context, c kolomoq.Conn) (*kolomoq.Session, error)) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	l, err := kolosecure.Listen(ctx, "127.0.0.1:0", id, &kolosecure.Config{Transport: secTransport})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var sessions []*kolomoq.Session
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := l.Accept(ctx)
			if err != nil {
				return
			}
			wg.Go(func() {
				if s, err := serve(ctx, c); err == nil {
					mu.Lock()
					sessions = append(sessions, s)
					mu.Unlock()
				}
			})
		}
	})
	t.Cleanup(func() {
		cancel()
		l.Close()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		for _, s := range sessions {
			s.Close()
		}
	})
	return l.Addr().String()
}

// dial connects n to the peer listening at addr and returns n's session, serving origin.
func (n *secNode) dial(t *testing.T, addr string, peer ed25519.PublicKey, origin *kolomoq.Origin) *kolomoq.Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := kolosecure.Dial(ctx, addr, n.id, peer, &kolosecure.Config{Transport: secTransport})
	if err != nil {
		t.Fatal(err)
	}
	s, err := kolomoq.NewClient(ctx, c, &kolomoq.Config{Origin: origin})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// secRelay is a real kolo-moq Relay on 127.0.0.1 with RoomPolicy for secRoom.
type secRelay struct {
	*secNode
	mu      sync.Mutex
	members map[string]bool
}

func newSecRelay(t *testing.T) *secRelay {
	t.Helper()
	id, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	rm := &secRelay{secNode: &secNode{id: id, pub: id.PublicKey()}, members: map[string]bool{}}
	policy := kolomoq.RoomPolicy{Room: secRoom, Member: func(peer ed25519.PublicKey) bool {
		rm.mu.Lock()
		defer rm.mu.Unlock()
		return rm.members[string(peer)]
	}}
	r, err := kolomoq.NewRelay(&kolomoq.RelayConfig{Session: kolomoq.Config{Authorize: policy.Authorize}})
	if err != nil {
		t.Fatal(err)
	}
	rm.addr = secListen(t, id, r.NewServer)
	return rm
}

// secParty is a participant: a node with a protected broadcast of its namespace and a Keyring.
type secParty struct {
	*secNode
	ns     kolomoq.Namespace
	sender *secobj.Sender
	b      *Broadcast
	keys   *secobj.Keyring
	relay  *kolomoq.Session
}

func newSecParty(t *testing.T, relays ...ed25519.PublicKey) *secParty {
	t.Helper()
	n := newSecNode(t)
	p := &secParty{secNode: n, ns: memberNS(n.pub)}
	var err error
	if p.sender, err = secobj.NewSender(n.origin, p.ns, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.sender.Close() })
	if p.b, err = NewSecureBroadcast(n.origin, p.sender, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.b.Close() })
	if p.keys, err = secobj.NewKeyring(secobj.KeyringConfig{Authorize: ownerAuth, Relays: relays}); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *secParty) join(t *testing.T, rm *secRelay) {
	t.Helper()
	rm.mu.Lock()
	rm.members[string(p.pub)] = true
	rm.mu.Unlock()
	p.relay = p.dial(t, rm.addr, rm.pub, p.origin)
	if _, err := p.relay.PublishNamespace(testCtx(t), p.ns); err != nil {
		t.Fatal(err)
	}
}

// pairwise returns p's direct session to from; its peer is from.
func (p *secParty) pairwise(t *testing.T, from *secParty) *kolomoq.Session {
	t.Helper()
	return p.dial(t, from.addr, from.pub, nil)
}

// receive runs p.keys.Receive for from's namespace on s until the returned stop is called, and
// waits for the first key it installs.
func (p *secParty) receive(t *testing.T, s *kolomoq.Session, from *secParty) (stop func()) {
	t.Helper()
	changed := p.keys.Changed()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.keys.Receive(ctx, s, p.pub, from.ns) }()
	select {
	case <-changed:
	case err := <-done:
		cancel()
		t.Fatalf("receive: %v", err)
	case <-time.After(20 * time.Second):
		cancel()
		t.Fatal("no key arrived")
	}
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

// openOnce opens obj of track under ns, read on s, with a new Opener of keys.
func openOnce(keys *secobj.Keyring, s *kolomoq.Session, ns kolomoq.Namespace, track string, obj kolomoq.Object) ([]byte, []kolomoq.Property, error) {
	o, err := keys.Opener(s, ns, track)
	if err != nil {
		return nil, nil, err
	}
	return o.Open(obj)
}

func secWatch(t *testing.T, s *kolomoq.Session, ns kolomoq.Namespace, opts WatchOptions) *Watcher {
	t.Helper()
	w, err := Watch(testCtx(t), s, ns, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func rawSub(t *testing.T, s *kolomoq.Session, ns kolomoq.Namespace, track string, join kolomoq.Join) *kolomoq.Subscription {
	t.Helper()
	sub, err := s.Subscribe(testCtx(t), ns, track, &kolomoq.SubscribeOptions{Join: join})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sub.Close() })
	return sub
}

// nextNormal returns the next object of sub that has a payload, or fails after d.
func nextNormal(t *testing.T, sub *kolomoq.Subscription, d time.Duration) (kolomoq.Object, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for {
		o, err := sub.ReadObject(ctx)
		if err != nil || o.Status == kolomoq.StatusNormal {
			return o, err
		}
	}
}

// checkSealed checks that o carries exactly one visible property, Immutable Properties holding
// the Key ID kid at type 0x78, and that its payload does not hold plain.
func checkSealed(t *testing.T, what string, o kolomoq.Object, kid uint64, plain []byte) {
	t.Helper()
	if kid > 127 {
		t.Fatalf("Key ID %d needs a longer vi64 than this check builds", kid)
	}
	want := []kolomoq.Property{{Type: 0x0B, Bytes: []byte{secobj.PropertyKeyID, byte(kid)}}}
	if len(o.Properties) != 1 || o.Properties[0].Type != want[0].Type || !bytes.Equal(o.Properties[0].Bytes, want[0].Bytes) {
		t.Errorf("%s: visible properties %+v, want %+v", what, o.Properties, want)
	}
	if len(plain) > 0 && bytes.Contains(o.Payload, plain) {
		t.Errorf("%s: payload holds the plaintext", what)
	}
}

func audioFrame(i int, level *AudioLevel) Frame {
	return Frame{Timestamp: 1_000_000_000 + uint64(i)*960, Data: bytes.Repeat([]byte{0xA5}, 80), AudioLevel: level}
}

// TestSecureRelay publishes a protected broadcast through a real Relay with RoomPolicy and plays it
// at two members, and checks that the relay only sees sealed objects.
func TestSecureRelay(t *testing.T) {
	checkGoroutines(t)
	rm := newSecRelay(t)
	a, b, c := newSecParty(t, rm.pub), newSecParty(t, rm.pub), newSecParty(t, rm.pub)
	for _, p := range []*secParty{a, b, c} {
		p.join(t, rm)
	}
	if err := a.sender.SetMembers([]ed25519.PublicKey{b.pub, c.pub}); err != nil {
		t.Fatal(err)
	}
	for _, p := range []*secParty{b, c} {
		p.receive(t, p.pairwise(t, a), a)
	}
	clock := NewClock()
	startSource(t, addTrack(t, a.b, audioTrack()), clock, 20*time.Millisecond, 1)
	startSource(t, addTrack(t, a.b, videoTrack()), clock, 33*time.Millisecond, 15)

	var wg sync.WaitGroup
	for _, p := range []*secParty{b, c} {
		w := secWatch(t, p.relay, a.ns, WatchOptions{Capabilities: avCaps(), Keys: p.keys})
		wg.Go(func() {
			audio, video := collectAV(t, w, clock, 50, 30)
			if len(video) == 0 || !video[0].f.Keyframe {
				t.Error("protected video did not start on a keyframe")
			}
			for _, f := range audio {
				if f.f.AudioLevel != nil {
					t.Error("a protected frame has an audio level")
				}
			}
			s := w.Stats()
			if s.Audio.Unauthenticated+s.Video.Unauthenticated+s.Audio.Malformed+s.Video.Malformed != 0 {
				t.Errorf("honest relay: stats %+v", s)
			}
		})
	}
	wg.Wait()

	kid := a.sender.KeyID()
	plain, err := a.b.Catalog().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for _, track := range []string{CatalogTrack, "mic", "cam"} {
		o, err := nextNormal(t, rawSub(t, c.relay, a.ns, track, kolomoq.JoinCurrentGroup), 10*time.Second)
		if err != nil {
			t.Fatalf("%s: %v", track, err)
		}
		if track == CatalogTrack {
			checkSealed(t, track, o, kid, plain[1:])
			if len(o.Payload) != sealedLen(len(plain)) {
				t.Errorf("sealed catalog of %d bytes, want %d", len(o.Payload), sealedLen(len(plain)))
			}
			continue
		}
		checkSealed(t, track, o, kid, nil)
	}

	// A data track of the application uses the same Sender and Keyring.
	chat, err := a.origin.CreateTrack(a.ns, "chat", &kolomoq.TrackOptions{Priority: 90})
	if err != nil {
		t.Fatal(err)
	}
	defer chat.Close()
	sub := rawSub(t, b.relay, a.ns, "chat", kolomoq.JoinNextObject)
	waitFor(t, "chat subscriber", func() bool { return chat.Subscribers() > 0 })
	g, err := chat.NewGroup()
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("raised hand")
	if _, err := a.sender.Write(g, "chat", msg); err != nil {
		t.Fatal(err)
	}
	o, err := nextNormal(t, sub, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	checkSealed(t, "chat", o, a.sender.KeyID(), msg)
	if pt, _, err := openOnce(b.keys, b.relay, a.ns, "chat", o); err != nil || !bytes.Equal(pt, msg) {
		t.Errorf("chat opened %q, %v", pt, err)
	}
}

// TestSecureAudioSizes checks C2: protected audio carries no AUDIO_LEVEL, so ciphertext sizes are
// equal with and without voice.
func TestSecureAudioSizes(t *testing.T) {
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
	levels := []*AudioLevel{nil, {Voice: true, Level: 3}, {Voice: false, Level: 127}, {Voice: true, Level: 127}, {Level: 0}}
	for i := range 20 {
		if err := mic.WriteFrame(audioFrame(i, levels[i%len(levels)])); err != nil {
			t.Fatal(err)
		}
	}
	if err := mic.WriteFrame(audioFrame(20, &AudioLevel{Level: 128})); !errors.Is(err, ErrInvalidFrame) {
		t.Errorf("an invalid audio level on a protected track: %v", err)
	}
	op, err := w.keys.Opener(s, a.ns, "mic")
	if err != nil {
		t.Fatal(err)
	}
	size := -1
	for i := range 20 {
		o, err := nextNormal(t, sub, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		checkSealed(t, "mic", o, a.sender.KeyID(), audioFrame(i, nil).Data)
		if size >= 0 && len(o.Payload) != size {
			t.Errorf("object %d: %d bytes, others %d", i, len(o.Payload), size)
		}
		size = len(o.Payload)
		pt, props, err := op.Open(o)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(pt, audioFrame(i, nil).Data) {
			t.Error("payload changed")
		}
		var prev uint64
		list := 0
		for _, p := range props {
			if p.Type == PropertyAudioLevel {
				t.Errorf("object %d carries AUDIO_LEVEL %#x", i, p.Value)
			}
			list += vi64Len(p.Type-prev) + vi64Len(p.Value)
			prev = p.Type
		}
		// Unprotected, the same properties are visible; protected, the visible list holds only the
		// 4-byte Key ID property.
		if over := len(o.Payload) + 4 - len(pt) - list; over != 24 {
			t.Errorf("object %d: %d bytes of overhead, want 24", i, over)
		}
	}
}

func vi64Len(v uint64) int {
	n := 1
	for n < 8 && v >= 1<<(7*n) {
		n++
	}
	if n == 8 && v >= 1<<56 {
		return 9
	}
	return n
}

// TestSecureWrongSession checks C4 in direct mode: a member serving another member's namespace on
// its own session is refused, the owner's own session is not.
func TestSecureWrongSession(t *testing.T) {
	checkGoroutines(t)
	v, m, w := newSecParty(t), newSecParty(t), newSecParty(t)
	if err := v.sender.SetMembers([]ed25519.PublicKey{w.pub, m.pub}); err != nil {
		t.Fatal(err)
	}
	sv := w.pairwise(t, v)
	w.receive(t, sv, v)
	clock := NewClock()
	startSource(t, addTrack(t, v.b, audioTrack()), clock, 20*time.Millisecond, 1)

	// m forwards v's broadcast unchanged on its own origin; every object authenticates.
	forward(t, m.dial(t, v.addr, v.pub, nil), v.ns, m.origin, v.ns, map[string]string{CatalogTrack: CatalogTrack, "mic": "mic"}, nil)
	opts := WatchOptions{Capabilities: avCaps(), Keys: w.keys}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := Watch(ctx, w.pairwise(t, m), v.ns, opts); !errors.Is(err, secobj.ErrWrongSession) || !strings.HasPrefix(err.Error(), "kolomedia: ") {
		t.Fatalf("watch through another member: %v", err)
	}
	first := secWatch(t, sv, v.ns, opts)
	collect(t, first, clock, Audio, 10)
}

// TestSecureKeyHold checks that an object under a Key ID newer than every key held waits for its
// key within MaxDelay and MaxFrames, and is counted as NoKey beyond.
func TestSecureKeyHold(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withhold  time.Duration
		maxFrames int
		dropped   bool
	}{
		{"key within MaxDelay", 100 * time.Millisecond, 0, false},
		{"key after MaxDelay", 1200 * time.Millisecond, 0, true},
		{"over MaxFrames", 300 * time.Millisecond, 8, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkGoroutines(t)
			a, wp := newSecParty(t), newSecParty(t)
			if err := a.sender.SetMembers([]ed25519.PublicKey{wp.pub}); err != nil {
				t.Fatal(err)
			}
			s := wp.pairwise(t, a)
			stop := wp.receive(t, s, a)
			clock := NewClock()
			startSource(t, addTrack(t, a.b, audioTrack()), clock, 20*time.Millisecond, 1)
			w := secWatch(t, s, a.ns, WatchOptions{Capabilities: avCaps(), Keys: wp.keys, Playout: PlayoutConfig{MaxFrames: tc.maxFrames}})
			collect(t, w, clock, Audio, 10)
			if n := w.Stats().Audio.NoKey; n != 0 {
				t.Fatalf("NoKey %d before the rotation", n)
			}

			stop()
			if err := a.sender.Rotate(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(tc.withhold)
			wp.receive(t, s, a)
			collect(t, w, clock, Audio, 10)
			st := w.Stats().Audio
			if tc.dropped != (st.NoKey > 0) || st.Unauthenticated != 0 {
				t.Errorf("withheld %v: NoKey %d, Unauthenticated %d", tc.withhold, st.NoKey, st.Unauthenticated)
			}
		})
	}
}

// TestSecureCameraToggle checks C7: a video track removed and added again restarts its group IDs,
// so the sender rotates before it seals the new track, and the watcher plays it.
func TestSecureCameraToggle(t *testing.T) {
	checkGoroutines(t)
	a, wp := newSecParty(t), newSecParty(t)
	if err := a.sender.SetMembers([]ed25519.PublicKey{wp.pub}); err != nil {
		t.Fatal(err)
	}
	s := wp.pairwise(t, a)
	wp.receive(t, s, a)
	clock := NewClock()
	src := startSource(t, addTrack(t, a.b, videoTrack()), clock, 33*time.Millisecond, 10)
	w := secWatch(t, s, a.ns, WatchOptions{Capabilities: avCaps(), Keys: wp.keys})
	collect(t, w, clock, Video, 5)

	kid := a.sender.KeyID()
	src.Stop()
	if err := a.b.RemoveTrack("cam"); err != nil {
		t.Fatal(err)
	}
	cam := addTrack(t, a.b, videoTrack())
	sub := rawSub(t, s, a.ns, "cam", kolomoq.JoinNextObject)
	waitFor(t, "subscriber", func() bool { return cam.writer.Subscribers() > 0 })
	startSource(t, cam, clock, 33*time.Millisecond, 10)
	o, err := nextNormal(t, sub, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if a.sender.KeyID() <= kid {
		t.Fatalf("Key ID %d after the toggle, %d before", a.sender.KeyID(), kid)
	}
	checkSealed(t, "cam", o, a.sender.KeyID(), nil)
	waitFor(t, "the new camera track", func() bool { return w.Stats().Video.Subscriptions == 2 })
	collect(t, w, clock, Video, 5)
	if st := w.Stats().Video; st.Unauthenticated != 0 {
		t.Errorf("stats %+v", st)
	}
}

// TestSecureWriteFailure checks that a frame that cannot be sealed is not written at all.
func TestSecureWriteFailure(t *testing.T) {
	checkGoroutines(t)
	if _, err := NewSecureBroadcast(newOrigin(t, nil), nil, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("nil sender: %v", err)
	}
	a, wp := newSecParty(t), newSecParty(t)
	s := wp.pairwise(t, a)
	mic := addTrack(t, a.b, audioTrack())
	sub := rawSub(t, s, a.ns, "mic", kolomoq.JoinNextObject)
	waitFor(t, "subscriber", func() bool { return mic.writer.Subscribers() > 0 })
	if err := mic.WriteFrame(audioFrame(0, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := nextNormal(t, sub, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	a.sender.Close()
	if err := mic.WriteFrame(audioFrame(1, nil)); !errors.Is(err, kolomoq.ErrClosed) || !strings.HasPrefix(err.Error(), "kolomedia: ") {
		t.Errorf("write with a closed sender: %v", err)
	}
	if o, err := nextNormal(t, sub, 300*time.Millisecond); err == nil {
		t.Errorf("an object was written after sealing failed: %d bytes", len(o.Payload))
	}
	o2 := newOrigin(t, nil)
	if _, err := NewSecureBroadcast(o2, a.sender, nil); !errors.Is(err, kolomoq.ErrClosed) {
		t.Errorf("closed sender: %v", err)
	}
	if w, err := o2.CreateTrack(a.ns, CatalogTrack, nil); err != nil {
		t.Errorf("the failed broadcast left its catalog track: %v", err)
	} else {
		w.Close()
	}
}

// TestSecureBroadcastOptions checks that NewSecureBroadcast applies its options like NewBroadcast.
func TestSecureBroadcastOptions(t *testing.T) {
	checkGoroutines(t)
	n := newSecNode(t)
	s, err := secobj.NewSender(n.origin, memberNS(n.pub), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if _, err := NewSecureBroadcast(n.origin, s, &BroadcastOptions{CatalogLimits: CatalogLimits{MaxNameLen: defaultMaxNameLen + 1}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("name limit above the MoQ bound: %v", err)
	}
	b, err := NewSecureBroadcast(n.origin, s, &BroadcastOptions{CatalogLimits: CatalogLimits{MaxTracks: 1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	addTrack(t, b, audioTrack())
	if _, err := b.AddTrack(lowVideo(), PackagerOptions{}); !errors.Is(err, ErrInvalidCatalog) {
		t.Errorf("AddTrack past the option's MaxTracks: %v", err)
	}
}

// TestSecureConcurrentRotation writes audio and video while the members change and the camera
// toggles, and checks that the watcher keeps playing with no failed authentication.
func TestSecureConcurrentRotation(t *testing.T) {
	checkGoroutines(t)
	a, wp, x := newSecParty(t), newSecParty(t), newSecParty(t)
	if err := a.sender.SetMembers([]ed25519.PublicKey{wp.pub}); err != nil {
		t.Fatal(err)
	}
	s := wp.pairwise(t, a)
	wp.receive(t, s, a)
	clock := NewClock()
	startSource(t, addTrack(t, a.b, audioTrack()), clock, 20*time.Millisecond, 1)
	src := startSource(t, addTrack(t, a.b, videoTrack()), clock, 33*time.Millisecond, 5)
	w := secWatch(t, s, a.ns, WatchOptions{Capabilities: avCaps(), Keys: wp.keys})
	collectAV(t, w, clock, 10, 5)

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 10 {
			members := []ed25519.PublicKey{wp.pub}
			if i%2 == 0 {
				members = append(members, x.pub)
			}
			if err := a.sender.SetMembers(members); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
	})
	wg.Go(func() {
		for range 3 {
			src.Stop()
			if err := a.b.RemoveTrack("cam"); err != nil {
				t.Error(err)
				return
			}
			p, err := a.b.AddTrack(videoTrack(), PackagerOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			src = startSource(t, p, clock, 33*time.Millisecond, 5)
			time.Sleep(100 * time.Millisecond)
		}
	})
	collectAV(t, w, clock, 30, 10)
	wg.Wait()
	collectAV(t, w, clock, 10, 5)
	// Each subscription has its own replay window, so a restarted one reads its current group again.
	st := w.Stats()
	if st.Audio.Unauthenticated != 0 || st.Video.Unauthenticated != 0 {
		t.Errorf("stats %+v", st)
	}
}

// TestSecureLateJoiner checks that a member who joins a running broadcast opens nothing sealed
// before it joined, and reads the catalog that the next frame publishes under the new key.
func TestSecureLateJoiner(t *testing.T) {
	checkGoroutines(t)
	a, j := newSecParty(t), newSecParty(t)
	clock := NewClock()
	startSource(t, addTrack(t, a.b, audioTrack()), clock, 20*time.Millisecond, 1)
	startSource(t, addTrack(t, a.b, videoTrack()), clock, 33*time.Millisecond, 10)
	time.Sleep(200 * time.Millisecond)
	version := a.b.Catalog().Version

	if err := a.sender.SetMembers([]ed25519.PublicKey{j.pub}); err != nil {
		t.Fatal(err)
	}
	s := j.pairwise(t, a)
	j.receive(t, s, a)
	w := secWatch(t, s, a.ns, WatchOptions{Capabilities: avCaps(), Keys: j.keys})
	if v := w.Stats().Catalog; v <= version {
		t.Errorf("joiner plays catalog version %d, published before it joined at %d", v, version)
	}
	audio, video := collectAV(t, w, clock, 20, 10)
	if len(video) == 0 || !video[0].f.Keyframe {
		t.Error("the joiner's video did not start on a keyframe")
	}
	if len(audio) > 0 && audio[0].offset > time.Second {
		t.Errorf("the joiner played audio captured %v before", audio[0].offset)
	}
	if st := w.Stats(); st.Audio.Unauthenticated+st.Video.Unauthenticated != 0 {
		t.Errorf("stats %+v", st)
	}
}
