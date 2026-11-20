package kolomedia

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolomoq "github.com/ciranda-dev/kolo-moq"
	"github.com/ciranda-dev/kolo-moq/secobj"
	kolosecure "github.com/ciranda-dev/kolo-secure"
)

// Synthetic media of a protected group call: Opus-sized constant bitrate audio (32 kbit/s, 20 ms)
// and one 720p-class video stream of about 1 Mbit/s at 30 fps, with 4 KiB delta frames and a 32 KiB
// keyframe every 2 s and at every key rotation (the sender forces one, decision 0010).
const (
	scaleAudioBytes = 80
	scaleVideoDelta = 4096
	scaleVideoKey   = 32768
	scaleGOP        = 60
	scaleAudioEvery = 20 * time.Millisecond
	scaleVideoEvery = time.Second / 30

	// scaleKeyBound is the hold limit of decision 0010: a frame under a key still in flight waits
	// at most the playout MaxDelay for it.
	scaleKeyBound = 400 * time.Millisecond
)

// raceBuild reports whether the test binary was built with the race detector.
func raceBuild() bool {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return false
	}
	for _, s := range bi.Settings {
		if s.Key == "-race" {
			return s.Value == "true"
		}
	}
	return false
}

func scaleFrame(size int, seq uint64, at time.Duration, magic *[16]byte) []byte {
	d := bytes.Repeat([]byte{0xA5}, size)
	binary.BigEndian.PutUint64(d, seq)
	binary.BigEndian.PutUint64(d[8:], uint64(at))
	copy(d[16:], magic[:])
	return d
}

type scaleSource struct {
	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

func (s *scaleSource) Stop() {
	s.once.Do(func() { close(s.stop) })
	s.wg.Wait()
}

// scaleParty is a participant of the call: its own identity, node, Sender, Keyring and relay session.
type scaleParty struct {
	*secParty
	magic  [16]byte
	ctx    context.Context
	cancel context.CancelFunc
	vseq   atomic.Uint64
	gone   atomic.Bool

	mu       sync.Mutex
	links    map[*scaleParty]*keyLink
	sessions []*kolomoq.Session
	audio    *scaleSource
	video    *scaleSource
}

// keyLink is a participant's pairwise session to a sender and its Keyring.Receive on it.
type keyLink struct {
	sess   *kolomoq.Session
	cancel context.CancelFunc
	done   chan struct{}
}

// pairWatch is one participant watching another through the relay.
type pairWatch struct {
	v, s       *scaleParty
	w          *Watcher
	muted      atomic.Bool
	firstVideo atomic.Int64 // play time of the first video frame, since the clock start
	markAt     atomic.Int64 // capture time from which the next video frame is the one to time
	markPlay   atomic.Int64
	oldNoKey   atomic.Uint64 // counted before the first frame played, at a joiner
	oldRetired atomic.Uint64
	oldBadCat  atomic.Uint64
	lastCap    [3]atomic.Int64 // newest capture time played, per kind
	wg         sync.WaitGroup
}

// scenario holds what one scenario measures.
type scenario struct {
	name   string
	start  time.Time
	end    time.Time
	events int
	played [3]atomic.Int64
	lost   [3]atomic.Int64
	maxGap [3]atomic.Int64

	mu      sync.Mutex
	keyLat  []time.Duration // key delivery to a member that already has a session with the sender
	joinLat []time.Duration // key delivery to a new member, session set-up included
	first   []time.Duration // time to the joiner's first video frame, at the others
	firstJ  []time.Duration // time to the first video frame of each sender, at the joiner
	resume  []time.Duration // video resumes after a camera toggle
	st0     statSum
	st1     statSum
}

func (sc *scenario) add(dst *[]time.Duration, d time.Duration) {
	sc.mu.Lock()
	*dst = append(*dst, d)
	sc.mu.Unlock()
}

// statSum adds up the Watcher counters of a call. old counts what a joiner could not open of the
// media the relay still held from before it joined: the objects sealed under the previous key and
// the catalog.
type statSum struct{ noKey, retired, unauth, malformed, gaps, badCat, old uint64 }

func (a statSum) sub(b statSum) statSum {
	return statSum{a.noKey - b.noKey, a.retired - b.retired, a.unauth - b.unauth, a.malformed - b.malformed, a.gaps - b.gaps, a.badCat - b.badCat, a.old - b.old}
}

type scaleRoom struct {
	t      *testing.T
	rm     *secRelay
	clock  *Clock
	ctx    context.Context
	cancel context.CancelFunc
	spy    *scaleSpy
	sc     atomic.Pointer[scenario]
	probes atomic.Uint64

	// reordered counts frames played after a later one. The playout drops a frame older than one
	// already played, so none is expected; strictOrder makes one fail the test.
	reordered   atomic.Int64
	strictOrder bool

	mu      sync.Mutex
	parties []*scaleParty
	pairs   map[[2]*scaleParty]*pairWatch
	folded  statSum
	done    []*scenario
}

func newScaleRoom(t *testing.T) *scaleRoom {
	r := &scaleRoom{t: t, rm: newSecRelay(t), clock: NewClock(), pairs: map[[2]*scaleParty]*pairWatch{}}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.spy = newScaleSpy(r)
	return r
}

// check stops the test at the first failure of a step, so one failure does not become minutes of waiting.
func (r *scaleRoom) check() {
	r.t.Helper()
	if r.t.Failed() {
		r.t.FailNow()
	}
}

func (r *scaleRoom) elapsed() time.Duration { return time.Since(r.clock.Start()) }

func (r *scaleRoom) begin(name string) {
	sc := &scenario{name: name, start: time.Now()}
	sc.st0 = r.sumStats()
	r.sc.Store(sc)
}

func (r *scaleRoom) end() *scenario {
	sc := r.sc.Load()
	sc.end = time.Now()
	sc.st1 = r.sumStats()
	r.mu.Lock()
	r.done = append(r.done, sc)
	r.mu.Unlock()
	r.check()
	return sc
}

func (r *scaleRoom) sumStats() statSum {
	r.mu.Lock()
	defer r.mu.Unlock()
	sum := r.folded
	for _, pw := range r.pairs {
		if !pw.muted.Load() {
			sum = sum.plus(pw.counted())
		}
	}
	return sum
}

// counted returns the Watcher's stats without the objects of before the joiner's first frame.
func (pw *pairWatch) counted() statSum {
	st := addStats(statSum{}, pw.w.Stats())
	st.noKey -= pw.oldNoKey.Load()
	st.retired -= pw.oldRetired.Load()
	st.badCat -= pw.oldBadCat.Load()
	return st
}

func (a statSum) plus(b statSum) statSum {
	return statSum{a.noKey + b.noKey, a.retired + b.retired, a.unauth + b.unauth, a.malformed + b.malformed, a.gaps + b.gaps, a.badCat + b.badCat, a.old + b.old}
}

func addStats(sum statSum, st WatchStats) statSum {
	sum.noKey += st.Audio.NoKey + st.Video.NoKey
	sum.retired += st.Audio.Retired + st.Video.Retired
	sum.unauth += st.Audio.Unauthenticated + st.Video.Unauthenticated
	sum.malformed += st.Audio.Malformed + st.Video.Malformed
	sum.gaps += st.Video.Gaps
	sum.badCat += st.BadCatalogs
	return sum
}

func (r *scaleRoom) snapshot() []*scaleParty {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.parties)
}

func pubs(ps []*scaleParty) []ed25519.PublicKey {
	out := make([]ed25519.PublicKey, len(ps))
	for i, p := range ps {
		out[i] = p.pub
	}
	return out
}

func without(ps []*scaleParty, p *scaleParty) []*scaleParty {
	return slices.DeleteFunc(slices.Clone(ps), func(x *scaleParty) bool { return x == p })
}

func (r *scaleRoom) await(what string, d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Errorf("timed out after %v waiting for %s", d, what)
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

func par(n int, fn func(i int)) {
	sem := make(chan struct{}, 24)
	var wg sync.WaitGroup
	for i := range n {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			fn(i)
		})
	}
	wg.Wait()
}

func dialSession(ctx context.Context, id *koloid.Identity, addr string, peer ed25519.PublicKey, origin *kolomoq.Origin) (*kolomoq.Session, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	c, err := kolosecure.Dial(ctx, addr, id, peer, &kolosecure.Config{Transport: secTransport})
	if err != nil {
		return nil, err
	}
	return kolomoq.NewClient(ctx, c, &kolomoq.Config{Origin: origin})
}

func (r *scaleRoom) newParty() *scaleParty {
	p := &scaleParty{secParty: newSecParty(r.t, r.rm.pub), links: map[*scaleParty]*keyLink{}}
	rand.Read(p.magic[:])
	p.ctx, p.cancel = context.WithCancel(r.ctx)
	return p
}

func (r *scaleRoom) joinRelay(p *scaleParty) error {
	r.rm.mu.Lock()
	r.rm.members[string(p.pub)] = true
	r.rm.mu.Unlock()
	s, err := dialSession(p.ctx, p.id, r.rm.addr, r.rm.pub, p.origin)
	if err != nil {
		return err
	}
	p.relay = s
	p.mu.Lock()
	p.sessions = append(p.sessions, s)
	p.mu.Unlock()
	ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
	defer cancel()
	_, err = s.PublishNamespace(ctx, p.ns)
	return err
}

func (r *scaleRoom) keyLink(v, s *scaleParty) error {
	sess, err := dialSession(v.ctx, v.id, s.addr, s.pub, nil)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(v.ctx)
	l := &keyLink{sess: sess, cancel: cancel, done: make(chan struct{})}
	v.mu.Lock()
	v.links[s] = l
	v.mu.Unlock()
	go func() {
		defer close(l.done)
		err := v.keys.Receive(ctx, sess, v.pub, s.ns)
		if err != nil && ctx.Err() == nil && !v.gone.Load() && !s.gone.Load() {
			r.t.Errorf("receive keys of %x by %x: %v", s.pub[:4], v.pub[:4], err)
		}
	}()
	return nil
}

func (r *scaleRoom) startWatch(v, s *scaleParty) error {
	ctx, cancel := context.WithCancel(r.ctx)
	timer := time.AfterFunc(60*time.Second, cancel)
	w, err := Watch(ctx, v.relay, s.ns, WatchOptions{Capabilities: avCaps(), Keys: v.keys})
	timer.Stop()
	if err != nil {
		return err
	}
	pw := &pairWatch{v: v, s: s, w: w}
	r.mu.Lock()
	r.pairs[[2]*scaleParty{v, s}] = pw
	r.mu.Unlock()
	for _, k := range []Kind{Audio, Video} {
		pw.wg.Go(func() { r.read(pw, k) })
	}
	return nil
}

// link connects v to s: key session, key, then the watch.
func (r *scaleRoom) link(v, s *scaleParty) {
	if err := r.keyLink(v, s); err != nil {
		r.t.Errorf("key session %x to %x: %v", v.pub[:4], s.pub[:4], err)
		return
	}
	if err := r.startWatch(v, s); err != nil {
		r.t.Errorf("watch of %x by %x: %v", s.pub[:4], v.pub[:4], err)
	}
}

func (r *scaleRoom) pair(v, s *scaleParty) *pairWatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pairs[[2]*scaleParty{v, s}]
}

func (r *scaleRoom) read(pw *pairWatch, kind Kind) {
	var last uint64
	started := false
	for {
		f, _, err := pw.w.Next(r.ctx, kind)
		if err != nil {
			return
		}
		now := int64(r.elapsed())
		if len(f.Data) < 32 || !bytes.Equal(f.Data[16:32], pw.s.magic[:]) {
			r.t.Errorf("%s frame of %x at %x does not carry its sender's data", kind, pw.s.pub[:4], pw.v.pub[:4])
			return
		}
		seq := binary.BigEndian.Uint64(f.Data)
		capture := int64(binary.BigEndian.Uint64(f.Data[8:]))
		if cur := pw.lastCap[kind].Load(); capture > cur {
			pw.lastCap[kind].Store(capture)
		}
		if kind == Video {
			pw.firstVideo.CompareAndSwap(0, now)
			if m := pw.markAt.Load(); m > 0 && capture >= m {
				pw.markPlay.CompareAndSwap(0, now)
			}
		}
		if sc := r.sc.Load(); sc != nil && !pw.muted.Load() {
			sc.played[kind].Add(1)
			if started && seq > last+1 {
				gap := int64(seq - last - 1)
				sc.lost[kind].Add(gap)
				for cur := sc.maxGap[kind].Load(); gap > cur && !sc.maxGap[kind].CompareAndSwap(cur, gap); cur = sc.maxGap[kind].Load() {
				}
			}
		}
		if started && seq <= last {
			r.reordered.Add(1)
			if r.strictOrder {
				r.t.Errorf("%s frame %d of %x at %x after %d", kind, seq, pw.s.pub[:4], pw.v.pub[:4], last)
			}
		}
		last, started = seq, true
	}
}

func (r *scaleRoom) startAudio(p *scaleParty) error {
	tp, err := p.b.AddTrack(audioTrack(), PackagerOptions{})
	if err != nil {
		return err
	}
	src := &scaleSource{stop: make(chan struct{})}
	src.wg.Go(func() {
		tick := time.NewTicker(scaleAudioEvery)
		defer tick.Stop()
		var last uint64
		for seq := uint64(0); ; seq++ {
			now := time.Now()
			ts, _ := r.clock.At(now, 48000)
			ts = max(ts, last+1)
			last = ts
			err := tp.WriteFrame(Frame{Timestamp: ts, Keyframe: true, Data: scaleFrame(scaleAudioBytes, seq, now.Sub(r.clock.Start()), &p.magic)})
			if isClosed(err) {
				return
			} else if err != nil {
				r.t.Errorf("audio of %x: %v", p.pub[:4], err)
				return
			}
			select {
			case <-src.stop:
				return
			case <-tick.C:
			}
		}
	})
	p.audio = src
	return nil
}

func (r *scaleRoom) startVideo(p *scaleParty) error {
	tp, err := p.b.AddTrack(videoTrack(), PackagerOptions{})
	if err != nil {
		return err
	}
	src := &scaleSource{stop: make(chan struct{})}
	src.wg.Go(func() {
		tick := time.NewTicker(scaleVideoEvery)
		defer tick.Stop()
		kid, sinceKey := p.sender.KeyID(), scaleGOP
		var last uint64
		for {
			now := time.Now()
			ts, _ := r.clock.At(now, 90000)
			ts = max(ts, last+1)
			last = ts
			key := sinceKey >= scaleGOP || p.sender.KeyID() != kid
			kid = p.sender.KeyID()
			size := scaleVideoDelta
			if key {
				size, sinceKey = scaleVideoKey, 0
			}
			sinceKey++
			seq := p.vseq.Add(1) - 1
			err := tp.WriteFrame(Frame{Timestamp: ts, Keyframe: key, Data: scaleFrame(size, seq, now.Sub(r.clock.Start()), &p.magic)})
			if isClosed(err) {
				return
			} else if err != nil {
				r.t.Errorf("video of %x: %v", p.pub[:4], err)
				return
			}
			select {
			case <-src.stop:
				return
			case <-tick.C:
			}
		}
	})
	p.video = src
	return nil
}

type memberSet struct {
	s       *scaleParty
	members []*scaleParty
}

// probe is an object sealed under a sender's new key, and the time just before the rotation that
// made the key; a member opens it as soon as its Keyring holds the key.
type probe struct {
	s   *scaleParty
	t0  time.Time
	ret time.Duration // since the clock start, when SetMembers returned
	obj kolomoq.Object
	kid uint64
}

func (r *scaleRoom) sealProbe(s *scaleParty, t0 time.Time) (probe, error) {
	g := r.probes.Add(1)
	ct, props, err := s.sender.Seal("probe", g, 0, 0, []byte("probe"), nil)
	if err != nil {
		return probe{}, err
	}
	kid, _ := sealedKID(props)
	return probe{s: s, t0: t0, ret: r.elapsed(), kid: kid, obj: kolomoq.Object{Group: g, Payload: ct, Properties: props}}, nil
}

// rotate applies each member set at once, as every participant of a real call would, and returns
// the probe of each sender.
func (r *scaleRoom) rotate(specs []memberSet) []probe {
	out := make([]probe, len(specs))
	var wg sync.WaitGroup
	for i, sp := range specs {
		wg.Go(func() {
			t0 := time.Now()
			if err := sp.s.sender.SetMembers(pubs(sp.members)); err != nil {
				r.t.Errorf("SetMembers of %x: %v", sp.s.pub[:4], err)
				return
			}
			pr, err := r.sealProbe(sp.s, t0)
			if err != nil {
				r.t.Errorf("probe of %x: %v", sp.s.pub[:4], err)
				return
			}
			out[i] = pr
		})
	}
	wg.Wait()
	r.check()
	return out
}

// waitKey returns how long after pr.t0 v could open the probe, that is, held the sender's new key.
func (r *scaleRoom) waitKey(v *scaleParty, pr probe) (time.Duration, bool) {
	deadline := time.After(30 * time.Second)
	for {
		changed := v.keys.Changed()
		_, _, err := openOnce(v.keys, v.relay, pr.s.ns, "probe", pr.obj)
		switch {
		case err == nil:
			return time.Since(pr.t0), true
		case !errors.Is(err, secobj.ErrNoKey):
			r.t.Errorf("%x opening the probe of %x: %v", v.pub[:4], pr.s.pub[:4], err)
			return 0, false
		}
		select {
		case <-changed:
		case <-deadline:
			r.t.Errorf("%x did not get the key of %x in 30 s", v.pub[:4], pr.s.pub[:4])
			return 0, false
		case <-r.ctx.Done():
			return 0, false
		}
	}
}

// measure waits, for every probe, until each receiver holds the key and records the latencies in dst.
func (r *scaleRoom) measure(probes []probe, receivers func(s *scaleParty) []*scaleParty, dst func(s, v *scaleParty) *[]time.Duration) {
	sc := r.sc.Load()
	var wg sync.WaitGroup
	for _, pr := range probes {
		for _, v := range receivers(pr.s) {
			wg.Go(func() {
				if d, ok := r.waitKey(v, pr); ok {
					sc.add(dst(pr.s, v), d)
				}
			})
		}
	}
	wg.Wait()
	sc.mu.Lock()
	sc.events++
	sc.mu.Unlock()
}

// sealedKID returns the Key ID in the visible Immutable Properties of a protected object.
func sealedKID(props []kolomoq.Property) (uint64, bool) {
	for _, p := range props {
		if p.Type != 0x0B || len(p.Bytes) < 2 || p.Bytes[0] != secobj.PropertyKeyID {
			continue
		}
		switch b := p.Bytes[1:]; {
		case b[0] < 0x80:
			return uint64(b[0]), true
		case b[0] < 0xC0 && len(b) >= 2:
			return uint64(b[0]&0x3F)<<8 | uint64(b[1]), true
		}
	}
	return 0, false
}

// setup starts the call: every participant gives its key to all others, publishes and watches all others.
func (r *scaleRoom) setup(n int) {
	r.begin("setup")
	parties := make([]*scaleParty, n)
	for i := range parties {
		parties[i] = r.newParty()
	}
	par(n, func(i int) {
		if err := r.joinRelay(parties[i]); err != nil {
			r.t.Errorf("join the relay: %v", err)
		}
	})
	r.check()
	r.mu.Lock()
	r.parties = parties
	r.mu.Unlock()
	specs := make([]memberSet, n)
	for i, p := range parties {
		specs[i] = memberSet{p, without(parties, p)}
	}
	probes := r.rotate(specs)
	for _, p := range parties {
		if err := r.startAudio(p); err != nil {
			r.t.Fatal(err)
		}
		if err := r.startVideo(p); err != nil {
			r.t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		r.measure(probes, func(s *scaleParty) []*scaleParty { return without(parties, s) },
			func(_, _ *scaleParty) *[]time.Duration { return &r.sc.Load().joinLat })
	})
	par(n*n, func(k int) {
		if v, s := parties[k/n], parties[k%n]; v != s {
			r.link(v, s)
		}
	})
	wg.Wait()
	r.check()
	for _, p := range parties {
		r.spy.watch(p)
	}
	r.awaitFlowing(parties, parties)
	r.end()
}

func (r *scaleRoom) awaitFlowing(viewers, senders []*scaleParty) {
	r.await("video of every sender at every viewer", 60*time.Second, func() bool {
		for _, v := range viewers {
			for _, s := range senders {
				if v == s {
					continue
				}
				if pw := r.pair(v, s); pw == nil || pw.firstVideo.Load() == 0 {
					return false
				}
			}
		}
		return true
	})
}

// join adds a participant to the call. Time runs from the first step of the joiner (it connects to
// the relay) to its first video frame from each sender and each sender's first from it.
func (r *scaleRoom) join() *scaleParty {
	p := r.newParty()
	t0 := r.elapsed()
	if err := r.joinRelay(p); err != nil {
		r.t.Fatalf("join the relay: %v", err)
	}
	existing := r.snapshot()
	old := r.spy.snapshot()
	all := append(slices.Clone(existing), p)
	specs := []memberSet{{p, existing}}
	for _, e := range existing {
		specs = append(specs, memberSet{e, without(all, e)})
	}
	probes := r.rotate(specs)
	r.mu.Lock()
	r.parties = all
	r.mu.Unlock()
	if err := r.startAudio(p); err != nil {
		r.t.Fatal(err)
	}
	if err := r.startVideo(p); err != nil {
		r.t.Fatal(err)
	}
	r.spy.watch(p)

	sc := r.sc.Load()
	var wg sync.WaitGroup
	wg.Go(func() {
		r.measure(probes, func(s *scaleParty) []*scaleParty { return without(all, s) },
			func(s, v *scaleParty) *[]time.Duration {
				if s == p || v == p {
					return &sc.joinLat
				}
				return &sc.keyLat
			})
	})
	par(2*len(existing), func(k int) {
		if e := existing[k/2]; k%2 == 0 {
			r.link(e, p)
		} else {
			r.link(p, e)
		}
	})
	wg.Wait()
	r.check()
	r.awaitFlowing(all, all)
	r.check()
	r.mu.Lock()
	for _, e := range existing {
		pw := r.pairs[[2]*scaleParty{p, e}]
		st := pw.w.Stats()
		pw.oldNoKey.Store(st.Audio.NoKey + st.Video.NoKey)
		pw.oldRetired.Store(st.Audio.Retired + st.Video.Retired)
		pw.oldBadCat.Store(st.BadCatalogs)
		r.folded.old += st.Audio.NoKey + st.Video.NoKey + st.Audio.Retired + st.Video.Retired + st.BadCatalogs
	}
	r.mu.Unlock()
	for _, e := range existing {
		sc.add(&sc.firstJ, time.Duration(r.pair(p, e).firstVideo.Load()-int64(t0)))
		sc.add(&sc.first, time.Duration(r.pair(e, p).firstVideo.Load()-int64(t0)))
	}
	r.auditJoiner(p, existing, old)
	return p
}

// auditJoiner checks that a joiner opens nothing sealed before it joined: the objects the relay
// carried before the join, and what the relay serves from its cache to a FETCH.
func (r *scaleRoom) auditJoiner(p *scaleParty, existing []*scaleParty, old map[string][]kolomoq.Object) {
	checked := 0
	for _, e := range existing {
		kid := e.sender.KeyID()
		for _, track := range []string{CatalogTrack, "mic", "cam"} {
			for _, o := range old[ringKey(e, track)] {
				if k, _ := sealedKID(o.Properties); k >= kid {
					continue
				}
				checked++
				if _, _, err := openOnce(p.keys, p.relay, e.ns, track, o); err == nil {
					r.t.Errorf("joiner opened a %s object of %x sealed before it joined", track, e.pub[:4])
				}
			}
		}
	}
	if checked == 0 {
		r.t.Error("no object sealed before the join to try")
	}
	for _, e := range existing[:min(4, len(existing))] {
		for _, track := range []string{"mic", "cam"} {
			ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
			f, err := p.relay.Fetch(ctx, e.ns, track, &kolomoq.FetchOptions{RecentGroups: 16})
			if err != nil {
				cancel()
				r.t.Errorf("fetch %s of %x: %v", track, e.pub[:4], err)
				continue
			}
			kid := e.sender.KeyID()
			for {
				o, err := f.ReadObject(ctx)
				if err != nil {
					break
				}
				// Only objects sealed before the join are tried.
				if k, _ := sealedKID(o.Properties); o.Status == kolomoq.StatusNormal && k < kid {
					if _, _, err := openOnce(p.keys, p.relay, e.ns, track, o); err == nil {
						r.t.Errorf("joiner opened a fetched %s object of %x sealed before it joined", track, e.pub[:4])
					}
				}
			}
			f.Close()
			cancel()
		}
	}
}

// remove tells every other participant to stop giving its key to x, which stays connected to the
// relay and keeps trying, and checks that it reads nothing sealed after the rotation. Then x leaves.
func (r *scaleRoom) remove(x *scaleParty, observe time.Duration) {
	r.mu.Lock()
	for k, pw := range r.pairs {
		if k[0] == x {
			r.muteLocked(pw)
		}
	}
	r.mu.Unlock()
	remaining := without(r.snapshot(), x)
	specs := make([]memberSet, len(remaining))
	for i, e := range remaining {
		specs[i] = memberSet{e, without(remaining, e)}
	}
	probes := r.rotate(specs)
	for _, pr := range probes {
		if _, _, err := openOnce(x.keys, x.relay, pr.s.ns, "probe", pr.obj); !errors.Is(err, secobj.ErrNoKey) {
			r.t.Errorf("removed member opened an object sealed after its removal: %v", err)
		}
	}
	r.measure(probes, func(s *scaleParty) []*scaleParty { return without(remaining, s) },
		func(_, _ *scaleParty) *[]time.Duration { return &r.sc.Load().keyLat })
	time.Sleep(observe)
	// A loaded machine can leave the relay's view behind: wait until it holds what the rotation sealed.
	r.await("objects under the new keys at the relay", 20*time.Second, func() bool {
		for _, pr := range probes {
			found := false
			for _, o := range r.spy.snapshotOf(pr.s, "mic") {
				if k, _ := sealedKID(o.Properties); k >= pr.kid {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	})

	// What the relay carries after the rotation is sealed under the new key, which x never got.
	tried := 0
	for _, pr := range probes {
		for _, track := range []string{"mic", "cam"} {
			for _, o := range r.spy.snapshotOf(pr.s, track) {
				if k, _ := sealedKID(o.Properties); k < pr.kid {
					continue
				}
				tried++
				if _, _, err := openOnce(x.keys, x.relay, pr.s.ns, track, o); !errors.Is(err, secobj.ErrNoKey) {
					r.t.Errorf("removed member opened a %s object of %x sealed after the removal: %v", track, pr.s.pub[:4], err)
				}
			}
		}
		// A frame captured after SetMembers returned is sealed under the new key, so it never plays.
		pw := r.pair(x, pr.s)
		if pw == nil {
			continue
		}
		for _, k := range []Kind{Audio, Video} {
			if c := pw.lastCap[k].Load(); c >= int64(pr.ret) {
				r.t.Errorf("removed member played %s captured %v after the rotation", k, time.Duration(c-int64(pr.ret)))
			}
		}
	}
	if tried == 0 {
		r.t.Error("no object sealed after the removal to try")
	}
	r.drop(x)
}

// toggle turns the camera of x off, then on again after offFor: the new track restarts its group
// IDs, so the Sender rotates.
func (r *scaleRoom) toggle(x *scaleParty, offFor time.Duration) {
	others := without(r.snapshot(), x)
	kid := x.sender.KeyID()
	x.video.Stop()
	if err := x.b.RemoveTrack("cam"); err != nil {
		r.t.Fatal(err)
	}
	time.Sleep(offFor)
	t0 := time.Now()
	t0e := int64(r.elapsed())
	for _, v := range others {
		r.pair(v, x).markAt.Store(t0e)
	}
	if err := r.startVideo(x); err != nil {
		r.t.Fatal(err)
	}
	if !r.await("the camera toggle to rotate the key", 10*time.Second, func() bool { return x.sender.KeyID() > kid }) {
		r.check()
	}
	pr, err := r.sealProbe(x, t0)
	if err != nil {
		r.t.Fatal(err)
	}
	r.measure([]probe{pr}, func(*scaleParty) []*scaleParty { return others },
		func(_, _ *scaleParty) *[]time.Duration { return &r.sc.Load().keyLat })
	sc := r.sc.Load()
	if !r.await("video to resume at every viewer", 30*time.Second, func() bool {
		for _, v := range others {
			if r.pair(v, x).markPlay.Load() == 0 {
				return false
			}
		}
		return true
	}) {
		for _, v := range others {
			if pw := r.pair(v, x); pw.markPlay.Load() == 0 {
				st := pw.w.Stats()
				r.t.Logf("no video at %x from %x: catalog %d (sender %d), kid %d, video %+v", v.pub[:4], x.pub[:4], st.Catalog, x.b.Catalog().Version, x.sender.KeyID(), st.Video)
			}
		}
	}
	for _, v := range others {
		sc.add(&sc.resume, time.Duration(r.pair(v, x).markPlay.Load()-t0e))
	}
}

// drop ends a participant: its sources, watches, sessions and keys, and the keys others hold of it.
func (r *scaleRoom) drop(p *scaleParty) {
	p.gone.Store(true)
	r.mu.Lock()
	r.parties = without(r.parties, p)
	var pws []*pairWatch
	for k, pw := range r.pairs {
		if k[0] == p || k[1] == p {
			pws = append(pws, pw)
			r.muteLocked(pw)
			delete(r.pairs, k)
		}
	}
	r.mu.Unlock()
	for _, src := range []*scaleSource{p.audio, p.video} {
		if src != nil {
			src.Stop()
		}
	}
	for _, pw := range pws {
		r.closePair(pw)
	}
	r.rm.mu.Lock()
	delete(r.rm.members, string(p.pub))
	r.rm.mu.Unlock()
	var links []*keyLink
	for _, v := range r.snapshot() {
		v.mu.Lock()
		if l := v.links[p]; l != nil {
			links = append(links, l)
			delete(v.links, p)
		}
		v.mu.Unlock()
		v.keys.Remove(p.ns)
	}
	p.cancel()
	p.mu.Lock()
	for _, l := range p.links {
		links = append(links, l)
	}
	p.links = nil
	sessions := p.sessions
	p.mu.Unlock()
	for _, l := range links {
		l.cancel()
		l.sess.Close()
		<-l.done
	}
	for _, s := range sessions {
		s.Close()
	}
	p.b.Close()
	p.sender.Close()
}

// muteLocked stops counting a pair's frames and moves what its watch counted so far to r.folded, so
// the totals never go down.
func (r *scaleRoom) muteLocked(pw *pairWatch) {
	if !pw.muted.Swap(true) {
		r.folded = r.folded.plus(pw.counted())
	}
}

func (r *scaleRoom) closePair(pw *pairWatch) {
	pw.w.Close()
	pw.wg.Wait()
}

func (r *scaleRoom) shutdown() {
	for _, p := range r.snapshot() {
		r.drop(p)
	}
	r.spy.close()
	r.cancel()
}

func ringKey(p *scaleParty, track string) string { return fmt.Sprintf("%x/%s", p.pub, track) }

// scaleSpy is the relay operator's view: a room member that holds no key and reads every track of
// every participant. It checks that nothing it sees is plain, and keeps the latest objects.
type scaleSpy struct {
	r       *scaleRoom
	node    *secNode
	sess    *kolomoq.Session
	wg      sync.WaitGroup
	checked atomic.Int64

	mu   sync.Mutex
	ring map[string][]kolomoq.Object
}

func newScaleSpy(r *scaleRoom) *scaleSpy {
	sp := &scaleSpy{r: r, ring: map[string][]kolomoq.Object{}}
	sp.node = newSecNode(r.t)
	r.rm.mu.Lock()
	r.rm.members[string(sp.node.pub)] = true
	r.rm.mu.Unlock()
	var err error
	if sp.sess, err = dialSession(r.ctx, sp.node.id, r.rm.addr, r.rm.pub, nil); err != nil {
		r.t.Fatal(err)
	}
	return sp
}

func (sp *scaleSpy) close() {
	sp.sess.Close()
	sp.wg.Wait()
}

func (sp *scaleSpy) snapshot() map[string][]kolomoq.Object {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	out := make(map[string][]kolomoq.Object, len(sp.ring))
	for k, v := range sp.ring {
		out[k] = slices.Clone(v)
	}
	return out
}

func (sp *scaleSpy) snapshotOf(p *scaleParty, track string) []kolomoq.Object {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return slices.Clone(sp.ring[ringKey(p, track)])
}

// watch reads the three tracks of p; a track that ends and comes back (a camera toggle) is read again.
func (sp *scaleSpy) watch(p *scaleParty) {
	keep := map[string]int{CatalogTrack: 2, "mic": 25, "cam": 4}
	for _, track := range []string{CatalogTrack, "mic", "cam"} {
		sp.wg.Go(func() {
			join := kolomoq.JoinCurrentGroup
			if track == "mic" {
				join = kolomoq.JoinNextObject
			}
			for !p.gone.Load() && sp.r.ctx.Err() == nil {
				sub, err := sp.sess.Subscribe(sp.r.ctx, p.ns, track, &kolomoq.SubscribeOptions{Join: join})
				if err != nil {
					select {
					case <-sp.sess.Done():
						return
					default:
					}
					time.Sleep(50 * time.Millisecond)
					continue
				}
				for {
					o, err := sub.ReadObject(sp.r.ctx)
					if err != nil {
						break
					}
					if o.Status != kolomoq.StatusNormal {
						continue
					}
					sp.inspect(p, track, o)
					sp.mu.Lock()
					k := ringKey(p, track)
					sp.ring[k] = append(sp.ring[k], o)
					if len(sp.ring[k]) > keep[track] {
						sp.ring[k] = sp.ring[k][1:]
					}
					sp.mu.Unlock()
				}
				sub.Close()
			}
		})
	}
}

// inspect fails the test if a relay-visible object shows anything of the media.
func (sp *scaleSpy) inspect(p *scaleParty, track string, o kolomoq.Object) {
	sp.checked.Add(1)
	t := sp.r.t
	if _, ok := sealedKID(o.Properties); !ok || len(o.Properties) != 1 {
		t.Errorf("%s of %x: visible properties %+v", track, p.pub[:4], o.Properties)
	}
	if bytes.Contains(o.Payload, p.magic[:]) {
		t.Errorf("%s of %x: the relay sees plaintext media", track, p.pub[:4])
	}
	if track == CatalogTrack {
		if _, err := ParseCatalog(o.Payload, CatalogLimits{}); err == nil {
			t.Errorf("catalog of %x is readable at the relay", p.pub[:4])
		}
	}
}

// refusesKeys checks that no key track of p is reachable through the relay.
func (sp *scaleSpy) refusesKeys(p *scaleParty) {
	ctx, cancel := context.WithTimeout(sp.r.ctx, 10*time.Second)
	defer cancel()
	for _, member := range []ed25519.PublicKey{sp.node.pub, p.pub} {
		if sub, err := sp.sess.Subscribe(ctx, secobj.KeyNamespace(p.ns), secobj.KeyTrack(member), nil); err == nil {
			sub.Close()
			sp.r.t.Errorf("the relay serves the key track of %x", member[:4])
		}
	}
}

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	return s[max(int(math.Ceil(p*float64(len(s))))-1, 0)]
}

func msec(d time.Duration) string { return fmt.Sprintf("%.1f", float64(d)/float64(time.Millisecond)) }

func dist(d []time.Duration) string {
	if len(d) == 0 {
		return "-"
	}
	return fmt.Sprintf("%s / %s / %s (n=%d)", msec(pct(d, 0.5)), msec(pct(d, 0.95)), msec(pct(d, 1)), len(d))
}

func busyCPU() float64 {
	s := []metrics.Sample{{Name: "/cpu/classes/total:cpu-seconds"}, {Name: "/cpu/classes/idle:cpu-seconds"}}
	metrics.Read(s)
	return s[0].Value.Float64() - s[1].Value.Float64()
}

func heapAfterGC() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func (r *scaleRoom) report(t *testing.T, n int, extra string) {
	var b strings.Builder
	fmt.Fprintf(&b, "protected group call, %d participants, one relay with RoomPolicy, loopback, %d CPUs, race %v\n", n, runtime.NumCPU(), raceBuild())
	fmt.Fprintf(&b, "%-14s %7s  %-34s %-34s %-24s %-24s %s\n", "scenario", "seconds", "key delivery ms p50/p95/max", "joiner key ms (session set-up)", "audio lost (of)", "video lost (of)", "longest gap a/v | NoKey, retired, unauth, bad catalog | joiner old-key drops")
	r.mu.Lock()
	done := slices.Clone(r.done)
	r.mu.Unlock()
	for _, sc := range done {
		d := sc.st1.sub(sc.st0)
		fmt.Fprintf(&b, "%-14s %7.1f  %-34s %-34s %-24s %-24s %d/%d | %d, %d, %d, %d | %d\n", sc.name, sc.end.Sub(sc.start).Seconds(),
			dist(sc.keyLat), dist(sc.joinLat),
			fmt.Sprintf("%d (%d)", sc.lost[Audio].Load(), sc.played[Audio].Load()+sc.lost[Audio].Load()),
			fmt.Sprintf("%d (%d)", sc.lost[Video].Load(), sc.played[Video].Load()+sc.lost[Video].Load()),
			sc.maxGap[Audio].Load(), sc.maxGap[Video].Load(), d.noKey, d.retired, d.unauth, d.badCat, d.old)
	}
	for _, sc := range done {
		if len(sc.first)+len(sc.firstJ)+len(sc.resume) == 0 {
			continue
		}
		fmt.Fprintf(&b, "%s: first video frame ms p50/p95/max: joiner from each sender %s; others from the joiner %s; video resumes after a camera toggle %s\n",
			sc.name, dist(sc.firstJ), dist(sc.first), dist(sc.resume))
	}
	b.WriteString(extra)
	t.Logf("\n%s", b.String())
}

// scaleLatency collects the key delivery latencies of the rotations a call made after its set-up.
func (r *scaleRoom) scaleLatency() (member, joiner []time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sc := range r.done {
		if sc.name == "setup" {
			continue
		}
		member = append(member, sc.keyLat...)
		joiner = append(joiner, sc.joinLat...)
	}
	return member, joiner
}

func TestScaleSecureGroup(t *testing.T) {
	for _, n := range []int{4, 8, 20} {
		t.Run(fmt.Sprintf("%d", n), func(t *testing.T) {
			if n == 20 && testing.Short() {
				t.Skip("the 20-participant call is heavy: 15000 goroutines, 380 pairwise sessions")
			}
			if n == 20 && raceBuild() {
				t.Skip("with the race detector the 20-participant media load saturates every CPU and the call loses most video, so nothing it measures is meaningful; 4 and 8 run the same code under -race")
			}
			runScaleCall(t, n)
		})
	}
}

func runScaleCall(t *testing.T, n int) {
	var before, peak int
	before = runtime.NumGoroutine()
	var after int
	t.Cleanup(func() {
		t.Logf("goroutines: %d before, %d at the peak, %d after the participants left, %d after teardown", before, peak, after, runtime.NumGoroutine())
	})
	checkGoroutines(t)
	heap0 := heapAfterGC()
	r := newScaleRoom(t)
	r.strictOrder = n <= 8
	defer r.shutdown()
	r.setup(n)
	peak = runtime.NumGoroutine()

	// Steady state: nothing changes; the baseline of loss and CPU.
	time.Sleep(time.Second)
	heap1 := heapAfterGC()
	r.begin("steady")
	cpu0, wall0 := busyCPU(), time.Now()
	time.Sleep(6 * time.Second)
	cpu, wall := busyCPU()-cpu0, time.Since(wall0)
	sc := r.end()
	r.spy.refusesKeys(r.snapshot()[0])
	st := sc.st1.sub(sc.st0)
	if st.unauth != 0 || st.noKey != 0 || st.retired != 0 || st.badCat != 0 {
		t.Errorf("steady state: NoKey %d, Retired %d, Unauthenticated %d, bad catalogs %d", st.noKey, st.retired, st.unauth, st.badCat)
	}
	frames := sc.played[Audio].Load() + sc.played[Video].Load()
	extra := fmt.Sprintf("steady state: %.2f cores busy for the whole call, %.0f us of CPU per played frame (sealing, relaying, opening, playout; relay and %d participants in one process); heap %.1f MiB per participant after GC, %d MiB for %d\n",
		cpu/wall.Seconds(), 1e6*cpu/float64(frames), n, float64(heap1-heap0)/float64(n)/(1<<20), (heap1-heap0)>>20, n)

	// A participant leaves: removal, rotation, and the removed member reads nothing more.
	r.begin("leave")
	parties := r.snapshot()
	r.remove(parties[n/2], 1500*time.Millisecond)
	time.Sleep(time.Second)
	r.end()

	// A participant joins: rotation on join, the joiner sees only future media.
	r.begin("join")
	r.join()
	time.Sleep(3 * time.Second)
	r.end()

	// Camera toggles: the re-created video track rotates the key.
	r.begin("camera toggle")
	parties = r.snapshot()
	for _, i := range []int{0, n / 2, n - 1} {
		r.toggle(parties[i], 0)
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(time.Second)
	r.end()

	// Rapid churn: one leaves and one joins, ten times.
	r.begin("churn x10")
	for range 10 {
		r.remove(r.snapshot()[1], 300*time.Millisecond)
		r.join()
	}
	time.Sleep(2 * time.Second)
	r.end()

	member, joiner := r.scaleLatency()
	if r.spy.checked.Load() == 0 {
		t.Error("the relay's view checked no object")
	}
	extra += fmt.Sprintf("all rotations after set-up, key delivery ms p50/p95/max: members with a session %s; new members, session set-up included %s\n", dist(member), dist(joiner))
	r.report(t, n, extra+fmt.Sprintf("relay view: %d objects checked, none plain, key tracks refused; frames played out of order: %d\n", r.spy.checked.Load(), r.reordered.Load()))

	// The ADR gives 400 ms as the hold limit for every call size. The race detector slows everything
	// by a large factor, so the bound doubles there.
	bound := scaleKeyBound
	if raceBuild() {
		bound *= 2
	}
	if p := pct(member, 0.95); len(member) == 0 || p > bound {
		t.Errorf("key delivery p95 %v over the bound %v (n=%d)", p, bound, len(member))
	}
	if p := pct(joiner, 0.95); p > 5*time.Second {
		t.Errorf("key delivery to a new member p95 %v over 5 s", p)
	}
	var unauth, noKey uint64
	r.mu.Lock()
	for _, sc := range r.done {
		d := sc.st1.sub(sc.st0)
		unauth += d.unauth
		noKey += d.noKey
		if len(sc.first) > 0 && pct(sc.first, 0.95) > 5*time.Second || len(sc.firstJ) > 0 && pct(sc.firstJ, 0.95) > 5*time.Second {
			t.Errorf("%s: the first video frame after a join took over 5 s", sc.name)
		}
	}
	r.mu.Unlock()
	if unauth != 0 {
		t.Errorf("%d objects failed authentication in an honest call", unauth)
	}
	// Every key arrived within the hold limit, so no frame waited for one beyond it. Retired objects
	// show a relayed path slower than Retain, as on a saturated machine; the probes start Retain early.
	if pct(member, 1) < scaleKeyBound && noKey != 0 {
		t.Errorf("%d frames were dropped for want of a key although every key arrived within %v", noKey, scaleKeyBound)
	}

	r.shutdown()
	after = runtime.NumGoroutine()
}
