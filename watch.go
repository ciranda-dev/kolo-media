package kolomedia

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sync"
	"time"

	kolomoq "github.com/ciranda-dev/kolo-moq"
	"github.com/ciranda-dev/kolo-moq/secobj"
)

// WatchOptions configures Watch.
type WatchOptions struct {
	// Capabilities chooses the tracks to play, as in Select; with no codecs nothing plays.
	Capabilities Capabilities
	// Playout configures the playout; zero fields take their defaults.
	Playout PlayoutConfig
	// CatalogLimits bounds each received catalog; zero fields take their defaults.
	CatalogLimits CatalogLimits
	// Keys, if set, opens every object with its sender's key, drops objects without one, and
	// refuses a session that neither owns the namespace nor is a listed relay. Each subscription
	// opens through its own secobj.Opener, so several watches can share one Keyring.
	Keys *secobj.Keyring
	// RequestKeyframes asks the publisher for a new group, which starts with a keyframe, when the video track
	// waits for one, lost the rest of a group, or gets no frame for 500 ms. Requests are bounded: a request for
	// silence doubles the silence the next needs, and an unanswered one is repeated after 1, 2, 4 and 8 s, then
	// dropped until a frame plays. The publisher must serve it (TrackPublisher.KeyframeRequests). Off by default.
	RequestKeyframes bool
}

// WatchStats is a snapshot of a Watcher.
type WatchStats struct {
	// Playout holds the playout's counters, with the tracks named "audio" and "video".
	Playout PlayoutStats
	// Catalog is the version of the catalog in use; BadCatalogs counts catalog objects that were ignored.
	Catalog     uint64
	BadCatalogs uint64
	Audio       TrackWatchStats
	Video       TrackWatchStats
}

// TrackWatchStats describes the subscription of one kind.
type TrackWatchStats struct {
	// Track is the name of the track played, empty if none.
	Track string
	// Subscriptions counts the subscriptions started for this kind, switches and restarts included.
	Subscriptions uint64
	// Gaps counts video groups cut short because an object was missing or malformed.
	Gaps uint64
	// Malformed counts objects that failed Unpack or did not fit the playout.
	Malformed uint64
	// Dropped counts groups the transport dropped because the reader fell behind.
	Dropped uint64
	// NoKey counts objects dropped because their key did not arrive in time.
	NoKey uint64
	// Retired counts objects under a key already retired: later than Retain after a newer key, or
	// under a Key ID older than the keys held.
	Retired uint64
	// Unauthenticated counts objects that failed authentication, were not protected or were replays.
	Unauthenticated uint64
	// KeyframeRequests counts the requests for a new group sent for this kind.
	KeyframeRequests uint64
	// Err is why the last subscription ended, or nil while it runs. Once it ended, Next for its kind
	// blocks until a catalog update lists the track again or selects another, unless the catalog in
	// use still lists it: then it subscribes again at once, under the same catalog only once.
	Err error
}

// Watcher plays one broadcast received over a MoQ session. Its methods are safe for concurrent use.
type Watcher struct {
	s    *kolomoq.Session
	ns   kolomoq.Namespace
	opts WatchOptions
	play *Playout

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	once    sync.Once
	catSub  *kolomoq.Subscription
	catOpen *opener

	// applying serializes apply, resume and the final stop, which change kinds.
	applying sync.Mutex

	keyNeed chan struct{} // the video track needs a keyframe; the requester of keyframes reads it

	mu      sync.Mutex
	err     error
	catalog *Catalog
	kinds   [2]*watched
	stats   [2]TrackWatchStats
	bad     uint64
}

type watched struct {
	track  Track
	since  uint64 // the catalog version the subscription started under
	again  bool   // a second subscription started under the same catalog version
	sub    *kolomoq.Subscription
	op     *secobj.Opener // the subscription's own, under keys
	cancel context.CancelFunc
	done   chan struct{}
}

// Watch subscribes to the catalog of the broadcast under ns, plays the tracks Select picks and
// follows catalog updates. It returns once the first catalog arrived, even if it selects nothing yet,
// and fails if the catalog selects tracks and none of their subscriptions starts. The session stays
// the caller's. With opts.Keys, Watch fails with an error wrapping secobj.ErrWrongSession unless the
// session's peer owns ns or is a listed relay, and returns once a catalog that opens arrives.
func Watch(ctx context.Context, s *kolomoq.Session, ns kolomoq.Namespace, opts WatchOptions) (*Watcher, error) {
	var catOp *secobj.Opener
	if opts.Keys != nil {
		var err error
		if catOp, err = opts.Keys.Opener(s, ns, CatalogTrack); err != nil {
			return nil, fmt.Errorf("kolomedia: protected watch: %w", err)
		}
	}
	keyNeed := make(chan struct{}, 1)
	if opts.RequestKeyframes {
		user := opts.Playout.OnKeyframeNeeded
		opts.Playout.OnKeyframeNeeded = func(track string) {
			select {
			case keyNeed <- struct{}{}:
			default:
			}
			if user != nil {
				user(track)
			}
		}
	}
	play, err := newSlotPlayout(opts.Playout, systemClock{})
	if err != nil {
		return nil, err
	}
	catSub, err := s.Subscribe(ctx, ns, CatalogTrack, &kolomoq.SubscribeOptions{Priority: 0, Join: kolomoq.JoinCurrentGroup})
	if err != nil {
		return nil, moqError("subscribe to the catalog", err)
	}
	w := &Watcher{s: s, ns: slices.Clone(ns), opts: opts, play: play, catSub: catSub, keyNeed: keyNeed}
	w.catOpen = catalogOpener(opts.Keys, catOp, func(error) {
		w.mu.Lock()
		w.bad++
		w.mu.Unlock()
	})
	w.opts.Capabilities.Codecs = slices.Clone(opts.Capabilities.Codecs)
	cat, err := w.readCatalog(ctx)
	if err != nil {
		catSub.Close()
		return nil, err
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	if err := w.apply(ctx, cat); err != nil {
		w.cancel()
		play.Close()
		catSub.Close()
		return nil, err
	}
	w.wg.Add(2)
	if opts.RequestKeyframes {
		w.wg.Go(w.requestKeyframes)
	}
	go w.followCatalog()
	go func() {
		defer w.wg.Done()
		select {
		case <-s.Done():
			w.finish(sessionEnded(s))
		case <-w.ctx.Done():
		}
	}()
	return w, nil
}

// Next returns the next frame of the given kind when it is due, and the track it belongs to, whose
// Config the caller must not modify. It blocks while no track of that kind is selected. After the
// watch ended it returns an error wrapping ErrBroadcastEnded or ErrSessionEnded, or ErrClosed.
func (w *Watcher) Next(ctx context.Context, kind Kind) (Frame, Track, error) {
	f, t, err := w.play.next(ctx, kind.String())
	if errors.Is(err, ErrClosed) {
		w.mu.Lock()
		err = w.err
		w.mu.Unlock()
	}
	return f, t, err
}

// Catalog returns a copy of the catalog in use.
func (w *Watcher) Catalog() *Catalog {
	w.mu.Lock()
	defer w.mu.Unlock()
	c := *w.catalog
	c.Tracks = slices.Clone(c.Tracks)
	for i := range c.Tracks {
		c.Tracks[i].Config = slices.Clone(c.Tracks[i].Config)
	}
	return &c
}

// Tracks returns the tracks being played.
func (w *Watcher) Tracks() Selection {
	w.mu.Lock()
	defer w.mu.Unlock()
	var s Selection
	for i, p := range []**Track{&s.Audio, &s.Video} {
		if wt := w.kinds[i]; wt != nil {
			t := wt.track
			t.Config = slices.Clone(t.Config)
			*p = &t
		}
	}
	return s
}

// Stats returns a snapshot of the playout and the subscriptions.
func (w *Watcher) Stats() WatchStats {
	ps := w.play.Stats()
	w.mu.Lock()
	defer w.mu.Unlock()
	s := WatchStats{Playout: ps, Catalog: w.catalog.Version, BadCatalogs: w.bad, Audio: w.stats[0], Video: w.stats[1]}
	for i, p := range []*TrackWatchStats{&s.Audio, &s.Video} {
		if wt := w.kinds[i]; wt != nil {
			p.Dropped += wt.sub.Dropped()
		}
	}
	return s
}

// Close stops every subscription and goroutine of the watch; blocked and later Next calls return ErrClosed.
func (w *Watcher) Close() error {
	w.finish(ErrClosed)
	w.wg.Wait()
	return nil
}

func (w *Watcher) finish(err error) {
	w.once.Do(func() {
		w.mu.Lock()
		w.err = err
		w.mu.Unlock()
		w.cancel()
		w.play.Close()
	})
}

// readCatalog returns the next catalog newer than the one in use.
func (w *Watcher) readCatalog(ctx context.Context) (*Catalog, error) {
	for {
		obj, _, err := w.catOpen.next(ctx, w.catSub)
		if err != nil {
			return nil, endedBy(err, ErrBroadcastEnded, "read the catalog")
		}
		// End of Track is the publisher's own end mark; the subscription may end later.
		if obj.Status == kolomoq.StatusEndOfTrack {
			return nil, ErrBroadcastEnded
		}
		if obj.Status != kolomoq.StatusNormal {
			continue
		}
		c, err := ParseCatalog(obj.Payload, w.opts.CatalogLimits)
		w.mu.Lock()
		cur := w.catalog
		if err == nil && (obj.ID != 0 || c.Version != obj.Group) {
			err = ErrInvalidCatalog
		}
		if err != nil {
			w.bad++
		}
		w.mu.Unlock()
		if err == nil && (cur == nil || c.Version > cur.Version) {
			return c, nil
		}
	}
}

func (w *Watcher) followCatalog() {
	defer w.wg.Done()
	defer w.catSub.Close()
	defer func() {
		w.applying.Lock()
		defer w.applying.Unlock()
		for k := range w.kinds {
			w.stop(k)
		}
	}()
	for {
		c, err := w.readCatalog(w.ctx)
		if err != nil {
			select {
			case <-w.s.Done():
				err = sessionEnded(w.s)
			default:
				if !errors.Is(err, ErrBroadcastEnded) {
					err = fmt.Errorf("%w: %w", ErrBroadcastEnded, err)
				}
			}
			w.finish(err)
			return
		}
		_ = w.apply(w.ctx, c) // a failed subscription is in Stats
	}
}

// apply makes c the catalog in use and subscribes to what it selects. It returns the first
// subscription error if tracks were selected and none could start.
func (w *Watcher) apply(ctx context.Context, c *Catalog) error {
	w.applying.Lock()
	defer w.applying.Unlock()
	var failed error
	sel, _ := Select(c, w.opts.Capabilities)
	for k, want := range []*Track{sel.Audio, sel.Video} {
		cur := w.kinds[k]
		if want != nil && cur != nil && cur.alive() && sameTrack(&cur.track, want) {
			continue
		}
		if err := w.switchTo(ctx, k, want, c.Version); err != nil && failed == nil {
			failed = err
		}
	}
	w.mu.Lock()
	w.catalog = c
	running := w.kinds[0] != nil || w.kinds[1] != nil
	w.mu.Unlock()
	if running {
		return nil
	}
	return failed
}

// switchTo stops kind k and subscribes to want, if not nil, under catalog version. The same track
// keeps the playout's state, so a group it gets again plays only after the last frame played.
func (w *Watcher) switchTo(ctx context.Context, k int, want *Track, version uint64) error {
	same := want != nil && w.kinds[k] != nil && sameTrack(&w.kinds[k].track, want)
	w.stop(k)
	slot := Kind(k + 1).String()
	if !same {
		w.play.setTrack(slot, want)
	}
	if want == nil {
		return nil
	}
	err := w.start(ctx, k, *want, version)
	if err != nil {
		w.play.setTrack(slot, nil)
		w.mu.Lock()
		w.stats[k].Err = err
		w.mu.Unlock()
	}
	return err
}

// resume subscribes again to the track of wt, which ended by itself, while the catalog in use still
// lists it. Under a newer catalog the end was the old track's. Under the same catalog the SUBSCRIBE
// may have reached a relay before the old track's end and got the old track, so it is tried once more.
func (w *Watcher) resume(k int, wt *watched) {
	w.applying.Lock()
	defer w.applying.Unlock()
	same := w.catalog.Version == wt.since
	if w.ctx.Err() != nil || w.kinds[k] != wt || w.catalog.Version < wt.since || same && wt.again {
		return
	}
	w.mu.Lock()
	ended := w.stats[k].Err
	w.mu.Unlock()
	err := w.switchTo(w.ctx, k, &wt.track, w.catalog.Version)
	switch {
	case err == nil && same:
		w.kinds[k].again = true
	case err != nil && same:
		// The track really ended: keep its end in Stats rather than the refusal of the retry.
		w.mu.Lock()
		w.stats[k].Err = ended
		w.mu.Unlock()
	}
}

func subscribeOptions(cfg PlayoutConfig, kind Kind) *kolomoq.SubscribeOptions {
	late := satAdd(cfg.MaxDelay, cfg.MaxBehind)
	opts := &kolomoq.SubscribeOptions{
		Priority: kolomoq.DefaultPriority,
		Join:     kolomoq.JoinNextObject,
		// A frame later than this can no longer play. The subgroup timeout also bounds how long the
		// subscription waits after PUBLISH_DONE for a stream that never arrives.
		ObjectDeliveryTimeout:   late,
		SubgroupDeliveryTimeout: late,
	}
	if kind == Video {
		opts.Join = kolomoq.JoinCurrentGroup
	}
	return opts
}

func (w *Watcher) start(ctx context.Context, k int, t Track, version uint64) error {
	opts := subscribeOptions(w.play.cfg, t.Kind)
	u, err := NewUnpackager(t)
	if err != nil {
		return err
	}
	var op *secobj.Opener
	if w.opts.Keys != nil {
		if op, err = w.opts.Keys.Opener(w.s, w.ns, t.Name); err != nil {
			return fmt.Errorf("kolomedia: track %q: %w", t.Name, err)
		}
	}
	sub, err := w.s.Subscribe(ctx, w.ns, t.Name, opts)
	if err != nil {
		return fmt.Errorf("kolomedia: subscribe to track %q: %w", t.Name, err)
	}
	tctx, cancel := context.WithCancel(w.ctx)
	wt := &watched{track: t, since: version, sub: sub, op: op, cancel: cancel, done: make(chan struct{})}
	w.mu.Lock()
	w.kinds[k] = wt
	w.stats[k].Track, w.stats[k].Err = t.Name, nil
	w.stats[k].Subscriptions++
	w.mu.Unlock()
	w.wg.Go(func() {
		if w.readTrack(tctx, k, wt, u) {
			w.resume(k, wt)
		}
	})
	return nil
}

// stop ends the subscription of kind k, if any, and waits for its reader.
func (w *Watcher) stop(k int) {
	wt := w.kinds[k]
	if wt == nil {
		return
	}
	wt.cancel()
	<-wt.done
	w.mu.Lock()
	w.kinds[k] = nil
	w.stats[k].Dropped += wt.sub.Dropped()
	w.stats[k].Track = ""
	w.mu.Unlock()
}

func (wt *watched) alive() bool {
	select {
	case <-wt.done:
		return false
	default:
		return true
	}
}

// readTrack plays the objects of wt and reports whether its subscription ended by itself.
func (w *Watcher) readTrack(ctx context.Context, k int, wt *watched, u *Unpackager) (ended bool) {
	defer close(wt.done)
	defer wt.sub.Close()
	slot := wt.track.Kind.String()
	video := wt.track.Kind == Video
	asm := groupAssembler{maxFrames: w.play.cfg.MaxFrames, maxBytes: w.play.cfg.MaxBytes}
	// Objects up to the Largest Object at subscribe time come from the publisher's cache, late by
	// design, so they do not count as network jitter.
	cached, hasCached := wt.sub.Largest()
	count := func(gaps, malformed uint64) {
		w.mu.Lock()
		w.stats[k].Gaps += gaps
		w.stats[k].Malformed += malformed
		w.mu.Unlock()
	}
	cfg := w.play.cfg
	op := &opener{keys: w.opts.Keys, op: wt.op, maxAge: cfg.MaxDelay, maxFrames: cfg.MaxFrames, maxBytes: cfg.MaxBytes, drop: func(err error) {
		w.mu.Lock()
		switch {
		case errors.Is(err, secobj.ErrNoKey):
			w.stats[k].NoKey++
		case errors.Is(err, secobj.ErrKeyRetired):
			w.stats[k].Retired++
		default:
			w.stats[k].Unauthenticated++
		}
		w.mu.Unlock()
	}}
	for {
		obj, at, err := op.next(ctx, wt.sub)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			w.mu.Lock()
			w.stats[k].Err = endedBy(err, ErrTrackEnded, "read the track")
			w.mu.Unlock()
			return true
		}
		if obj.Status != kolomoq.StatusNormal {
			continue
		}
		ready := []kolomoq.Object{obj}
		if video {
			var gaps int
			ready, gaps = asm.add(obj)
			if gaps > 0 {
				count(uint64(gaps), 0)
				w.needKeyframe()
			}
		}
		for _, o := range ready {
			props := make([]Property, len(o.Properties))
			for i, p := range o.Properties {
				props[i] = Property{Type: p.Type, Value: p.Value, Bytes: p.Bytes}
			}
			f, err := u.Unpack(o.ID, props, o.Payload)
			if err == nil {
				live := !hasCached || cached.Less(kolomoq.Location{Group: o.Group, Object: o.ID})
				err = w.play.push(slot, f, at, live)
			}
			if errors.Is(err, ErrClosed) {
				return false
			}
			if err != nil {
				if video {
					// The frames after this one in its group cannot decode.
					asm.cut(o.Group)
					count(1, 1)
					break
				}
				count(0, 1)
			}
		}
	}
}

// groupAssembler releases the objects of each video group in order from object 0 and holds those
// after a missing one, at most maxFrames objects and maxBytes bytes over all groups. A group with a
// hole when a newer group starts, or that would go over a bound, is cut: its later objects never
// play, so the playout's next frame is a newer group's keyframe.
type groupAssembler struct {
	started   bool
	newest    uint64
	groups    map[uint64]*assembling
	maxFrames int
	maxBytes  int
	frames    int
	bytes     int
}

type assembling struct {
	next    uint64
	pending map[uint64]kolomoq.Object
	bytes   int
	cut     bool
}

func objectBytes(o *kolomoq.Object) int {
	n := len(o.Payload)
	for _, p := range o.Properties {
		n += len(p.Bytes)
	}
	return n
}

func (a *groupAssembler) drop(g *assembling) {
	a.frames -= len(g.pending)
	a.bytes -= g.bytes
	g.cut, g.pending, g.bytes = true, nil, 0
}

func (a *groupAssembler) add(obj kolomoq.Object) (ready []kolomoq.Object, gaps int) {
	if a.started && obj.Group < a.newest && a.newest-obj.Group > 1 {
		return nil, 0
	}
	if !a.started || obj.Group > a.newest {
		for id, g := range a.groups {
			if id < obj.Group && !g.cut && len(g.pending) > 0 {
				a.drop(g)
				gaps++
			}
			if obj.Group-id > 1 {
				delete(a.groups, id)
			}
		}
		a.started, a.newest = true, obj.Group
	}
	if a.groups == nil {
		a.groups = make(map[uint64]*assembling)
	}
	g := a.groups[obj.Group]
	if g == nil {
		g = &assembling{}
		a.groups[obj.Group] = g
	}
	switch {
	case g.cut || obj.ID < g.next:
		return nil, gaps
	case obj.ID > g.next:
		if _, ok := g.pending[obj.ID]; ok {
			return nil, gaps
		}
		n := objectBytes(&obj)
		if a.frames >= a.maxFrames || n > a.maxBytes-a.bytes {
			a.drop(g)
			return nil, gaps + 1
		}
		if g.pending == nil {
			g.pending = make(map[uint64]kolomoq.Object)
		}
		g.pending[obj.ID] = obj
		g.bytes += n
		a.frames++
		a.bytes += n
		return nil, gaps
	}
	ready = append(ready, obj)
	g.next++
	for {
		o, ok := g.pending[g.next]
		if !ok {
			break
		}
		delete(g.pending, g.next)
		n := objectBytes(&o)
		g.bytes -= n
		a.frames--
		a.bytes -= n
		ready = append(ready, o)
		g.next++
	}
	return ready, gaps
}

func (a *groupAssembler) cut(group uint64) {
	if g := a.groups[group]; g != nil {
		a.drop(g)
	}
}

// endedBy maps the end of a subscription to ended: io.EOF, or a PUBLISH_DONE status such as the one a
// relay sends when the publisher's session fails. The status error stays in the chain. Any other error
// keeps its cause under a kolomedia: prefix naming op.
func endedBy(err, ended error, op string) error {
	var pd *kolomoq.PublishDoneError
	switch {
	case err == io.EOF:
		return ended
	case errors.As(err, &pd):
		return fmt.Errorf("%w: %w", ended, err)
	}
	return moqError(op, err)
}

func sessionEnded(s *kolomoq.Session) error {
	if err := s.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrSessionEnded, err)
	}
	return ErrSessionEnded
}

func sameTrack(a, b *Track) bool {
	x, y := *a, *b
	x.Config, y.Config = nil, nil
	return reflect.DeepEqual(x, y) && bytes.Equal(a.Config, b.Config)
}

// needKeyframe tells the requester of keyframes that the video track lost frames it cannot go on without.
func (w *Watcher) needKeyframe() {
	select {
	case w.keyNeed <- struct{}{}:
	default:
	}
}

// Keyframe request pacing, checked every keyframeCheck. Silence of keyframeStall counts as a lost group, but a
// still screen is as quiet, so each request for silence doubles the silence needed for the next, and after
// keyframeMaxQuiet of them silence stops counting until keyframeFlow frames arrive in a row or a loss is reported.
// An unanswered request repeats after NewGroupRequestInterval, doubling, and keyframeMaxUnanswered in a row stop
// the asking until a frame plays.
const (
	keyframeCheck         = 250 * time.Millisecond
	keyframeStall         = 500 * time.Millisecond
	keyframeMaxQuiet      = 5
	keyframeFlow          = 30
	keyframeMaxUnanswered = 5
)

// keyframeGate decides when the requester of keyframes asks. It reads no clock: the caller passes the time.
type keyframeGate struct {
	seen       uint64
	moved      time.Time
	last       time.Time
	flow       uint64
	need       bool
	silent     bool
	quiet      int
	unanswered int
}

func (g *keyframeGate) reset(now time.Time) {
	*g = keyframeGate{seen: g.seen, moved: now}
}

// signal records a loss the playout reported, so the request it causes does not count as for silence.
func (g *keyframeGate) signal() {
	g.need, g.silent, g.quiet = true, false, 0
}

// observe takes the video counters of one check. A frame that arrives while the track plays answers the requests so far.
func (g *keyframeGate) observe(now time.Time, received uint64, waiting bool) {
	if received != g.seen {
		if !waiting {
			g.unanswered = 0
		}
		if now.Sub(g.moved) < keyframeStall {
			g.flow += received - g.seen
		} else {
			g.flow = 0
		}
		if g.flow >= keyframeFlow {
			g.quiet = 0
		}
		g.seen, g.moved = received, now
	}
	switch {
	case waiting:
		g.signal()
	case !g.need && g.quiet < keyframeMaxQuiet && now.Sub(g.moved) >= keyframeStall<<g.quiet:
		g.need, g.silent = true, true
	}
}

func (g *keyframeGate) ready(now time.Time) bool {
	return g.need && g.unanswered < keyframeMaxUnanswered &&
		now.Sub(g.last) >= kolomoq.NewGroupRequestInterval<<g.unanswered
}

func (g *keyframeGate) asked(now time.Time) {
	if g.silent {
		g.quiet++
	}
	g.need, g.silent, g.last = false, false, now
	g.unanswered++
}

// requestKeyframes asks for a new group within the bounds of keyframeGate. It asks only while the catalog lists a
// video track, so a publisher that turns its camera off by removing the track is left alone.
func (w *Watcher) requestKeyframes() {
	const video = 1
	tick := time.NewTicker(keyframeCheck)
	defer tick.Stop()
	g := keyframeGate{moved: time.Now()}
	var sub *kolomoq.Subscription
	for {
		var now time.Time
		checked := false
		select {
		case <-w.ctx.Done():
			return
		case <-w.keyNeed:
			now = time.Now()
		case now = <-tick.C:
			checked = true
		}
		w.mu.Lock()
		wt := w.kinds[video]
		w.mu.Unlock()
		var cur *kolomoq.Subscription
		if wt != nil {
			cur = wt.sub
		}
		if cur != sub {
			g.reset(now)
			sub = cur
		}
		if wt == nil {
			continue
		}
		if checked {
			st := w.play.Stats().Tracks[Video.String()]
			g.observe(now, st.Received, st.WaitingKeyframe)
		} else {
			g.signal()
		}
		if !g.ready(now) {
			continue
		}
		g.asked(now)
		ctx, cancel := context.WithTimeout(w.ctx, 2*time.Second)
		err := wt.sub.RequestNewGroup(ctx)
		cancel()
		if err == nil {
			w.mu.Lock()
			w.stats[video].KeyframeRequests++
			w.mu.Unlock()
		}
	}
}
