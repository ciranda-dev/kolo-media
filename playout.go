package kolomedia

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

// PlayoutConfig configures a Playout. A zero field takes its default.
type PlayoutConfig struct {
	// MinDelay is the least playout delay, 100 ms by default: the jitter allowance of an interactive call.
	MinDelay time.Duration
	// MaxDelay caps the delay, 400 ms (a Wi-Fi retransmission burst) or MinDelay if larger by default.
	MaxDelay time.Duration
	// MaxBehind is how late past its due time a frame may still play, 50 ms by default. Later audio is
	// dropped; late video jumps to its newest due keyframe, or plays at once if there is none.
	MaxBehind time.Duration
	// MaxFrames bounds the frames buffered per track, 8192 by default.
	MaxFrames int
	// MaxBytes bounds the frame data buffered per track, 16 MiB by default.
	MaxBytes int
	// OnKeyframeNeeded, if set, is called once when a video track drops a delta frame while it waits
	// for a keyframe, at start or after a gap. It runs on the goroutine calling Next and must not block.
	OnKeyframeNeeded func(track string)
}

func (c PlayoutConfig) withDefaults() (PlayoutConfig, error) {
	if c.MinDelay < 0 || c.MaxDelay < 0 || c.MaxBehind < 0 || c.MaxFrames < 0 || c.MaxBytes < 0 {
		return c, fmt.Errorf("%w: negative value", ErrInvalidConfig)
	}
	if c.MinDelay == 0 {
		c.MinDelay = 100 * time.Millisecond
	}
	if c.MaxDelay == 0 {
		c.MaxDelay = max(400*time.Millisecond, c.MinDelay)
	}
	if c.MaxBehind == 0 {
		c.MaxBehind = 50 * time.Millisecond
	}
	if c.MaxFrames == 0 {
		c.MaxFrames = 8192
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 16 << 20
	}
	if c.MinDelay > c.MaxDelay {
		return c, fmt.Errorf("%w: MinDelay %v over MaxDelay %v", ErrInvalidConfig, c.MinDelay, c.MaxDelay)
	}
	return c, nil
}

// PlayoutStats is a snapshot of a Playout.
type PlayoutStats struct {
	// Delay is the current playout delay.
	Delay time.Duration
	// Jitter is the measured 95th percentile of arrival lateness.
	Jitter time.Duration
	// Tracks holds the counters of each track by name.
	Tracks map[string]TrackStats
}

// TrackStats counts the frames of one track.
type TrackStats struct {
	// Received counts the valid frames pushed.
	Received uint64
	// Played counts the frames returned by Next.
	Played uint64
	// Late counts frames dropped for being older than a frame already played or skipped.
	Late uint64
	// Duplicate counts frames dropped for a timestamp already buffered or just played.
	Duplicate uint64
	// Ahead counts frames dropped for arriving more than 2 s earlier than the reference allows.
	Ahead uint64
	// Overflow counts frames dropped because the buffer was over MaxFrames or MaxBytes.
	Overflow uint64
	// Skipped counts frames dropped for being more than MaxBehind late (skip to live).
	Skipped uint64
	// NoKeyframe counts video frames dropped while waiting for a keyframe.
	NoKeyframe uint64
	// Buffered is the number of buffered frames.
	Buffered int
	// BufferedBytes is the size of the buffered frames' data.
	BufferedBytes int
	// WaitingKeyframe reports a video track that needs a keyframe before it can play again.
	WaitingKeyframe bool
	// LateRun is how long the current run of frames skipped for being more than MaxBehind late has lasted, 0 once a
	// frame played. An audio run that lasts 2 s while another track plays shifts the audio to play again.
	LateRun time.Duration
}

// Playout buffers the received frames of one broadcast and releases each when it is due, keeping
// the tracks in sync; video follows the audio actually played. It is safe for concurrent use.
type Playout struct {
	cfg   PlayoutConfig
	clk   clock
	start time.Time

	mu         sync.Mutex
	changed    chan struct{}
	closed     bool
	tracks     map[string]*trackState
	est        estimator
	delay      time.Duration
	outLatency time.Duration
	audioLag   time.Duration
}

type trackState struct {
	track      Track
	prev       Track // the last track set, kept while the slot is empty
	frames     []*buffered
	bytes      int
	played     bool
	last       uint64
	staleSince time.Duration // first arrival of the current run of frames dropped as late, or -1
	// aheadSince is the first arrival of the current run of this track's frames dropped as ahead, or -1. It is
	// the track's own: a run of audio frames must not be cut short by the late video frames that arrive between them.
	aheadSince time.Duration
	needKey    bool
	notified   bool
	latency    time.Duration
	stats      TrackStats
	// lateSince is when the current run of frames skipped as too late began, or -1, and lateLast the last skip of it;
	// lateOthers is how many frames the other tracks had played then. shift delays the track's due times after its
	// timeline fell behind the others', until its frames arrive in time without it for a window (earlySince, or -1).
	// lastLag is how late past its due time the last frame played.
	lateSince  time.Duration
	lateLast   time.Duration
	lateOthers uint64
	shift      time.Duration
	earlySince time.Duration
	lastLag    time.Duration
}

type buffered struct {
	f     Frame
	media time.Duration
	at    time.Duration // arrival
}

// NewPlayout returns a Playout for the tracks of one broadcast, which share one time base.
// At most one track may be audio: it paces the others.
func NewPlayout(tracks []Track, cfg PlayoutConfig) (*Playout, error) {
	return newPlayout(tracks, cfg, systemClock{})
}

func newPlayout(tracks []Track, cfg PlayoutConfig, clk clock) (*Playout, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if len(tracks) == 0 {
		return nil, fmt.Errorf("%w: no tracks", ErrInvalidConfig)
	}
	p := &Playout{
		cfg:     cfg,
		clk:     clk,
		start:   clk.now(),
		changed: make(chan struct{}),
		tracks:  make(map[string]*trackState, len(tracks)),
		delay:   cfg.MinDelay,
	}
	audio := 0
	for _, t := range tracks {
		if err := checkTrack(&t); err != nil {
			return nil, err
		}
		if _, ok := p.tracks[t.Name]; ok {
			return nil, fmt.Errorf("%w %q: listed twice", ErrInvalidTrack, t.Name)
		}
		if t.Kind == Audio {
			if audio++; audio > 1 {
				return nil, fmt.Errorf("%w: more than one audio track", ErrInvalidConfig)
			}
		}
		t.Config = slices.Clone(t.Config)
		p.tracks[t.Name] = &trackState{track: t, needKey: t.Kind == Video, staleSince: -1, aheadSince: -1, lateSince: -1, earlySince: -1}
	}
	return p, nil
}

// newSlotPlayout returns a Playout with an empty "audio" and "video" slot, for setTrack.
func newSlotPlayout(cfg PlayoutConfig, clk clock) (*Playout, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	p := &Playout{cfg: cfg, clk: clk, start: clk.now(), changed: make(chan struct{}), delay: cfg.MinDelay}
	p.tracks = map[string]*trackState{
		Audio.String(): {staleSince: -1, aheadSince: -1, lateSince: -1, earlySince: -1},
		Video.String(): {staleSince: -1, aheadSince: -1, lateSince: -1, earlySince: -1},
	}
	return p, nil
}

// setTrack makes t, or nothing if t is nil, the source of a slot, dropping what the slot buffered.
// The play position stays while the slot is empty and when t is the slot's previous track, so the
// group a new subscription gets again after a removal and re-add does not play twice.
func (p *Playout) setTrack(slot string, t *Track) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.tracks[slot]
	s.drop(len(s.frames))
	next := trackState{staleSince: -1, aheadSince: -1, lateSince: -1, earlySince: -1, latency: s.latency, stats: s.stats, prev: s.prev}
	keep := t == nil || sameTrack(&s.prev, t)
	if keep {
		next.played, next.last, next.needKey, next.notified, next.shift = s.played, s.last, s.needKey, s.notified, s.shift
	}
	if t != nil {
		next.track = *t
		next.track.Config = slices.Clone(t.Config)
		next.prev = next.track
		if !keep {
			next.needKey = t.Kind == Video
		}
	}
	*s = next
	p.notify()
}

// Push copies a received frame of the named track into its buffer; arrival is when it was received,
// the zero time meaning now. Dropped frames are counted in Stats, not reported as errors.
func (p *Playout) Push(track string, f Frame, arrival time.Time) error {
	return p.push(track, f, arrival, true)
}

// push is Push; a frame sent from the publisher's cache rather than live is not measured (measure false).
func (p *Playout) push(track string, f Frame, arrival time.Time, measure bool) error {
	if arrival.IsZero() {
		arrival = p.clk.now()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	t, ok := p.tracks[track]
	if !ok || t.track.Kind == 0 {
		return fmt.Errorf("%w %q", ErrUnknownTrack, track)
	}
	if err := validateFrame(&t.track, &f); err != nil {
		return err
	}
	if len(f.Data) > p.cfg.MaxBytes {
		return fmt.Errorf("%w: %d bytes, limit %d", ErrFrameTooLarge, len(f.Data), p.cfg.MaxBytes)
	}
	media, err := ToDuration(f.Timestamp, t.track.Timescale)
	if err != nil {
		return err
	}
	t.stats.Received++
	a := max(arrival.Sub(p.start), 0)
	key := a - media

	// A frame far earlier than the reference allows is a bad timestamp, unless only such frames
	// arrive for a whole estimate window: then the sender's time base moved and the reference restarts.
	if measure && len(p.est.sorted) > 0 && key < satSub(p.est.reference(), estimateWindow) {
		if t.aheadSince < 0 {
			t.aheadSince = a
		}
		if a-t.aheadSince < estimateWindow {
			t.stats.Ahead++
			return nil
		}
		p.est = estimator{}
	}
	t.aheadSince = -1

	// Likewise a frame older than the last one played is late, unless nothing newer has come for a
	// whole window: then the last frame played was the bad one.
	if t.played && f.Timestamp <= t.last {
		if t.staleSince < 0 {
			t.staleSince = a
		}
		if a-t.staleSince < estimateWindow {
			if f.Timestamp == t.last {
				t.stats.Duplicate++
			} else {
				if measure {
					p.observe(a, key)
				}
				t.stats.Late++
			}
			return nil
		}
		t.played = false
		t.needKey = t.track.Kind == Video
	}

	i, found := slices.BinarySearchFunc(t.frames, f.Timestamp, func(b *buffered, ts uint64) int {
		switch {
		case b.f.Timestamp < ts:
			return -1
		case b.f.Timestamp > ts:
			return 1
		}
		return 0
	})
	if found {
		t.stats.Duplicate++
		return nil
	}
	t.staleSince = -1
	if measure {
		p.observe(a, key)
	}
	defer p.notify()
	full := len(t.frames) >= p.cfg.MaxFrames || t.bytes+len(f.Data) > p.cfg.MaxBytes
	if i == 0 && full {
		// It would be evicted at once as the oldest frame.
		t.overflow()
		return nil
	}
	f.Data = slices.Clone(f.Data)
	if f.AudioLevel != nil {
		level := *f.AudioLevel
		f.AudioLevel = &level
	}
	t.frames = slices.Insert(t.frames, i, &buffered{f: f, media: media, at: a})
	t.bytes += len(f.Data)
	for len(t.frames) > p.cfg.MaxFrames || t.bytes > p.cfg.MaxBytes {
		t.drop(1)
		t.overflow()
	}
	return nil
}

func (p *Playout) observe(a, key time.Duration) {
	p.est.observe(a, key)
	p.delay = min(max(p.est.jitter(), p.cfg.MinDelay), p.cfg.MaxDelay)
}

// Next returns the next frame of the named track when it is due, blocking until then. It returns
// ctx.Err() if ctx ends first and ErrClosed once the Playout is closed.
func (p *Playout) Next(ctx context.Context, track string) (Frame, error) {
	f, _, err := p.next(ctx, track)
	return f, err
}

// next is Next that also returns the track the frame belongs to.
func (p *Playout) next(ctx context.Context, track string) (Frame, Track, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return Frame{}, Track{}, ErrClosed
		}
		t, ok := p.tracks[track]
		if !ok {
			p.mu.Unlock()
			return Frame{}, Track{}, fmt.Errorf("%w %q", ErrUnknownTrack, track)
		}
		r := p.poll(t, p.clk.now())
		tr := t.track
		changed := p.changed
		p.mu.Unlock()
		if r.keyframeNeeded && p.cfg.OnKeyframeNeeded != nil {
			p.cfg.OnKeyframeNeeded(track)
		}
		if r.ok {
			return r.f, tr, nil
		}
		var timer <-chan time.Time
		stop := func() bool { return false }
		if r.wait > 0 {
			timer, stop = p.clk.after(r.wait)
		}
		select {
		case <-ctx.Done():
			stop()
			return Frame{}, Track{}, ctx.Err()
		case <-changed:
			stop()
		case <-timer:
		}
	}
}

type pollResult struct {
	f              Frame
	ok             bool
	wait           time.Duration
	keyframeNeeded bool
}

// poll releases the head of t if it is due, after dropping what cannot play. With no frame due,
// wait is how long until the head is due, or 0 if t is empty.
func (p *Playout) poll(t *trackState, now time.Time) pollResult {
	var r pollResult
	n := now.Sub(p.start)
	video := t.track.Kind == Video
	for len(t.frames) > 0 {
		h := t.frames[0]
		if video && t.needKey && !h.f.Keyframe {
			t.skip(1)
			t.stats.NoKeyframe++
			if !t.notified {
				t.notified = true
				r.keyframeNeeded = true
			}
			continue
		}
		due := p.due(t, h.media)
		if t.shift > 0 {
			if h.at <= satSub(due, t.shift) {
				if t.earlySince < 0 {
					t.earlySince = n
				}
				if satSub(n, t.earlySince) >= estimateWindow {
					t.shift, t.earlySince = 0, -1 // the timeline caught up
					continue
				}
			} else {
				t.earlySince = -1
			}
		}
		if late := satSub(n, due); late > p.cfg.MaxBehind {
			if t.lateSince < 0 || satSub(n, t.lateLast) >= estimateWindow {
				t.lateSince, t.lateOthers = n, p.playedBesides(t)
			}
			t.lateLast = n
			if !video {
				if satSub(n, t.lateSince) >= estimateWindow && p.playedBesides(t) > t.lateOthers && p.othersOnTime(t) {
					// The other tracks hold the reference and play: this timeline fell behind theirs, and no delay
					// covers that. It plays again, out of sync by what it fell behind.
					t.shift = satAdd(t.shift, late)
					t.lateSince = -1
					continue
				}
				t.skip(1)
				t.stats.Skipped++
				continue
			}
			if k := p.dueKeyframe(t, n); k > 0 {
				t.skip(k)
				t.stats.Skipped += uint64(k)
				continue
			}
		} else if due > n {
			r.wait = satSub(due, n)
			return r
		}
		r.f, r.ok = h.f, true
		t.skip(1)
		t.stats.Played++
		t.lateSince, t.lastLag = -1, max(satSub(n, due), 0)
		if video && r.f.Keyframe {
			t.needKey, t.notified = false, false
		}
		if lag := max(satSub(n, due), 0); !video && lag != p.audioLag {
			p.audioLag = lag
			p.notify()
		}
		return r
	}
	return r
}

func (p *Playout) due(t *trackState, media time.Duration) time.Duration {
	d := satAdd(p.est.reference(), media)
	d = satAdd(d, p.delay)
	d = satAdd(d, p.outLatency-t.latency)
	d = satAdd(d, t.shift)
	if t.track.Kind == Video {
		d = satAdd(d, p.audioLag)
	}
	return d
}

// playedBesides is how many frames the tracks other than t have played.
func (p *Playout) playedBesides(t *trackState) uint64 {
	var n uint64
	for _, o := range p.tracks {
		if o != t {
			n += o.stats.Played
		}
	}
	return n
}

// othersOnTime reports whether the last frame each other track played was at most MaxBehind late: a delay on the path
// makes them late too, a timeline of t's own that fell behind does not.
func (p *Playout) othersOnTime(t *trackState) bool {
	for _, o := range p.tracks {
		if o != t && o.stats.Played > 0 && o.lastLag > p.cfg.MaxBehind {
			return false
		}
	}
	return true
}

// dueKeyframe returns the index of the newest buffered keyframe after the head that is due, or 0.
func (p *Playout) dueKeyframe(t *trackState, n time.Duration) int {
	for i := len(t.frames) - 1; i > 0; i-- {
		if t.frames[i].f.Keyframe && p.due(t, t.frames[i].media) <= n {
			return i
		}
	}
	return 0
}

// skip removes the first n frames as played or passed over, so a frame arriving again for one of
// them counts as late.
func (t *trackState) skip(n int) {
	t.played, t.last = true, t.frames[n-1].f.Timestamp
	t.drop(n)
}

func (t *trackState) drop(n int) {
	for i := range n {
		t.bytes -= len(t.frames[i].f.Data)
		t.frames[i] = nil
	}
	t.frames = t.frames[n:]
}

func (t *trackState) overflow() {
	t.stats.Overflow++
	if t.track.Kind == Video {
		t.needKey = true
	}
}

// SetOutputLatency reports how long the application holds frames of the named track after Next,
// such as an audio device buffer. The other tracks are delayed to match.
func (p *Playout) SetOutputLatency(track string, d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("%w: negative output latency", ErrInvalidConfig)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	t, ok := p.tracks[track]
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownTrack, track)
	}
	t.latency = d
	p.outLatency = 0
	for _, t := range p.tracks {
		p.outLatency = max(p.outLatency, t.latency)
	}
	p.notify()
	return nil
}

// Stats returns a snapshot of the delay and the counters of every track.
func (p *Playout) Stats() PlayoutStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := PlayoutStats{Delay: p.delay, Jitter: p.est.jitter(), Tracks: make(map[string]TrackStats, len(p.tracks))}
	n := p.clk.now().Sub(p.start)
	for name, t := range p.tracks {
		ts := t.stats
		if t.lateSince >= 0 {
			ts.LateRun = max(satSub(n, t.lateSince), 0)
		}
		ts.Buffered, ts.BufferedBytes = len(t.frames), t.bytes
		ts.WaitingKeyframe = t.track.Kind == Video && t.needKey
		s.Tracks[name] = ts
	}
	return s
}

// Close drops every buffered frame and makes blocked and later calls return ErrClosed.
func (p *Playout) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	for _, t := range p.tracks {
		t.drop(len(t.frames))
	}
	p.est = estimator{}
	p.notify()
	return nil
}

func (p *Playout) notify() {
	close(p.changed)
	p.changed = make(chan struct{})
}

type clock interface {
	now() time.Time
	after(d time.Duration) (<-chan time.Time, func() bool)
}

type systemClock struct{}

func (systemClock) now() time.Time { return time.Now() }

func (systemClock) after(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)
	return t.C, t.Stop
}
