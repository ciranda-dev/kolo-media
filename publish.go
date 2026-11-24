package kolomedia

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	kolomoq "github.com/ciranda-dev/kolo-moq"
	"github.com/ciranda-dev/kolo-moq/secobj"
)

// MoQ publisher priorities (lower is sent first): the catalog, then audio, then video, all ahead of a
// track at kolomoq.DefaultPriority.
const (
	CatalogPriority = 28
	AudioPriority   = 48
	VideoPriority   = 68
)

// MediaGroupTimeout is the subgroup delivery timeout of media tracks: a group not sent 30 s after it
// ended is no use to a live subscriber and is dropped.
const MediaGroupTimeout = 30 * time.Second

// Broadcast publishes the tracks of one broadcast and its catalog under a namespace of an origin.
// Its methods are safe for concurrent use.
type Broadcast struct {
	origin *kolomoq.Origin
	ns     kolomoq.Namespace
	sender *secobj.Sender // nil for an unprotected broadcast
	limits CatalogLimits

	mu      sync.Mutex
	closed  bool
	catalog *kolomoq.TrackWriter
	prev    *kolomoq.GroupWriter // the last catalog group, closed
	version uint64
	tracks  map[string]*TrackPublisher
	order   []string
	keyID   uint64        // the sender's Key ID when the catalog was last published
	done    chan struct{} // closed by Close; ends followKeys
	wg      sync.WaitGroup
}

// TrackPublisher publishes the frames of one track of a Broadcast. Its methods are safe for concurrent use.
type TrackPublisher struct {
	b     *Broadcast
	track Track

	mu       sync.Mutex
	closed   bool
	writer   *kolomoq.TrackWriter
	packager *Packager
	group    *kolomoq.GroupWriter
}

// NewBroadcast creates the catalog track under ns in origin and publishes an empty catalog, version 0.
// Catalog versions are the catalog track's group IDs. AddTrack checks the catalog against lim, the first
// of limits if there is one: a receiver reads it only if its own CatalogLimits.MaxTracks is as high.
func NewBroadcast(origin *kolomoq.Origin, ns kolomoq.Namespace, limits ...CatalogLimits) (*Broadcast, error) {
	return startBroadcast(origin, ns, nil, limits)
}

func startBroadcast(origin *kolomoq.Origin, ns kolomoq.Namespace, sender *secobj.Sender, limits []CatalogLimits) (*Broadcast, error) {
	w, err := origin.CreateTrack(ns, CatalogTrack, &kolomoq.TrackOptions{Priority: CatalogPriority})
	if errors.Is(err, kolomoq.ErrTrackExists) {
		return nil, fmt.Errorf("%w: %w", ErrBroadcastExists, err)
	} else if err != nil {
		return nil, moqError("create catalog track", err)
	}
	b := &Broadcast{origin: origin, ns: slices.Clone(ns), sender: sender, catalog: w, tracks: make(map[string]*TrackPublisher), done: make(chan struct{})}
	if len(limits) > 0 {
		b.limits = limits[0]
	}
	if err := b.publishCatalog(nil); err != nil {
		w.Close()
		return nil, err
	}
	if sender != nil {
		b.wg.Go(b.followKeys)
	}
	return b, nil
}

// followKeys publishes the catalog again under each new key without waiting for a frame, so a
// member who just joined reads it while the sender writes nothing.
func (b *Broadcast) followKeys() {
	for {
		changed := b.sender.Changed()
		b.rekeyed()
		select {
		case <-changed:
		case <-b.done:
			return
		}
	}
}

// Catalog returns the catalog last published.
func (b *Broadcast) Catalog() *Catalog {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.catalogLocked(nil)
}

func (b *Broadcast) catalogLocked(extra *Track) *Catalog {
	c := &Catalog{Version: b.version}
	for _, name := range b.order {
		t := b.tracks[name].track
		t.Config = slices.Clone(t.Config)
		c.Tracks = append(c.Tracks, t)
	}
	if extra != nil {
		c.Tracks = append(c.Tracks, *extra)
	}
	return c
}

// publishCatalog writes the catalog with extra appended as the next version, in a new group. An oversize
// catalog fails before any group opens, so the previous one stays current for a subscriber joining now.
func (b *Broadcast) publishCatalog(extra *Track) error {
	next := uint64(0)
	if b.prev != nil {
		next = b.prev.ID() + 1
	}
	c := b.catalogLocked(extra)
	c.Version = next
	data, err := c.MarshalBinaryLimits(b.limits)
	if err != nil {
		return err
	}
	if b.prev != nil {
		// A closed group checks the size of a payload before it checks that it is closed, and writes nothing
		// either way, so this tells whether data fits without opening the group.
		probe := data
		if b.sender != nil {
			probe = make([]byte, sealedLen(len(data)))
		}
		if _, err := b.prev.WriteObject(probe); errors.Is(err, kolomoq.ErrObjectTooLarge) {
			return fmt.Errorf("%w: %d bytes: %w", ErrInvalidCatalog, len(data), err)
		}
	}
	g, err := b.catalog.NewGroup()
	if err != nil {
		return moqError("publish catalog", err)
	}
	b.prev = g
	var kid uint64
	if b.sender != nil {
		kid = b.sender.KeyID()
		_, err = b.sender.Write(g, CatalogTrack, data)
	} else {
		_, err = g.WriteObject(data)
	}
	if err != nil {
		g.Close()
		return moqError("publish catalog", err)
	}
	if err := g.Close(); err != nil {
		return moqError("publish catalog", err)
	}
	b.version, b.keyID = c.Version, kid
	return nil
}

// AddTrack adds t to the broadcast and publishes the new catalog. A zero t.Priority takes
// AudioPriority or VideoPriority. The catalog must fit the origin's MaxObjectSize, else the error wraps
// ErrInvalidCatalog and kolomoq.ErrObjectTooLarge and the track is not added.
func (b *Broadcast) AddTrack(t Track, opts PackagerOptions) (*TrackPublisher, error) {
	if t.Priority == 0 {
		t.Priority = AudioPriority
		if t.Kind == Video {
			t.Priority = VideoPriority
		}
	}
	t.Config = slices.Clone(t.Config)
	pk, err := NewPackager(t, opts)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	if t.Name == CatalogTrack {
		return nil, fmt.Errorf("%w %q: the catalog's name", ErrInvalidTrack, t.Name)
	}
	if _, ok := b.tracks[t.Name]; ok {
		return nil, fmt.Errorf("%w %q: already in the broadcast", ErrInvalidTrack, t.Name)
	}
	c := b.catalogLocked(&t)
	if err := c.validate(b.limits.withDefaults()); err != nil {
		return nil, err
	}
	w, err := b.origin.CreateTrack(b.ns, t.Name, &kolomoq.TrackOptions{Priority: t.Priority, SubgroupDeliveryTimeout: MediaGroupTimeout})
	if errors.Is(err, kolomoq.ErrTrackExists) {
		return nil, fmt.Errorf("%w %q: %w", ErrInvalidTrack, t.Name, err)
	} else if err != nil {
		return nil, moqError("create track", err)
	}
	if err := b.publishCatalog(&t); err != nil {
		w.Close()
		return nil, err
	}
	p := &TrackPublisher{b: b, track: t, writer: w, packager: pk}
	b.tracks[t.Name] = p
	b.order = append(b.order, t.Name)
	return p, nil
}

// RemoveTrack removes the named track, publishes the new catalog and ends the track for its subscribers.
func (b *Broadcast) RemoveTrack(name string) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	p, ok := b.tracks[name]
	if !ok {
		b.mu.Unlock()
		return fmt.Errorf("%w %q", ErrUnknownTrack, name)
	}
	return b.removeLocked(p)
}

// removeLocked removes p, which is in the broadcast, and unlocks b.mu.
func (b *Broadcast) removeLocked(p *TrackPublisher) error {
	delete(b.tracks, p.track.Name)
	b.order = slices.DeleteFunc(b.order, func(n string) bool { return n == p.track.Name })
	err := b.publishCatalog(nil)
	b.mu.Unlock()
	return errors.Join(err, p.end())
}

// Close ends every track and the catalog; subscribers see the broadcast end.
func (b *Broadcast) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.done)
	tracks := slices.Collect(maps.Values(b.tracks))
	b.tracks = map[string]*TrackPublisher{}
	b.order = nil
	b.mu.Unlock()
	b.wg.Wait()
	var errs []error
	for _, p := range tracks {
		errs = append(errs, p.end())
	}
	return errors.Join(append(errs, moqError("close catalog track", b.catalog.Close()))...)
}

// Track returns the track's description as published in the catalog.
func (p *TrackPublisher) Track() Track {
	t := p.track
	t.Config = slices.Clone(t.Config)
	return t
}

// WriteFrame packages f and publishes it, starting a new group where the Packager does. It never
// waits for subscribers. A failed write leaves only a new group for the next frame, which on a video
// track must be a keyframe (ErrNeedKeyframe). On a protected broadcast f.AudioLevel is checked and
// dropped, and the catalog is published again under each new key; a failure there is not the frame's error.
func (p *TrackPublisher) WriteFrame(f Frame) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	saved := *p.packager
	obj, err := p.packager.Pack(f)
	if err != nil {
		return err
	}
	if obj.ID == 0 {
		if p.group, err = p.writer.NewGroup(); err != nil {
			p.failed(saved)
			return moqError("write frame", err)
		}
	}
	s := p.b.sender
	props := make([]kolomoq.Property, 0, len(obj.Properties))
	for _, pr := range obj.Properties {
		// Its vi64 takes 1 byte for silence and 2 for voice, so ciphertext sizes would reveal speech.
		if s != nil && pr.Type == PropertyAudioLevel {
			continue
		}
		props = append(props, kolomoq.Property{Type: pr.Type, Value: pr.Value, Bytes: pr.Bytes})
	}
	if s != nil {
		_, err = s.Write(p.group, p.track.Name, obj.Payload, props...)
	} else {
		_, err = p.group.WriteObject(obj.Payload, props...)
	}
	if err != nil {
		p.failed(saved)
		return moqError("write frame", err)
	}
	if s != nil {
		p.b.rekeyed()
	}
	return nil
}

// rekeyed publishes the catalog again when the sender's key changed since it was last published.
// A failure is retried at the next key change or frame.
func (b *Broadcast) rekeyed() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed && b.sender.KeyID() != b.keyID {
		_ = b.publishCatalog(nil)
	}
}

// failed undoes the Pack of a frame that was not written and makes the next frame start a group,
// so the packager's object IDs stay those of the MoQ group.
func (p *TrackPublisher) failed(saved Packager) {
	*p.packager = saved
	p.packager.restart = true
	p.group = nil
}

// KeyframeRequests receives when a watcher asks, with WatchOptions.RequestKeyframes, for a new group. The
// next frame written must then be a keyframe. Signals merge. Answer with at most one keyframe a second: watchers add up.
func (p *TrackPublisher) KeyframeRequests() <-chan struct{} { return p.writer.NewGroupRequests() }

// Close removes the track from its broadcast, as Broadcast.RemoveTrack. Closing it again, or after
// the broadcast closed, does nothing.
func (p *TrackPublisher) Close() error {
	b := p.b
	b.mu.Lock()
	if b.tracks[p.track.Name] != p {
		b.mu.Unlock()
		return nil
	}
	return b.removeLocked(p)
}

func (p *TrackPublisher) end() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return moqError("close track", p.writer.Close())
}
