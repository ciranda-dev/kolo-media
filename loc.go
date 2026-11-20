package kolomedia

import (
	"bytes"
	"fmt"
	"time"
)

// LOC property types (draft-ietf-moq-loc-04). An even type carries a varint Value, an odd type Bytes.
const (
	PropertyTimescale   = 0x08
	PropertyAudioLevel  = 0x0C
	PropertyVideoConfig = 0x0D
	PropertyAudioConfig = 0x0F
	PropertyTimestamp   = 0x10
)

// Property is one MoQ object property: Value when Type is even, Bytes when Type is odd.
type Property struct {
	Type  uint64
	Value uint64
	Bytes []byte
}

// Object is a frame packaged for MoQ: its place in the track, its properties in ascending type
// order and its payload. Payload and Bytes share memory with the frame and track; do not modify them.
type Object struct {
	Group      uint64
	ID         uint64
	Properties []Property
	Payload    []byte
}

// PackagerOptions configures a Packager.
type PackagerOptions struct {
	// AudioGroupDuration starts a new audio group once the current one spans this long.
	// Zero puts every audio frame in its own group.
	AudioGroupDuration time.Duration
}

// Packager turns the frames of one track into LOC objects and decides the group boundaries:
// a video track starts a group at each keyframe, an audio track per frame or per AudioGroupDuration.
// A Packager is not safe for concurrent use.
type Packager struct {
	track      Track
	groupTicks uint64
	started    bool
	last       uint64
	groupStart uint64
	group      uint64
	id         uint64
	restart    bool // the next frame starts a group
}

// NewPackager returns a Packager for track t, which must be valid.
func NewPackager(t Track, opts PackagerOptions) (*Packager, error) {
	if err := checkTrack(&t); err != nil {
		return nil, err
	}
	ticks, err := FromDuration(opts.AudioGroupDuration, t.Timescale)
	if err != nil {
		return nil, fmt.Errorf("%w: audio group duration %v", ErrTimestampRange, opts.AudioGroupDuration)
	}
	return &Packager{track: t, groupTicks: ticks}, nil
}

// Pack returns the object for the next frame. Timestamps must increase and a video track must
// start with a keyframe.
func (p *Packager) Pack(f Frame) (Object, error) {
	t := &p.track
	if err := validateFrame(t, &f); err != nil {
		return Object{}, err
	}
	if p.started && f.Timestamp <= p.last {
		return Object{}, fmt.Errorf("%w: %d after %d", ErrTimestampOrder, f.Timestamp, p.last)
	}
	var newGroup bool
	if t.Kind == Video {
		if (!p.started || p.restart) && !f.Keyframe {
			return Object{}, ErrNeedKeyframe
		}
		newGroup = f.Keyframe
	} else {
		newGroup = !p.started || p.restart || f.Timestamp-p.groupStart >= p.groupTicks
	}
	switch {
	case !p.started:
	case newGroup:
		p.group++
		p.id = 0
	default:
		p.id++
	}
	if newGroup {
		p.groupStart = f.Timestamp
	}
	p.started, p.restart = true, false
	p.last = f.Timestamp

	props := make([]Property, 0, 4)
	if t.Timescale != DefaultTimescale {
		props = append(props, Property{Type: PropertyTimescale, Value: t.Timescale})
	}
	if f.AudioLevel != nil {
		v := uint64(f.AudioLevel.Level)
		if f.AudioLevel.Voice {
			v |= 0x80
		}
		props = append(props, Property{Type: PropertyAudioLevel, Value: v})
	}
	if newGroup && len(t.Config) > 0 {
		props = append(props, Property{Type: configProperty(t.Kind), Bytes: t.Config})
	}
	props = append(props, Property{Type: PropertyTimestamp, Value: f.Timestamp})
	return Object{Group: p.group, ID: p.id, Properties: props, Payload: f.Data}, nil
}

func configProperty(k Kind) uint64 {
	if k == Video {
		return PropertyVideoConfig
	}
	return PropertyAudioConfig
}

// Unpackager turns the LOC objects of one track back into frames. It is safe for concurrent use.
type Unpackager struct {
	track    Track
	duration uint64
}

// NewUnpackager returns an Unpackager for track t, which must be valid.
func NewUnpackager(t Track) (*Unpackager, error) {
	if err := checkTrack(&t); err != nil {
		return nil, err
	}
	u := &Unpackager{track: t}
	switch {
	case t.Kind == Audio:
		u.duration, _ = FromDuration(t.FrameDuration, t.Timescale)
	case t.FrameRate > 0:
		if d := float64(t.Timescale)/t.FrameRate + 0.5; d < 1<<63 {
			u.duration = uint64(d)
		}
	}
	return u, nil
}

// Unpack returns the frame in the object with the given ID in its group; Data is the payload itself and
// Duration comes from the catalog. Unknown property types are ignored.
func (u *Unpackager) Unpack(id uint64, props []Property, payload []byte) (Frame, error) {
	t := &u.track
	f := Frame{Duration: u.duration, Keyframe: t.Kind == Audio || id == 0, Data: payload}
	var seen [5]bool
	var timescale uint64 = DefaultTimescale
	var config []byte
	for i := range props {
		p := &props[i]
		slot := -1
		switch p.Type {
		case PropertyTimescale:
			slot = 0
		case PropertyAudioLevel:
			slot = 1
		case PropertyVideoConfig:
			slot = 2
		case PropertyAudioConfig:
			slot = 3
		case PropertyTimestamp:
			slot = 4
		}
		if slot < 0 {
			continue
		}
		if seen[slot] {
			return Frame{}, &PropertyError{Type: p.Type, Err: ErrDuplicateProperty}
		}
		seen[slot] = true
		malformed := &PropertyError{Type: p.Type, Err: ErrMalformedProperty}
		if p.Type%2 == 0 && len(p.Bytes) != 0 || p.Type%2 == 1 && p.Value != 0 {
			return Frame{}, malformed
		}
		switch p.Type {
		case PropertyTimescale:
			timescale = p.Value
		case PropertyAudioLevel:
			if t.Kind != Audio || p.Value > 0xFF {
				return Frame{}, malformed
			}
			f.AudioLevel = &AudioLevel{Voice: p.Value&0x80 != 0, Level: uint8(p.Value & 0x7F)}
		case PropertyVideoConfig, PropertyAudioConfig:
			if p.Type != configProperty(t.Kind) {
				return Frame{}, malformed
			}
			config = p.Bytes
		case PropertyTimestamp:
			f.Timestamp = p.Value
		}
	}
	if !seen[4] {
		return Frame{}, &PropertyError{Type: PropertyTimestamp, Err: ErrMissingProperty}
	}
	if timescale != t.Timescale {
		if !seen[0] {
			return Frame{}, &PropertyError{Type: PropertyTimescale, Err: ErrMissingProperty}
		}
		return Frame{}, &PropertyError{Type: PropertyTimescale, Err: ErrPropertyMismatch}
	}
	cfg := configProperty(t.Kind)
	hasConfig := seen[2] || seen[3]
	if hasConfig && !bytes.Equal(config, t.Config) {
		return Frame{}, &PropertyError{Type: cfg, Err: ErrPropertyMismatch}
	}
	if !hasConfig && len(t.Config) > 0 && id == 0 {
		return Frame{}, &PropertyError{Type: cfg, Err: ErrMissingProperty}
	}
	if err := validateFrame(t, &f); err != nil {
		return Frame{}, err
	}
	return f, nil
}
