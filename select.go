package kolomedia

import "slices"

// Capabilities describes what a receiver can decode. A zero limit means no limit.
type Capabilities struct {
	// Codecs lists the codecs the receiver decodes. Without an audio or video codec, that kind is not selected.
	Codecs []Codec
	// MaxWidth, MaxHeight and MaxFrameRate bound video tracks.
	MaxWidth     uint32
	MaxHeight    uint32
	MaxFrameRate float64
	// MaxBitrate bounds each selected track. A track of unknown bitrate passes.
	MaxBitrate uint64
	// VideoGroup, if set, limits video to the tracks of this rendition group.
	VideoGroup string
}

// Selection holds the tracks to subscribe to. A kind with no matching track is nil.
type Selection struct {
	Audio *Track
	Video *Track
}

// Select picks at most one audio and one video track of c that fit caps, preferring the lowest Priority
// number, then the highest quality, then catalog order. It returns ErrNoMatch if it picks nothing.
func Select(c *Catalog, caps Capabilities) (Selection, error) {
	var s Selection
	if c == nil {
		return s, ErrNoMatch
	}
	for i := range c.Tracks {
		t := &c.Tracks[i]
		if !caps.fits(t) {
			continue
		}
		best := &s.Audio
		if t.Kind == Video {
			best = &s.Video
		}
		if *best == nil || better(t, *best) {
			*best = t
		}
	}
	if s.Audio == nil && s.Video == nil {
		return s, ErrNoMatch
	}
	for _, p := range []**Track{&s.Audio, &s.Video} {
		if *p != nil {
			t := **p
			t.Config = slices.Clone(t.Config)
			*p = &t
		}
	}
	return s, nil
}

func (caps *Capabilities) fits(t *Track) bool {
	if !slices.Contains(caps.Codecs, t.Codec) || caps.MaxBitrate > 0 && t.Bitrate > caps.MaxBitrate {
		return false
	}
	if t.Kind == Audio {
		return true
	}
	return (caps.MaxWidth == 0 || t.Width <= caps.MaxWidth) &&
		(caps.MaxHeight == 0 || t.Height <= caps.MaxHeight) &&
		(caps.MaxFrameRate == 0 || t.FrameRate <= caps.MaxFrameRate) &&
		(caps.VideoGroup == "" || t.Group == caps.VideoGroup)
}

// better reports whether a is preferred over b, two tracks of the same kind.
func better(a, b *Track) bool {
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	if a.Kind == Video {
		if pa, pb := uint64(a.Width)*uint64(a.Height), uint64(b.Width)*uint64(b.Height); pa != pb {
			return pa > pb
		}
		if a.FrameRate != b.FrameRate {
			return a.FrameRate > b.FrameRate
		}
		return a.Bitrate > b.Bitrate
	}
	if a.Bitrate != b.Bitrate {
		return a.Bitrate > b.Bitrate
	}
	if a.SampleRate != b.SampleRate {
		return a.SampleRate > b.SampleRate
	}
	return a.Channels > b.Channels
}
