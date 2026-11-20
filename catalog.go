package kolomedia

import (
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	kolocodec "github.com/ciranda-dev/kolo-codec"
)

// CatalogTrack is the name of the track that carries a broadcast's catalog: one group per
// catalog version, with the encoded catalog as its only object.
const CatalogTrack = "catalog"

const catalogFormat = 1

// Kind is the media kind of a track.
type Kind uint8

// Track kinds.
const (
	Audio Kind = 1
	Video Kind = 2
)

// String returns "audio", "video" or "unknown".
func (k Kind) String() string {
	switch k {
	case Audio:
		return "audio"
	case Video:
		return "video"
	}
	return "unknown"
}

// Codec names the encoding of a track's frames.
type Codec string

// Supported codecs.
const (
	Opus     Codec = "opus"
	PCMS16LE Codec = "pcm-s16le"
	VP8      Codec = "vp8"
	VP9      Codec = "vp9"
)

const maxCodecLen = len(PCMS16LE)

func (c Codec) kind() Kind {
	switch c {
	case Opus, PCMS16LE:
		return Audio
	case VP8, VP9:
		return Video
	}
	return 0
}

// Track describes one track of a broadcast.
type Track struct {
	// Name is the MoQ track name: not empty, unique in the catalog and not CatalogTrack.
	Name string
	// Kind is Audio or Video and must match the codec.
	Kind Kind
	// Codec is one of the supported codecs.
	Codec Codec
	// Timescale is the number of timestamp units per second, usually DefaultTimescale.
	Timescale uint64
	// Priority is the MoQ publisher priority: a lower number is sent first.
	Priority uint8
	// Group names a rendition group: tracks with the same non-empty Group are alternatives of one source.
	Group string
	// Config is the optional codec configuration a decoder needs.
	Config []byte
	// Bitrate is in bits per second, 0 if unknown.
	Bitrate uint64

	// SampleRate, Channels and FrameDuration describe an audio track and are required for one.
	SampleRate    uint32
	Channels      uint8
	FrameDuration time.Duration

	// Width and Height in pixels are required for a video track; FrameRate in frames per second is 0 if unknown.
	Width     uint32
	Height    uint32
	FrameRate float64
}

// Catalog describes one broadcast. Version increases with every change.
type Catalog struct {
	Version uint64
	Tracks  []Track
}

// CatalogLimits bounds what ParseCatalog accepts. A field outside 1 to its default takes the default,
// so whatever ParseCatalog accepts, MarshalBinary encodes again.
type CatalogLimits struct {
	// MaxTracks defaults to 1024, the renditions per broadcast a player keeps.
	MaxTracks int
	// MaxNameLen bounds track and group names in bytes. It defaults to 4096, the MoQ bound on a full track name.
	MaxNameLen int
	// MaxConfigLen defaults to 65508, the largest config that fits one object property.
	MaxConfigLen int
}

// defaultMaxConfigLen is MoQ's 65535-byte limit on all properties of one object, less the most the
// other properties of a group's first object take: TIMESCALE (1 + 9 bytes), AUDIO_LEVEL (1 + 2),
// the config's type and 3-byte length (1 + 3) and TIMESTAMP (1 + 9).
const defaultMaxConfigLen = 65535 - 10 - 3 - 4 - 10

func (l CatalogLimits) withDefaults() CatalogLimits {
	clamp := func(v, def int) int {
		if v <= 0 || v > def {
			return def
		}
		return v
	}
	return CatalogLimits{
		MaxTracks:    clamp(l.MaxTracks, 1024),
		MaxNameLen:   clamp(l.MaxNameLen, 4096),
		MaxConfigLen: clamp(l.MaxConfigLen, defaultMaxConfigLen),
	}
}

// MarshalBinary validates the catalog against the default limits and returns its canonical encoding.
func (c *Catalog) MarshalBinary() ([]byte, error) {
	if err := c.validate(CatalogLimits{}.withDefaults()); err != nil {
		return nil, err
	}
	var e kolocodec.Encoder
	e.PutFixed([]byte{catalogFormat})
	e.PutUvarint(c.Version)
	e.PutUvarint(uint64(len(c.Tracks)))
	for i := range c.Tracks {
		t := &c.Tracks[i]
		e.PutString(t.Name)
		e.PutUvarint(uint64(t.Kind))
		e.PutString(string(t.Codec))
		e.PutUvarint(t.Timescale)
		e.PutUvarint(uint64(t.Priority))
		e.PutString(t.Group)
		e.PutBytes(t.Config)
		e.PutUvarint(t.Bitrate)
		if t.Kind == Audio {
			e.PutUvarint(uint64(t.SampleRate))
			e.PutUvarint(uint64(t.Channels))
			e.PutUvarint(uint64(t.FrameDuration))
		} else {
			e.PutUvarint(uint64(t.Width))
			e.PutUvarint(uint64(t.Height))
			e.PutUint64(math.Float64bits(t.FrameRate))
		}
	}
	return e.Bytes(), nil
}

// ParseCatalog decodes and validates an encoded catalog. It accepts only the canonical encoding,
// never panics and allocates in proportion to len(b).
func ParseCatalog(b []byte, lim CatalogLimits) (*Catalog, error) {
	lim = lim.withDefaults()
	d := kolocodec.NewDecoder(b)
	format, err := d.Fixed(1)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCatalog, err)
	}
	if format[0] != catalogFormat {
		return nil, fmt.Errorf("%w: version %d", ErrCatalogFormat, format[0])
	}
	c := &Catalog{}
	if c.Version, err = d.Uvarint(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCatalog, err)
	}
	n, err := d.UvarintMax(uint64(lim.MaxTracks))
	if err != nil {
		return nil, fmt.Errorf("%w: track count: %w", ErrInvalidCatalog, err)
	}
	// A valid track encodes in at least 15 bytes, so a count the input cannot hold allocates nothing.
	if n > uint64(d.Len()/15) {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCatalog, kolocodec.ErrShortBuffer)
	}
	c.Tracks = make([]Track, n)
	for i := range c.Tracks {
		if err := decodeTrack(d, &c.Tracks[i], lim); err != nil {
			return nil, fmt.Errorf("%w: track %d: %w", ErrInvalidCatalog, i, err)
		}
	}
	if err := d.Done(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCatalog, err)
	}
	if err := c.validate(lim); err != nil {
		return nil, err
	}
	return c, nil
}

func decodeTrack(d *kolocodec.Decoder, t *Track, lim CatalogLimits) error {
	var err error
	uv := func(limit uint64) uint64 {
		var v uint64
		if err == nil {
			v, err = d.UvarintMax(limit)
		}
		return v
	}
	str := func(limit int) string {
		var s string
		if err == nil {
			s, err = d.String(limit)
		}
		return s
	}
	t.Name = str(lim.MaxNameLen)
	t.Kind = Kind(uv(math.MaxUint8))
	t.Codec = Codec(str(maxCodecLen))
	t.Timescale = uv(math.MaxUint64)
	t.Priority = uint8(uv(math.MaxUint8))
	t.Group = str(lim.MaxNameLen)
	if err == nil {
		t.Config, err = d.Bytes(lim.MaxConfigLen)
	}
	if len(t.Config) == 0 {
		t.Config = nil
	}
	t.Bitrate = uv(math.MaxUint64)
	switch {
	case err != nil:
	case t.Kind == Audio:
		t.SampleRate = uint32(uv(math.MaxUint32))
		t.Channels = uint8(uv(math.MaxUint8))
		t.FrameDuration = time.Duration(uv(math.MaxInt64))
	case t.Kind == Video:
		t.Width = uint32(uv(math.MaxUint32))
		t.Height = uint32(uv(math.MaxUint32))
		if err == nil {
			var bits uint64
			bits, err = d.Uint64()
			t.FrameRate = math.Float64frombits(bits)
		}
	default:
		err = fmt.Errorf("kind %d", t.Kind)
	}
	return err
}

func (c *Catalog) validate(lim CatalogLimits) error {
	if len(c.Tracks) > lim.MaxTracks {
		return fmt.Errorf("%w: %d tracks, limit %d", ErrInvalidCatalog, len(c.Tracks), lim.MaxTracks)
	}
	names := make(map[string]int, len(c.Tracks))
	groups := make(map[string]*Track)
	for i := range c.Tracks {
		t := &c.Tracks[i]
		if err := t.validate(lim); err != nil {
			return fmt.Errorf("%w: track %d: %v", ErrInvalidCatalog, i, err)
		}
		if j, ok := names[t.Name]; ok {
			return fmt.Errorf("%w: tracks %d and %d are both named %q", ErrInvalidCatalog, j, i, t.Name)
		}
		names[t.Name] = i
		if t.Group == "" {
			continue
		}
		if g, ok := groups[t.Group]; ok && (g.Kind != t.Kind || g.Timescale != t.Timescale) {
			return fmt.Errorf("%w: group %q mixes kinds or timescales", ErrInvalidCatalog, t.Group)
		} else if !ok {
			groups[t.Group] = t
		}
	}
	return nil
}

func (t *Track) validate(lim CatalogLimits) error {
	switch {
	case t.Name == "":
		return errors.New("empty name")
	case t.Name == CatalogTrack:
		return fmt.Errorf("name %q is reserved", CatalogTrack)
	case len(t.Name) > lim.MaxNameLen || len(t.Group) > lim.MaxNameLen:
		return fmt.Errorf("name or group over %d bytes", lim.MaxNameLen)
	case !utf8.ValidString(t.Name) || !utf8.ValidString(t.Group):
		return errors.New("name or group is not UTF-8")
	case t.Kind != Audio && t.Kind != Video:
		return fmt.Errorf("kind %d", t.Kind)
	case t.Codec.kind() != t.Kind:
		return fmt.Errorf("codec %q is not a known %s codec", t.Codec, t.Kind)
	case t.Timescale == 0:
		return errors.New("timescale is 0")
	case len(t.Config) > lim.MaxConfigLen:
		return fmt.Errorf("config over %d bytes", lim.MaxConfigLen)
	}
	if t.Kind == Audio {
		return t.validateAudio()
	}
	return t.validateVideo()
}

func (t *Track) validateAudio() error {
	switch {
	case t.Width != 0 || t.Height != 0 || t.FrameRate != 0:
		return errors.New("video fields set on an audio track")
	case t.SampleRate == 0 || t.Channels == 0 || t.FrameDuration <= 0:
		return errors.New("sample rate, channels and frame duration are required")
	}
	if t.Codec != Opus {
		return nil
	}
	switch t.SampleRate {
	case 8000, 12000, 16000, 24000, 48000:
	default:
		return fmt.Errorf("opus sample rate %d", t.SampleRate)
	}
	// An Opus packet holds whole frames of 2.5 to 60 ms and at most 120 ms (RFC 6716).
	const step = 2500 * time.Microsecond
	if t.FrameDuration%step != 0 || t.FrameDuration > 120*time.Millisecond {
		return fmt.Errorf("opus frame duration %v", t.FrameDuration)
	}
	return nil
}

func (t *Track) validateVideo() error {
	switch {
	case t.SampleRate != 0 || t.Channels != 0 || t.FrameDuration != 0:
		return errors.New("audio fields set on a video track")
	case t.Width == 0 || t.Height == 0:
		return errors.New("width and height are required")
	case math.Signbit(t.FrameRate) || math.IsNaN(t.FrameRate) || math.IsInf(t.FrameRate, 0):
		return fmt.Errorf("frame rate %v", t.FrameRate)
	}
	limit := uint32(65536)
	if t.Codec == VP8 {
		limit = 16383
	}
	if t.Width > limit || t.Height > limit {
		return fmt.Errorf("%s frame size %dx%d over %d", t.Codec, t.Width, t.Height, limit)
	}
	return nil
}

func checkTrack(t *Track) error {
	if err := t.validate(CatalogLimits{}.withDefaults()); err != nil {
		return fmt.Errorf("%w %q: %v", ErrInvalidTrack, t.Name, err)
	}
	return nil
}
