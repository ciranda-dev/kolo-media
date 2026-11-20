package kolomedia

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
)

func testCatalog() *Catalog {
	high := videoTrack()
	high.Config = []byte{1, 2, 3}
	low := videoTrack()
	low.Name, low.Width, low.Height, low.Bitrate, low.FrameRate = "cam-low", 320, 180, 150_000, 15
	pcm := Track{Name: "pcm", Kind: Audio, Codec: PCMS16LE, Timescale: 44100, SampleRate: 44100, Channels: 2, FrameDuration: 10 * time.Millisecond}
	vp9 := Track{Name: "screen", Kind: Video, Codec: VP9, Timescale: DefaultTimescale, Priority: 200, Width: 65536, Height: 1}
	return &Catalog{Version: 7, Tracks: []Track{audioTrack(), high, low, pcm, vp9}}
}

func TestCatalogRoundTrip(t *testing.T) {
	c := testCatalog()
	b, err := c.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseCatalog(b, CatalogLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, c) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", got, c)
	}
	again, err := got.MarshalBinary()
	if err != nil || !bytes.Equal(again, b) {
		t.Fatalf("encoding is not stable: %v", err)
	}
	empty, err := (&Catalog{}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if c, err := ParseCatalog(empty, CatalogLimits{}); err != nil || len(c.Tracks) != 0 {
		t.Fatalf("empty catalog: %v", err)
	}
}

func TestCatalogValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(c *Catalog)
	}{
		{"empty name", func(c *Catalog) { c.Tracks[0].Name = "" }},
		{"reserved name", func(c *Catalog) { c.Tracks[0].Name = CatalogTrack }},
		{"duplicate name", func(c *Catalog) { c.Tracks[1].Name = c.Tracks[0].Name }},
		{"long name", func(c *Catalog) { c.Tracks[0].Name = strings.Repeat("a", 4097) }},
		{"long group", func(c *Catalog) { c.Tracks[1].Group = strings.Repeat("a", 4097) }},
		{"invalid UTF-8", func(c *Catalog) { c.Tracks[0].Name = "\xff" }},
		{"kind 0", func(c *Catalog) { c.Tracks[0].Kind = 0 }},
		{"kind 3", func(c *Catalog) { c.Tracks[0].Kind = 3 }},
		{"unknown codec", func(c *Catalog) { c.Tracks[0].Codec = "aac" }},
		{"codec of the other kind", func(c *Catalog) { c.Tracks[0].Codec = VP8 }},
		{"timescale 0", func(c *Catalog) { c.Tracks[0].Timescale = 0 }},
		{"config too long", func(c *Catalog) { c.Tracks[0].Config = make([]byte, 65509) }},
		{"opus sample rate", func(c *Catalog) { c.Tracks[0].SampleRate = 44100 }},
		{"opus 3 ms", func(c *Catalog) { c.Tracks[0].FrameDuration = 3 * time.Millisecond }},
		{"opus 122.5 ms", func(c *Catalog) { c.Tracks[0].FrameDuration = 122500 * time.Microsecond }},
		{"audio without channels", func(c *Catalog) { c.Tracks[3].Channels = 0 }},
		{"audio without sample rate", func(c *Catalog) { c.Tracks[3].SampleRate = 0 }},
		{"audio without frame duration", func(c *Catalog) { c.Tracks[3].FrameDuration = 0 }},
		{"negative frame duration", func(c *Catalog) { c.Tracks[3].FrameDuration = -time.Millisecond }},
		{"audio with width", func(c *Catalog) { c.Tracks[0].Width = 1 }},
		{"audio with frame rate", func(c *Catalog) { c.Tracks[0].FrameRate = 1 }},
		{"video with sample rate", func(c *Catalog) { c.Tracks[1].SampleRate = 1 }},
		{"video with frame duration", func(c *Catalog) { c.Tracks[1].FrameDuration = 1 }},
		{"video without width", func(c *Catalog) { c.Tracks[1].Width = 0 }},
		{"video without height", func(c *Catalog) { c.Tracks[1].Height = 0 }},
		{"vp8 too wide", func(c *Catalog) { c.Tracks[1].Width = 16384 }},
		{"vp9 too tall", func(c *Catalog) { c.Tracks[4].Height = 65537 }},
		{"NaN frame rate", func(c *Catalog) { c.Tracks[1].FrameRate = math.NaN() }},
		{"infinite frame rate", func(c *Catalog) { c.Tracks[1].FrameRate = math.Inf(1) }},
		{"negative frame rate", func(c *Catalog) { c.Tracks[1].FrameRate = -30 }},
		{"negative zero frame rate", func(c *Catalog) { c.Tracks[1].FrameRate = math.Copysign(0, -1) }},
		{"group mixes kinds", func(c *Catalog) { c.Tracks[0].Group = "camera" }},
		{"group mixes timescales", func(c *Catalog) { c.Tracks[2].Timescale = 1000 }},
		{"too many tracks", func(c *Catalog) {
			for i := range 1020 {
				t := c.Tracks[3]
				t.Name = strings.Repeat("x", i+1)
				c.Tracks = append(c.Tracks, t)
			}
		}},
	}
	for _, tc := range cases {
		c := testCatalog()
		tc.mutate(c)
		if _, err := c.MarshalBinary(); !errors.Is(err, ErrInvalidCatalog) {
			t.Errorf("%s: MarshalBinary error %v", tc.name, err)
		}
	}
	// Limits that pass: the boundaries themselves.
	c := testCatalog()
	c.Tracks[0].Name = strings.Repeat("a", 4096)
	c.Tracks[0].Config = make([]byte, 65508)
	c.Tracks[0].FrameDuration = 120 * time.Millisecond
	c.Tracks[1].Width, c.Tracks[1].Height = 16383, 16383
	if _, err := c.MarshalBinary(); err != nil {
		t.Errorf("boundaries: %v", err)
	}
}

// catalogBytes writes a catalog header with the given format and track count, then lets track add bytes.
func catalogBytes(version byte, count uint64, track func(e *kolocodec.Encoder)) []byte {
	var e kolocodec.Encoder
	e.PutFixed([]byte{version})
	e.PutUvarint(1)
	e.PutUvarint(count)
	if track != nil {
		track(&e)
	}
	return e.Bytes()
}

func videoFields(e *kolocodec.Encoder, kind uint64, codec string, priority uint64, rate float64) {
	e.PutString("v")
	e.PutUvarint(kind)
	e.PutString(codec)
	e.PutUvarint(90000)
	e.PutUvarint(priority)
	e.PutString("")
	e.PutBytes(nil)
	e.PutUvarint(0)
	e.PutUvarint(640)
	e.PutUvarint(480)
	e.PutUint64(math.Float64bits(rate))
}

func TestParseCatalogHostile(t *testing.T) {
	good := catalogBytes(1, 1, func(e *kolocodec.Encoder) { videoFields(e, 2, "vp8", 5, 30) })
	if _, err := ParseCatalog(good, CatalogLimits{}); err != nil {
		t.Fatalf("hand-built catalog: %v", err)
	}
	for n := range len(good) {
		if _, err := ParseCatalog(good[:n], CatalogLimits{}); err == nil {
			t.Errorf("truncated to %d bytes: no error", n)
		}
	}
	if _, err := ParseCatalog(append(good, 0), CatalogLimits{}); !errors.Is(err, kolocodec.ErrTrailingBytes) {
		t.Errorf("trailing byte: %v", err)
	}
	if _, err := ParseCatalog(catalogBytes(2, 0, nil), CatalogLimits{}); !errors.Is(err, ErrCatalogFormat) {
		t.Errorf("format 2: %v", err)
	}
	if _, err := ParseCatalog([]byte{1, 0x80, 0x00, 0}, CatalogLimits{}); !errors.Is(err, kolocodec.ErrNonCanonical) {
		t.Errorf("non-minimal varint: %v", err)
	}
	if _, err := ParseCatalog(catalogBytes(1, 1000, nil), CatalogLimits{}); !errors.Is(err, kolocodec.ErrShortBuffer) {
		t.Errorf("count without tracks: %v", err)
	}
	// A track count the input cannot hold allocates nothing for the tracks.
	short := catalogBytes(1, 1024, func(e *kolocodec.Encoder) { e.PutFixed(make([]byte, 1000)) })
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 100 {
		ParseCatalog(short, CatalogLimits{})
	}
	runtime.ReadMemStats(&after)
	if n := after.TotalAlloc - before.TotalAlloc; n > 100*4096 {
		t.Errorf("100 parses of a short catalog allocated %d bytes", n)
	}
	if _, err := ParseCatalog(catalogBytes(1, 1025, nil), CatalogLimits{}); !errors.Is(err, kolocodec.ErrTooLarge) {
		t.Errorf("count over the limit: %v", err)
	}
	if _, err := ParseCatalog(catalogBytes(1, math.MaxUint64, nil), CatalogLimits{}); !errors.Is(err, ErrInvalidCatalog) {
		t.Errorf("huge count: %v", err)
	}
	bad := []struct {
		name  string
		track func(e *kolocodec.Encoder)
	}{
		{"kind 3", func(e *kolocodec.Encoder) { videoFields(e, 3, "vp8", 5, 30) }},
		{"kind 300", func(e *kolocodec.Encoder) { videoFields(e, 300, "vp8", 5, 30) }},
		{"long codec", func(e *kolocodec.Encoder) { videoFields(e, 2, "vp8-and-more", 5, 30) }},
		{"priority 256", func(e *kolocodec.Encoder) { videoFields(e, 2, "vp8", 256, 30) }},
		{"NaN frame rate", func(e *kolocodec.Encoder) { videoFields(e, 2, "vp8", 5, math.NaN()) }},
		{"audio codec on video", func(e *kolocodec.Encoder) { videoFields(e, 2, "opus", 5, 30) }},
	}
	for _, tc := range bad {
		if _, err := ParseCatalog(catalogBytes(1, 1, tc.track), CatalogLimits{}); !errors.Is(err, ErrInvalidCatalog) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	// A frame duration over the largest time.Duration.
	var e kolocodec.Encoder
	e.PutFixed([]byte{1})
	e.PutUvarint(1)
	e.PutUvarint(1)
	e.PutString("a")
	e.PutUvarint(1)
	e.PutString("pcm-s16le")
	e.PutUvarint(8000)
	e.PutUvarint(0)
	e.PutString("")
	e.PutBytes(nil)
	e.PutUvarint(0)
	e.PutUvarint(8000)
	e.PutUvarint(1)
	e.PutUvarint(math.MaxInt64 + 1)
	if _, err := ParseCatalog(e.Bytes(), CatalogLimits{}); !errors.Is(err, kolocodec.ErrTooLarge) {
		t.Errorf("frame duration overflow: %v", err)
	}
}

func TestParseCatalogLimits(t *testing.T) {
	b, err := testCatalog().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseCatalog(b, CatalogLimits{MaxTracks: 4}); !errors.Is(err, kolocodec.ErrTooLarge) {
		t.Errorf("MaxTracks: %v", err)
	}
	if _, err := ParseCatalog(b, CatalogLimits{MaxNameLen: 3}); !errors.Is(err, ErrInvalidCatalog) {
		t.Errorf("MaxNameLen: %v", err)
	}
	if _, err := ParseCatalog(b, CatalogLimits{MaxConfigLen: 2}); !errors.Is(err, kolocodec.ErrTooLarge) {
		t.Errorf("MaxConfigLen: %v", err)
	}
	if _, err := ParseCatalog(b, CatalogLimits{MaxTracks: 5, MaxNameLen: 7, MaxConfigLen: 3}); err != nil {
		t.Errorf("exact limits: %v", err)
	}
}

func TestKindString(t *testing.T) {
	if Audio.String() != "audio" || Video.String() != "video" || Kind(9).String() != "unknown" {
		t.Error("Kind.String")
	}
}

func FuzzParseCatalog(f *testing.F) {
	b, err := testCatalog().MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(b)
	f.Add(catalogBytes(1, 1, func(e *kolocodec.Encoder) { videoFields(e, 2, "vp9", 0, 0) }))
	f.Add([]byte{1, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := ParseCatalog(b, CatalogLimits{})
		if err != nil {
			if c != nil {
				t.Fatal("catalog returned with an error")
			}
			return
		}
		again, err := c.MarshalBinary()
		if err != nil {
			t.Fatalf("parsed catalog does not marshal: %v", err)
		}
		if !bytes.Equal(again, b) {
			t.Fatalf("not canonical:\n in %x\nout %x", b, again)
		}
	})
}

func TestCatalogLimitsClamp(t *testing.T) {
	c := testCatalog()
	c.Tracks[0].Name = strings.Repeat("a", 5000)
	var e kolocodec.Encoder
	e.PutFixed([]byte{1})
	e.PutUvarint(1)
	e.PutUvarint(1)
	e.PutString(strings.Repeat("a", 5000))
	if _, err := ParseCatalog(e.Bytes(), CatalogLimits{MaxNameLen: 8192}); !errors.Is(err, kolocodec.ErrTooLarge) {
		t.Errorf("a limit above the default: %v", err)
	}
	if got := (CatalogLimits{MaxTracks: -1, MaxNameLen: 9999, MaxConfigLen: 10}).withDefaults(); got != (CatalogLimits{1024, 4096, 10}) {
		t.Errorf("withDefaults = %+v", got)
	}
}
