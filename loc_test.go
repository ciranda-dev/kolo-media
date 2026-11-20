package kolomedia

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func mustPackager(t *testing.T, tr Track, opts PackagerOptions) *Packager {
	t.Helper()
	p, err := NewPackager(tr, opts)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func mustUnpackager(t *testing.T, tr Track) *Unpackager {
	t.Helper()
	u, err := NewUnpackager(tr)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestPackVideoGroups(t *testing.T) {
	tr := videoTrack()
	tr.Config = []byte{9, 9}
	p := mustPackager(t, tr, PackagerOptions{})
	if _, err := p.Pack(Frame{Timestamp: 1, Data: []byte{1}}); !errors.Is(err, ErrNeedKeyframe) {
		t.Fatalf("delta first: %v", err)
	}
	keys := []bool{true, false, false, true, false, true, true}
	want := [][2]uint64{{0, 0}, {0, 1}, {0, 2}, {1, 0}, {1, 1}, {2, 0}, {3, 0}}
	for i, key := range keys {
		data := []byte{byte(i), 1, 2}
		o, err := p.Pack(Frame{Timestamp: uint64(1000 + 3000*i), Keyframe: key, Data: data})
		if err != nil {
			t.Fatal(err)
		}
		if o.Group != want[i][0] || o.ID != want[i][1] {
			t.Errorf("frame %d at group %d object %d, want %v", i, o.Group, o.ID, want[i])
		}
		if &o.Payload[0] != &data[0] {
			t.Errorf("frame %d: payload is not the frame data", i)
		}
		props := []Property{{Type: PropertyTimescale, Value: 90000}}
		if key {
			props = append(props, Property{Type: PropertyVideoConfig, Bytes: tr.Config})
		}
		props = append(props, Property{Type: PropertyTimestamp, Value: uint64(1000 + 3000*i)})
		if !reflect.DeepEqual(o.Properties, props) {
			t.Errorf("frame %d properties %+v, want %+v", i, o.Properties, props)
		}
	}
}

func TestPackAudioGroups(t *testing.T) {
	tr := audioTrack()
	p := mustPackager(t, tr, PackagerOptions{})
	for i := range 4 {
		o, err := p.Pack(Frame{Timestamp: uint64(960 * i), Data: []byte{1}})
		if err != nil {
			t.Fatal(err)
		}
		if o.Group != uint64(i) || o.ID != 0 {
			t.Errorf("default grouping: frame %d at %d/%d", i, o.Group, o.ID)
		}
	}
	p = mustPackager(t, tr, PackagerOptions{AudioGroupDuration: 100 * time.Millisecond})
	for i := range 12 {
		o, err := p.Pack(Frame{Timestamp: uint64(960 * i), Data: []byte{1}})
		if err != nil {
			t.Fatal(err)
		}
		if o.Group != uint64(i/5) || o.ID != uint64(i%5) {
			t.Errorf("100 ms groups: frame %d at %d/%d", i, o.Group, o.ID)
		}
	}
	if _, err := NewPackager(tr, PackagerOptions{AudioGroupDuration: -1}); !errors.Is(err, ErrTimestampRange) {
		t.Errorf("negative group duration: %v", err)
	}
}

func TestPackProperties(t *testing.T) {
	tr := audioTrack()
	tr.Timescale = DefaultTimescale
	tr.Config = []byte{7}
	p := mustPackager(t, tr, PackagerOptions{})
	o, err := p.Pack(Frame{Timestamp: 5, Data: []byte{1}, AudioLevel: &AudioLevel{Voice: true, Level: 30}})
	if err != nil {
		t.Fatal(err)
	}
	want := []Property{
		{Type: PropertyAudioLevel, Value: 0x80 | 30},
		{Type: PropertyAudioConfig, Bytes: []byte{7}},
		{Type: PropertyTimestamp, Value: 5},
	}
	if !reflect.DeepEqual(o.Properties, want) {
		t.Errorf("properties %+v, want %+v", o.Properties, want)
	}
	for i := 1; i < len(o.Properties); i++ {
		if o.Properties[i-1].Type >= o.Properties[i].Type {
			t.Error("properties not in ascending type order")
		}
	}
	o, _ = p.Pack(Frame{Timestamp: 6, Data: []byte{1}, AudioLevel: &AudioLevel{Level: 127}})
	if !reflect.DeepEqual(o.Properties[0], Property{Type: PropertyAudioLevel, Value: 127}) {
		t.Errorf("silent level %+v", o.Properties[0])
	}
}

func TestPackErrors(t *testing.T) {
	a := mustPackager(t, audioTrack(), PackagerOptions{})
	v := mustPackager(t, videoTrack(), PackagerOptions{})
	pcm := audioTrack()
	pcm.Codec, pcm.Channels, pcm.FrameDuration = PCMS16LE, 2, 10*time.Millisecond
	pc := mustPackager(t, pcm, PackagerOptions{})
	cases := []struct {
		name string
		p    *Packager
		f    Frame
		want error
	}{
		{"empty data", a, Frame{Timestamp: 1}, ErrInvalidFrame},
		{"level over 127", a, Frame{Timestamp: 1, Data: []byte{1}, AudioLevel: &AudioLevel{Level: 128}}, ErrInvalidFrame},
		{"level on video", v, Frame{Timestamp: 1, Keyframe: true, Data: []byte{1}, AudioLevel: &AudioLevel{}}, ErrInvalidFrame},
		{"timestamp plus duration overflows", a, Frame{Timestamp: math.MaxUint64, Duration: 1, Data: []byte{1}}, ErrInvalidFrame},
		{"partial pcm sample", pc, Frame{Timestamp: 1, Data: []byte{1, 2, 3}}, ErrInvalidFrame},
	}
	for _, c := range cases {
		if _, err := c.p.Pack(c.f); !errors.Is(err, c.want) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	if _, err := pc.Pack(Frame{Timestamp: 1, Data: []byte{1, 2, 3, 4}}); err != nil {
		t.Errorf("whole pcm sample: %v", err)
	}
	if _, err := a.Pack(Frame{Timestamp: 10, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	for _, ts := range []uint64{10, 9} {
		if _, err := a.Pack(Frame{Timestamp: ts, Data: []byte{1}}); !errors.Is(err, ErrTimestampOrder) {
			t.Errorf("timestamp %d after 10: %v", ts, err)
		}
	}
	bad := audioTrack()
	bad.Channels = 0
	if _, err := NewPackager(bad, PackagerOptions{}); !errors.Is(err, ErrInvalidTrack) {
		t.Errorf("invalid track: %v", err)
	}
	if _, err := NewUnpackager(bad); !errors.Is(err, ErrInvalidTrack) {
		t.Errorf("invalid track: %v", err)
	}
}

func TestLOCRoundTrip(t *testing.T) {
	for _, tr := range []Track{audioTrack(), videoTrack()} {
		tr.Config = []byte{4, 5}
		p := mustPackager(t, tr, PackagerOptions{})
		u := mustUnpackager(t, tr)
		for i := range 5 {
			f := Frame{Timestamp: uint64(100 * i), Keyframe: tr.Kind == Audio || i%3 == 0, Data: []byte{byte(i), 9}}
			if tr.Kind == Audio && i%2 == 0 {
				f.AudioLevel = &AudioLevel{Voice: i == 2, Level: uint8(10 * i)}
			}
			o, err := p.Pack(f)
			if err != nil {
				t.Fatal(err)
			}
			got, err := u.Unpack(o.ID, o.Properties, o.Payload)
			if err != nil {
				t.Fatalf("%s frame %d: %v", tr.Kind, i, err)
			}
			f.Duration = 960
			if tr.Kind == Video {
				f.Duration = 3000
			}
			if !reflect.DeepEqual(got, f) {
				t.Errorf("%s frame %d: got %+v, want %+v", tr.Kind, i, got, f)
			}
		}
	}
	tr := videoTrack()
	tr.FrameRate = 0
	f, err := mustUnpackager(t, tr).Unpack(0, []Property{{Type: PropertyTimescale, Value: 90000}, {Type: PropertyTimestamp, Value: 1}}, []byte{1})
	if err != nil || f.Duration != 0 {
		t.Errorf("unknown frame rate: duration %d, %v", f.Duration, err)
	}
}

func TestUnpackMalformed(t *testing.T) {
	video := videoTrack()
	video.Config = []byte{1, 2}
	vu := mustUnpackager(t, video)
	au := mustUnpackager(t, audioTrack())
	ts := Property{Type: PropertyTimestamp, Value: 1}
	vscale := Property{Type: PropertyTimescale, Value: 90000}
	ascale := Property{Type: PropertyTimescale, Value: 48000}
	vcfg := Property{Type: PropertyVideoConfig, Bytes: []byte{1, 2}}
	cases := []struct {
		name  string
		u     *Unpackager
		id    uint64
		props []Property
		typ   uint64
		want  error
	}{
		{"no timestamp", au, 0, []Property{ascale}, PropertyTimestamp, ErrMissingProperty},
		{"two timestamps", au, 0, []Property{ascale, ts, ts}, PropertyTimestamp, ErrDuplicateProperty},
		{"two timescales", au, 0, []Property{ascale, ascale, ts}, PropertyTimescale, ErrDuplicateProperty},
		{"timestamp with bytes", au, 0, []Property{ascale, {Type: PropertyTimestamp, Bytes: []byte{1}}}, PropertyTimestamp, ErrMalformedProperty},
		{"config with value", vu, 0, []Property{vscale, {Type: PropertyVideoConfig, Value: 1}, ts}, PropertyVideoConfig, ErrMalformedProperty},
		{"level over 0xFF", au, 0, []Property{ascale, {Type: PropertyAudioLevel, Value: 0x100}, ts}, PropertyAudioLevel, ErrMalformedProperty},
		{"level on video", vu, 1, []Property{vscale, {Type: PropertyAudioLevel, Value: 1}, ts}, PropertyAudioLevel, ErrMalformedProperty},
		{"video config on audio", au, 0, []Property{ascale, {Type: PropertyVideoConfig, Bytes: []byte{1}}, ts}, PropertyVideoConfig, ErrMalformedProperty},
		{"audio config on video", vu, 1, []Property{vscale, {Type: PropertyAudioConfig, Bytes: []byte{1}}, ts}, PropertyAudioConfig, ErrMalformedProperty},
		{"other timescale", vu, 1, []Property{{Type: PropertyTimescale, Value: 1000}, ts}, PropertyTimescale, ErrPropertyMismatch},
		{"no timescale", vu, 1, []Property{ts}, PropertyTimescale, ErrMissingProperty},
		{"timescale on a default track", mustUnpackager(t, Track{Name: "a", Kind: Audio, Codec: Opus, Timescale: DefaultTimescale, SampleRate: 48000, Channels: 2, FrameDuration: 10 * time.Millisecond}), 0, []Property{ascale, ts}, PropertyTimescale, ErrPropertyMismatch},
		{"other config", vu, 0, []Property{vscale, {Type: PropertyVideoConfig, Bytes: []byte{1, 3}}, ts}, PropertyVideoConfig, ErrPropertyMismatch},
		{"empty config", vu, 0, []Property{vscale, {Type: PropertyVideoConfig}, ts}, PropertyVideoConfig, ErrPropertyMismatch},
		{"keyframe without config", vu, 0, []Property{vscale, ts}, PropertyVideoConfig, ErrMissingProperty},
	}
	for _, c := range cases {
		_, err := c.u.Unpack(c.id, c.props, []byte{1})
		var pe *PropertyError
		if !errors.As(err, &pe) || pe.Type != c.typ || !errors.Is(err, c.want) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	if _, err := vu.Unpack(2, []Property{vscale, vcfg, ts}, []byte{1}); err != nil {
		t.Errorf("config on a delta frame: %v", err)
	}
	if _, err := vu.Unpack(2, []Property{vscale, ts}, nil); !errors.Is(err, ErrInvalidFrame) {
		t.Errorf("empty payload: %v", err)
	}
	unknown := []Property{{Type: 0x7F, Bytes: []byte{1, 2}}, ascale, {Type: 0x78, Value: 9}, ts, {Type: 0x3800, Value: 1}}
	if f, err := au.Unpack(0, unknown, []byte{1}); err != nil || f.Timestamp != 1 {
		t.Errorf("unknown properties: %+v, %v", f, err)
	}
	f, err := vu.Unpack(3, []Property{vscale, ts}, []byte{1})
	if err != nil || f.Keyframe {
		t.Errorf("delta frame: %+v, %v", f, err)
	}
	pcm := audioTrack()
	pcm.Codec, pcm.Channels, pcm.FrameDuration = PCMS16LE, 1, 10*time.Millisecond
	if _, err := mustUnpackager(t, pcm).Unpack(0, []Property{ascale, ts}, []byte{1}); !errors.Is(err, ErrInvalidFrame) {
		t.Errorf("partial pcm sample: %v", err)
	}
	if _, err := au.Unpack(0, []Property{ascale, {Type: PropertyTimestamp, Value: math.MaxUint64}}, []byte{1}); !errors.Is(err, ErrInvalidFrame) {
		t.Errorf("timestamp plus duration overflows: %v", err)
	}
	pe := &PropertyError{Type: PropertyTimestamp, Err: ErrMissingProperty}
	if pe.Error() != "kolomedia: property 0x10: missing property" {
		t.Errorf("message %q", pe.Error())
	}
	if msg := (&PropertyError{Type: 9}).Error(); msg != "kolomedia: property 0x9" {
		t.Errorf("message without Err %q", msg)
	}
}

func FuzzUnpack(f *testing.F) {
	f.Add(uint64(0), []byte{0x08, 0x10, 0x10, 0x01}, []byte{1})
	f.Add(uint64(1), []byte{0x0D, 0x02, 0x0C, 0xFF, 0x10, 0x00}, []byte{})
	video := videoTrack()
	video.Config = []byte{1}
	var units []*Unpackager
	for _, tr := range []Track{audioTrack(), video} {
		u, err := NewUnpackager(tr)
		if err != nil {
			f.Fatal(err)
		}
		units = append(units, u)
	}
	f.Fuzz(func(t *testing.T, id uint64, raw, payload []byte) {
		// raw is a list of (type byte, value byte); odd types take the value as a length of following bytes.
		var props []Property
		for len(raw) >= 2 {
			p := Property{Type: uint64(raw[0])}
			if p.Type%2 == 0 {
				p.Value = uint64(raw[1]) * 1000
				raw = raw[2:]
			} else {
				n := min(int(raw[1]), len(raw)-2)
				p.Bytes = raw[2 : 2+n]
				raw = raw[2+n:]
			}
			props = append(props, p)
		}
		for _, u := range units {
			fr, err := u.Unpack(id, props, payload)
			if err != nil {
				continue
			}
			if !bytes.Equal(fr.Data, payload) || len(fr.Data) == 0 {
				t.Fatal("frame data is not the payload")
			}
			var tsCount int
			for _, p := range props {
				if p.Type == PropertyTimestamp {
					tsCount++
					if p.Value != fr.Timestamp {
						t.Fatal("timestamp differs from the property")
					}
				}
			}
			if tsCount != 1 {
				t.Fatalf("accepted %d timestamps", tsCount)
			}
		}
	})
}
