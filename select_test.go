package kolomedia

import (
	"errors"
	"testing"
)

func TestSelect(t *testing.T) {
	c := testCatalog()
	all := []Codec{Opus, PCMS16LE, VP8, VP9}
	s, err := Select(c, Capabilities{Codecs: all})
	if err != nil {
		t.Fatal(err)
	}
	// The pcm track has priority 0 and the opus track 1: the lower number wins.
	if s.Audio.Name != "pcm" || s.Video.Name != "cam" {
		t.Errorf("selected %s and %s", s.Audio.Name, s.Video.Name)
	}
	s.Video.Config[0] = 99
	if c.Tracks[1].Config[0] == 99 {
		t.Error("selection shares the catalog's config")
	}

	cases := []struct {
		name         string
		caps         Capabilities
		audio, video string
	}{
		{"opus only", Capabilities{Codecs: []Codec{Opus, VP8}}, "mic", "cam"},
		{"size limit picks the low rendition", Capabilities{Codecs: all, MaxWidth: 640}, "pcm", "cam-low"},
		{"height limit", Capabilities{Codecs: all, MaxHeight: 200}, "pcm", "cam-low"},
		{"frame rate limit", Capabilities{Codecs: all, MaxFrameRate: 20}, "pcm", "cam-low"},
		{"bitrate limit", Capabilities{Codecs: all, MaxBitrate: 200_000}, "pcm", "cam-low"},
		{"no vp8 leaves vp9", Capabilities{Codecs: []Codec{Opus, VP9}}, "mic", "screen"},
		{"group", Capabilities{Codecs: all, VideoGroup: "camera", MaxWidth: 500}, "pcm", "cam-low"},
		{"unknown group", Capabilities{Codecs: all, VideoGroup: "nope"}, "pcm", ""},
		{"audio only", Capabilities{Codecs: []Codec{Opus}}, "mic", ""},
		{"video only", Capabilities{Codecs: []Codec{VP9}}, "", "screen"},
	}
	for _, tc := range cases {
		s, err := Select(c, tc.caps)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if name(s.Audio) != tc.audio || name(s.Video) != tc.video {
			t.Errorf("%s: selected %q and %q, want %q and %q", tc.name, name(s.Audio), name(s.Video), tc.audio, tc.video)
		}
	}
}

func name(t *Track) string {
	if t == nil {
		return ""
	}
	return t.Name
}

func TestSelectRanking(t *testing.T) {
	v := func(n string, prio uint8, w, h uint32, fps float64, br uint64) Track {
		tr := videoTrack()
		tr.Name, tr.Priority, tr.Width, tr.Height, tr.FrameRate, tr.Bitrate, tr.Group = n, prio, w, h, fps, br, ""
		return tr
	}
	a := func(n string, br uint64, rate uint32, ch uint8) Track {
		tr := audioTrack()
		tr.Name, tr.Bitrate, tr.SampleRate, tr.Channels, tr.Priority = n, br, rate, ch, 0
		return tr
	}
	caps := Capabilities{Codecs: []Codec{Opus, VP8}}
	cases := []struct {
		tracks []Track
		want   string
	}{
		{[]Track{v("big", 5, 1920, 1080, 30, 0), v("urgent", 1, 320, 180, 15, 0)}, "urgent"},
		{[]Track{v("small", 1, 320, 180, 60, 0), v("big", 1, 1920, 1080, 30, 0)}, "big"},
		{[]Track{v("slow", 1, 640, 360, 15, 0), v("fast", 1, 640, 360, 30, 0)}, "fast"},
		{[]Track{v("thin", 1, 640, 360, 30, 100), v("rich", 1, 640, 360, 30, 900)}, "rich"},
		{[]Track{v("first", 1, 640, 360, 30, 100), v("second", 1, 640, 360, 30, 100)}, "first"},
		{[]Track{a("thin", 16000, 48000, 1), a("rich", 64000, 16000, 1)}, "rich"},
		{[]Track{a("narrow", 0, 16000, 2), a("wide", 0, 48000, 1)}, "wide"},
		{[]Track{a("mono", 0, 48000, 1), a("stereo", 0, 48000, 2)}, "stereo"},
	}
	for i, tc := range cases {
		s, err := Select(&Catalog{Tracks: tc.tracks}, caps)
		if err != nil {
			t.Fatal(err)
		}
		got := name(s.Video)
		if got == "" {
			got = name(s.Audio)
		}
		if got != tc.want {
			t.Errorf("case %d: picked %q, want %q", i, got, tc.want)
		}
	}
}

func TestSelectNoMatch(t *testing.T) {
	c := testCatalog()
	for _, caps := range []Capabilities{
		{},
		{Codecs: []Codec{"h264"}},
		{Codecs: []Codec{VP8, VP9}, MaxWidth: 100},
		{Codecs: []Codec{Opus}, MaxBitrate: 1000},
	} {
		if s, err := Select(c, caps); !errors.Is(err, ErrNoMatch) || s.Audio != nil || s.Video != nil {
			t.Errorf("%+v: %+v, %v", caps, s, err)
		}
	}
	if _, err := Select(nil, Capabilities{Codecs: []Codec{Opus}}); !errors.Is(err, ErrNoMatch) {
		t.Errorf("nil catalog: %v", err)
	}
}
