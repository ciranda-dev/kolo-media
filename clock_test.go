package kolomedia

import (
	"errors"
	"math"
	"math/big"
	"math/rand/v2"
	"testing"
	"time"
)

func TestRescale(t *testing.T) {
	cases := []struct {
		ts, from, to, want uint64
	}{
		{0, 48000, 90000, 0},
		{48000, 48000, 90000, 90000},
		{960, 48000, 1_000_000, 20000},
		{1, 90000, 1_000_000, 11}, // 11.11 rounds down
		{1, 3, 2, 1},              // 0.67 rounds up
		{1, 2, 1, 1},              // 0.5 rounds half up
		{1, 4, 1, 0},              // 0.25 rounds down
		{math.MaxUint64, 7, 7, math.MaxUint64},
		{math.MaxUint64, 1_000_000, 1, 18446744073710},
		{1 << 62, 2, 4, 1 << 63},
	}
	for _, c := range cases {
		got, err := Rescale(c.ts, c.from, c.to)
		if err != nil || got != c.want {
			t.Errorf("Rescale(%d, %d, %d) = %d, %v; want %d", c.ts, c.from, c.to, got, err, c.want)
		}
	}
}

func TestRescaleErrors(t *testing.T) {
	if _, err := Rescale(1, 0, 1); !errors.Is(err, ErrInvalidTimescale) {
		t.Errorf("from 0: %v", err)
	}
	if _, err := Rescale(1, 1, 0); !errors.Is(err, ErrInvalidTimescale) {
		t.Errorf("to 0: %v", err)
	}
	if _, err := Rescale(math.MaxUint64, 1, 2); !errors.Is(err, ErrTimestampRange) {
		t.Errorf("overflow: %v", err)
	}
	// Adding half of from for rounding does not overflow at the top of the range.
	if got, err := Rescale(math.MaxUint64, 3, 3); err != nil || got != math.MaxUint64 {
		t.Errorf("same scale: %d, %v", got, err)
	}
	if _, err := Rescale(1<<63, 1, 2); !errors.Is(err, ErrTimestampRange) {
		t.Errorf("2^64: %v", err)
	}
}

func TestDurationConversions(t *testing.T) {
	d, err := ToDuration(90000, 90000)
	if err != nil || d != time.Second {
		t.Fatalf("ToDuration = %v, %v", d, err)
	}
	if _, err := ToDuration(math.MaxInt64, 1_000_000_000); err != nil {
		t.Errorf("largest duration: %v", err)
	}
	if _, err := ToDuration(math.MaxInt64+1, 1_000_000_000); !errors.Is(err, ErrTimestampRange) {
		t.Errorf("past the largest duration: %v", err)
	}
	if _, err := ToDuration(math.MaxUint64, 1); !errors.Is(err, ErrTimestampRange) {
		t.Errorf("overflow: %v", err)
	}
	if _, err := ToDuration(1, 0); !errors.Is(err, ErrInvalidTimescale) {
		t.Errorf("timescale 0: %v", err)
	}
	if _, err := FromDuration(-1, 48000); !errors.Is(err, ErrTimestampRange) {
		t.Errorf("negative: %v", err)
	}
	ts, err := FromDuration(20*time.Millisecond, 48000)
	if err != nil || ts != 960 {
		t.Errorf("FromDuration = %d, %v", ts, err)
	}
	// Ticks survive a round trip through a duration for any timescale up to 1 GHz.
	r := rand.New(rand.NewPCG(1, 2))
	for _, scale := range []uint64{1, 1000, 8000, 44100, 48000, 90000, 1_000_000, 1_000_000_000} {
		for range 1000 {
			ts := r.Uint64N(math.MaxInt64 / max(1_000_000_000/scale, 1))
			d, err := ToDuration(ts, scale)
			if err != nil {
				t.Fatalf("ToDuration(%d, %d): %v", ts, scale, err)
			}
			back, err := FromDuration(d, scale)
			if err != nil || back != ts {
				t.Fatalf("round trip %d at %d/s = %d, %v", ts, scale, back, err)
			}
		}
	}
}

func TestClock(t *testing.T) {
	start := time.Unix(1_800_000_000, 123_456_789)
	c := &Clock{start: start}
	if !c.Start().Equal(start) {
		t.Fatal("Start")
	}
	us, err := c.At(start, DefaultTimescale)
	// Unix microseconds, rounded to the nearest.
	if want := uint64(1_800_000_000_123_457); err != nil || us != want {
		t.Fatalf("At(start) = %d, %v; want %d", us, err, want)
	}
	// Audio and video of one capture instant land on the same media time.
	capture := start.Add(1234567 * time.Microsecond)
	a, err := c.At(capture, 48000)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.After(capture.Sub(start), 90000)
	if err != nil {
		t.Fatal(err)
	}
	da, _ := ToDuration(a, 48000)
	dv, _ := ToDuration(v, 90000)
	if diff := (da - dv).Abs(); diff > time.Second/48000 {
		t.Errorf("audio %v and video %v differ by %v", da, dv, diff)
	}
	later, _ := c.After(time.Second, DefaultTimescale)
	if later-us != 1_000_000 {
		t.Errorf("one second later is %d us later", later-us)
	}
	// A capture slightly before the start is fine.
	if _, err := c.After(-time.Second, DefaultTimescale); err != nil {
		t.Errorf("before start: %v", err)
	}
	if _, err := c.After(-time.Duration(start.UnixNano())-1, DefaultTimescale); !errors.Is(err, ErrTimestampRange) {
		t.Errorf("before the epoch: %v", err)
	}
	if _, err := c.After(math.MaxInt64, DefaultTimescale); !errors.Is(err, ErrTimestampRange) {
		t.Errorf("overflow: %v", err)
	}
	if _, err := (&Clock{}).After(0, DefaultTimescale); !errors.Is(err, ErrTimestampRange) {
		t.Errorf("zero Clock: %v", err)
	}
	if _, err := c.After(0, 0); !errors.Is(err, ErrInvalidTimescale) {
		t.Errorf("timescale 0: %v", err)
	}
	wall := time.Now().UnixMicro()
	ts, err := NewClock().At(time.Now(), DefaultTimescale)
	if err != nil || int64(ts) < wall || int64(ts) > wall+int64(time.Minute/time.Microsecond) {
		t.Errorf("NewClock().At(now) = %d, %v; wall clock %d", ts, err, wall)
	}
}

func FuzzRescale(f *testing.F) {
	f.Add(uint64(960), uint64(48000), uint64(90000))
	f.Add(uint64(math.MaxUint64), uint64(1), uint64(2))
	f.Add(uint64(1), uint64(2), uint64(1))
	f.Fuzz(func(t *testing.T, ts, from, to uint64) {
		got, err := Rescale(ts, from, to)
		if from == 0 || to == 0 {
			if !errors.Is(err, ErrInvalidTimescale) {
				t.Fatalf("zero timescale: %v", err)
			}
			return
		}
		// want = floor((ts*to + from/2) / from)
		want := new(big.Int).Mul(new(big.Int).SetUint64(ts), new(big.Int).SetUint64(to))
		want.Add(want, new(big.Int).SetUint64(from/2))
		want.Quo(want, new(big.Int).SetUint64(from))
		if !want.IsUint64() {
			if !errors.Is(err, ErrTimestampRange) {
				t.Fatalf("Rescale(%d, %d, %d) = %d, %v; want range error", ts, from, to, got, err)
			}
			return
		}
		if err != nil || got != want.Uint64() {
			t.Fatalf("Rescale(%d, %d, %d) = %d, %v; want %d", ts, from, to, got, err, want.Uint64())
		}
	})
}
