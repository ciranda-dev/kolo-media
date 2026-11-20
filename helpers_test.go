package kolomedia

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func audioTrack() Track {
	return Track{
		Name: "mic", Kind: Audio, Codec: Opus, Timescale: 48000, Priority: 1,
		Bitrate: 32000, SampleRate: 48000, Channels: 1, FrameDuration: 20 * time.Millisecond,
	}
}

func videoTrack() Track {
	return Track{
		Name: "cam", Kind: Video, Codec: VP8, Timescale: 90000, Priority: 2, Group: "camera",
		Bitrate: 1_000_000, Width: 1280, Height: 720, FrameRate: 30,
	}
}

// fakeClock is a manual clock: time moves only on advance, which fires the timers that are due.
type fakeClock struct {
	mu     sync.Mutex
	t      time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at time.Time
	c  chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1_800_000_000, 0)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) after(d time.Duration) (<-chan time.Time, func() bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ft := &fakeTimer{at: c.t.Add(d), c: make(chan time.Time, 1)}
	if d <= 0 {
		ft.c <- c.t
		return ft.c, func() bool { return false }
	}
	c.timers = append(c.timers, ft)
	return ft.c, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i, x := range c.timers {
			if x == ft {
				c.timers = append(c.timers[:i], c.timers[i+1:]...)
				return true
			}
		}
		return false
	}
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	kept := c.timers[:0]
	for _, ft := range c.timers {
		if ft.at.After(c.t) {
			kept = append(kept, ft)
		} else {
			ft.c <- c.t
		}
	}
	c.timers = kept
}

func (c *fakeClock) waiting() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitBlocked waits until n goroutines started by the calling goroutine are blocked waiting for a
// frame. Goroutines of other tests, such as those a failed test left blocked, are not counted.
func waitBlocked(t *testing.T, n int) {
	t.Helper()
	buf := make([]byte, 64)
	self := strings.Fields(string(buf[:runtime.Stack(buf, false)]))[1]
	waitFor(t, "Next to block", func() bool {
		blocked := 0
		for _, stack := range goroutines() {
			if strings.Contains(stack, "[select") && strings.Contains(stack, "(*Playout).next(") && strings.Contains(stack, "in goroutine "+self+"\n") {
				blocked++
			}
		}
		return blocked == n
	})
}

// checkGoroutines fails the test if goroutines started after it are still running after the cleanups.
func checkGoroutines(t *testing.T) {
	t.Helper()
	before := goroutines()
	t.Cleanup(func() {
		deadline := time.Now().Add(10 * time.Second)
		for {
			var leaked []string
			for id, stack := range goroutines() {
				if _, ok := before[id]; !ok {
					leaked = append(leaked, stack)
				}
			}
			if len(leaked) == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("%d goroutines leaked:\n%s", len(leaked), strings.Join(leaked, "\n\n"))
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

func goroutines() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := make(map[string]string)
	for _, stack := range strings.Split(string(buf), "\n\n") {
		if id, _, ok := strings.Cut(strings.TrimPrefix(stack, "goroutine "), " "); ok {
			out[id] = stack
		}
	}
	return out
}
