// Command watch connects to the relay that publish runs, watches its broadcast and prints, each
// second, the frames played per track, the tone's level and how far apart audio and video play.
//
//	watch -relay 127.0.0.1:4433 -peer <relay public key> -for 5s
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"slices"
	"sync"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolomedia "github.com/ciranda-dev/kolo-media"
	kolomoq "github.com/ciranda-dev/kolo-moq"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolotransport "github.com/ciranda-dev/kolo-transport"
)

var namespace = kolomoq.Namespace{"example", "live"}

func main() {
	addr := flag.String("relay", "127.0.0.1:4433", "relay address")
	peer := flag.String("peer", "", "relay public key, hex")
	duration := flag.Duration("for", 5*time.Second, "how long to watch")
	flag.Parse()
	key, err := hex.DecodeString(*peer)
	if err != nil || len(key) == 0 {
		log.Fatal("usage: watch -relay <address> -peer <hex public key> [-for 5s]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, *addr, key, *duration); err != nil {
		log.Fatal(err)
	}
}

type second struct {
	mu          sync.Mutex
	audio       int
	video       int
	keyframes   int
	lost        int
	power       float64
	samples     int
	audioOffset []time.Duration
	videoOffset []time.Duration
}

func run(ctx context.Context, addr string, peer []byte, duration time.Duration) error {
	id, err := koloid.Generate()
	if err != nil {
		return err
	}
	cfg := &kolosecure.Config{Transport: &kolotransport.Config{MaxIncomingStreams: 1000}}
	conn, err := kolosecure.Dial(ctx, addr, id, peer, cfg)
	if err != nil {
		return err
	}
	s, err := kolomoq.NewClient(ctx, conn, nil)
	if err != nil {
		return err
	}
	defer s.Close()
	caps := kolomedia.Capabilities{Codecs: []kolomedia.Codec{kolomedia.PCMS16LE, kolomedia.VP8}}
	w, err := kolomedia.Watch(ctx, s, namespace, kolomedia.WatchOptions{Capabilities: caps})
	if err != nil {
		return err
	}
	defer w.Close()
	sel := w.Tracks()
	if sel.Audio == nil || sel.Video == nil {
		return fmt.Errorf("broadcast %v has no tracks this watcher plays: %+v", namespace, w.Catalog())
	}
	fmt.Printf("watching %v, catalog version %d: %s (%s), %s (%s %dx%d)\n", namespace, w.Catalog().Version,
		sel.Audio.Name, sel.Audio.Codec, sel.Video.Name, sel.Video.Codec, sel.Video.Width, sel.Video.Height)

	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	cur := &second{}
	var curMu sync.Mutex
	current := func() *second {
		curMu.Lock()
		defer curMu.Unlock()
		return cur
	}
	var players sync.WaitGroup
	errs := make(chan error, 2)
	players.Go(func() { errs <- playAudio(ctx, w, current) })
	players.Go(func() { errs <- playVideo(ctx, w, current) })

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick.C:
		}
		curMu.Lock()
		last := cur
		cur = &second{}
		curMu.Unlock()
		if !done || last.audio+last.video > 0 {
			last.print()
		}
	}
	players.Wait()
	close(errs)
	for err := range errs {
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	st := w.Stats()
	a, v := st.Playout.Tracks["audio"], st.Playout.Tracks["video"]
	fmt.Printf("delay %v, jitter %v; audio played %d of %d; video played %d of %d, gaps %d\n",
		st.Playout.Delay, st.Playout.Jitter, a.Played, a.Received, v.Played, v.Received, st.Video.Gaps)
	return nil
}

func playAudio(ctx context.Context, w *kolomedia.Watcher, current func() *second) error {
	for {
		f, t, err := w.Next(ctx, kolomedia.Audio)
		if err != nil {
			return err
		}
		offset, err := playOffset(f, t)
		if err != nil {
			return err
		}
		var power float64
		for i := 0; i+1 < len(f.Data); i += 2 {
			v := float64(int16(binary.LittleEndian.Uint16(f.Data[i:]))) / math.MaxInt16
			power += v * v
		}
		s := current()
		s.mu.Lock()
		s.audio++
		s.power += power
		s.samples += len(f.Data) / 2
		s.audioOffset = append(s.audioOffset, offset)
		s.mu.Unlock()
	}
}

func playVideo(ctx context.Context, w *kolomedia.Watcher, current func() *second) error {
	var next uint64
	for {
		f, t, err := w.Next(ctx, kolomedia.Video)
		if err != nil {
			return err
		}
		offset, err := playOffset(f, t)
		if err != nil {
			return err
		}
		n := binary.BigEndian.Uint64(f.Data)
		s := current()
		s.mu.Lock()
		s.video++
		if f.Keyframe {
			s.keyframes++
		}
		if next > 0 && n > next {
			s.lost += int(n - next)
		}
		s.videoOffset = append(s.videoOffset, offset)
		s.mu.Unlock()
		next = n + 1
	}
}

// playOffset is how long after its timestamp a frame plays. Audio and video share one time base,
// so the difference of their offsets is how far apart they play.
func playOffset(f kolomedia.Frame, t kolomedia.Track) (time.Duration, error) {
	media, err := kolomedia.ToDuration(f.Timestamp, t.Timescale)
	if err != nil {
		return 0, err
	}
	return time.Duration(time.Now().UnixNano()) - media, nil
}

func (s *second) print() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.audio == 0 && s.video == 0 {
		fmt.Println("nothing played")
		return
	}
	level := math.Inf(-1)
	if s.samples > 0 && s.power > 0 {
		level = 10 * math.Log10(s.power/float64(s.samples))
	}
	line := fmt.Sprintf("audio %d frames, %.1f dBFS; video %d frames (keyframes %d, lost %d)", s.audio, level, s.video, s.keyframes, s.lost)
	if len(s.audioOffset) > 0 && len(s.videoOffset) > 0 {
		a := median(s.audioOffset)
		line += fmt.Sprintf("; audio plays %v after capture, video %v after audio",
			a.Round(time.Millisecond), (median(s.videoOffset) - a).Round(100*time.Microsecond))
	}
	fmt.Println(line)
}

func median(d []time.Duration) time.Duration {
	slices.Sort(d)
	return d[len(d)/2]
}
