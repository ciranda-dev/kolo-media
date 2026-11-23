// Command publish runs a MoQ relay on 127.0.0.1 and publishes a generated broadcast through it:
// a 440 Hz tone as 20 ms PCM frames and a 30 fps video track with a keyframe each second. The
// video payloads are numbered placeholders, not VP8; an application passes its encoder's output.
//
//	publish -listen 127.0.0.1:4433 -for 30s
//
// It prints the relay's address and public key, which watch takes as -relay and -peer.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolomedia "github.com/ciranda-dev/kolo-media"
	kolomoq "github.com/ciranda-dev/kolo-moq"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolotransport "github.com/ciranda-dev/kolo-transport"
)

// namespace is the broadcast's namespace; watch uses the same.
var namespace = kolomoq.Namespace{"example", "live"}

const (
	sampleRate = 48000
	frameTime  = 20 * time.Millisecond
	fps        = 30
)

func main() {
	listen := flag.String("listen", "127.0.0.1:4433", "IPv4 address for the relay")
	duration := flag.Duration("for", 30*time.Second, "how long to publish")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	if err := run(ctx, *listen); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, listen string) error {
	relayID, err := koloid.Generate()
	if err != nil {
		return err
	}
	pubID, err := koloid.Generate()
	if err != nil {
		return err
	}
	// Every audio frame is a group, and each group has its own stream.
	cfg := &kolosecure.Config{Transport: &kolotransport.Config{MaxIncomingStreams: 1000}}

	relay, err := kolomoq.NewRelay(nil)
	if err != nil {
		return err
	}
	ln, err := kolosecure.Listen(ctx, listen, relayID, cfg)
	if err != nil {
		return err
	}
	defer ln.Close()
	var sessions sync.WaitGroup
	defer sessions.Wait()
	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			sessions.Go(func() {
				s, err := relay.NewServer(ctx, conn)
				if err != nil {
					log.Print(err)
					return
				}
				select {
				case <-s.Done():
				case <-ctx.Done():
					s.Close()
				}
			})
		}
	}()
	fmt.Printf("relay %s peer %s\n", ln.Addr(), relayID.PublicKeyString())

	origin, err := kolomoq.NewOrigin(nil)
	if err != nil {
		return err
	}
	conn, err := kolosecure.Dial(ctx, ln.Addr().String(), pubID, relayID.PublicKey(), cfg)
	if err != nil {
		return err
	}
	s, err := kolomoq.NewClient(ctx, conn, &kolomoq.Config{Origin: origin})
	if err != nil {
		return err
	}
	defer s.Close()
	b, err := kolomedia.NewBroadcast(origin, namespace, nil)
	if err != nil {
		return err
	}
	defer b.Close()
	audio, err := b.AddTrack(kolomedia.Track{
		Name: "tone", Kind: kolomedia.Audio, Codec: kolomedia.PCMS16LE, Timescale: sampleRate,
		Bitrate: sampleRate * 16, SampleRate: sampleRate, Channels: 1, FrameDuration: frameTime,
	}, kolomedia.PackagerOptions{})
	if err != nil {
		return err
	}
	video, err := b.AddTrack(kolomedia.Track{
		Name: "pattern", Kind: kolomedia.Video, Codec: kolomedia.VP8, Timescale: 90000,
		Bitrate: 500_000, Width: 320, Height: 180, FrameRate: fps,
	}, kolomedia.PackagerOptions{})
	if err != nil {
		return err
	}
	if _, err := s.PublishNamespace(ctx, namespace); err != nil {
		return err
	}
	fmt.Printf("publishing %v: %s and %s\n", namespace, audio.Track().Name, video.Track().Name)

	clock := kolomedia.NewClock()
	var writers sync.WaitGroup
	var audioFrames, videoFrames int
	writers.Go(func() { audioFrames = writeTone(ctx, clock, audio) })
	writers.Go(func() { videoFrames = writePattern(ctx, clock, video) })
	writers.Wait()
	fmt.Printf("published %d audio and %d video frames\n", audioFrames, videoFrames)
	return nil
}

// writeTone writes 20 ms frames of a 440 Hz sine at a quarter of full scale until ctx ends.
func writeTone(ctx context.Context, clock *kolomedia.Clock, p *kolomedia.TrackPublisher) int {
	samples := int(sampleRate * frameTime / time.Second)
	tick := time.NewTicker(frameTime)
	defer tick.Stop()
	for n := 0; ; n++ {
		ts, err := clock.At(time.Now(), sampleRate)
		if err != nil {
			log.Print(err)
			return n
		}
		pcm := make([]byte, 2*samples)
		for i := range samples {
			v := 0.25 * math.Sin(2*math.Pi*440*float64(n*samples+i)/sampleRate)
			binary.LittleEndian.PutUint16(pcm[2*i:], uint16(int16(v*math.MaxInt16)))
		}
		if err := p.WriteFrame(kolomedia.Frame{Timestamp: ts, Data: pcm}); err != nil {
			log.Print(err)
			return n
		}
		select {
		case <-ctx.Done():
			return n + 1
		case <-tick.C:
		}
	}
}

// writePattern writes numbered placeholder frames at 30 fps, a keyframe every second.
func writePattern(ctx context.Context, clock *kolomedia.Clock, p *kolomedia.TrackPublisher) int {
	tick := time.NewTicker(time.Second / fps)
	defer tick.Stop()
	for n := 0; ; n++ {
		ts, err := clock.At(time.Now(), 90000)
		if err != nil {
			log.Print(err)
			return n
		}
		data := make([]byte, 1500)
		binary.BigEndian.PutUint64(data, uint64(n))
		if err := p.WriteFrame(kolomedia.Frame{Timestamp: ts, Keyframe: n%fps == 0, Data: data}); err != nil {
			log.Print(err)
			return n
		}
		select {
		case <-ctx.Done():
			return n + 1
		case <-tick.C:
		}
	}
}
