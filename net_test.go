package kolomedia

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolomoq "github.com/ciranda-dev/kolo-moq"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolotransport "github.com/ciranda-dev/kolo-transport"
)

var testTransport = &kolotransport.Config{MaxIncomingStreams: 1000}

var testNS = kolomoq.Namespace{"room", "alice"}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// dialPair connects a client to a server over kolo-secure on 127.0.0.1 and starts a MoQ session
// on each end with newServer and newClient; both close at the end of the test.
func dialPair(t *testing.T, newServer, newClient func(ctx context.Context, c kolomoq.Conn) (*kolomoq.Session, error)) (client, server *kolomoq.Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srvID, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cliID, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	l, err := kolosecure.Listen(ctx, "127.0.0.1:0", srvID, &kolosecure.Config{Transport: testTransport})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	type result struct {
		s   *kolomoq.Session
		err error
	}
	res := make(chan result, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			res <- result{nil, err}
			return
		}
		s, err := newServer(ctx, c)
		res <- result{s, err}
	}()
	cc, err := kolosecure.Dial(ctx, l.Addr().String(), cliID, srvID.PublicKey(), &kolosecure.Config{Transport: testTransport})
	if err != nil {
		t.Fatal(err)
	}
	client, err = newClient(ctx, cc)
	if err != nil {
		t.Fatal(err)
	}
	r := <-res
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(func() {
		client.Close()
		r.s.Close()
	})
	return client, r.s
}

// publisherPair returns a subscriber session connected to a publisher session that serves origin.
func publisherPair(t *testing.T, origin *kolomoq.Origin) (sub, pub *kolomoq.Session) {
	t.Helper()
	return dialPair(t,
		func(ctx context.Context, c kolomoq.Conn) (*kolomoq.Session, error) {
			return kolomoq.NewServer(ctx, c, &kolomoq.Config{Origin: origin})
		},
		func(ctx context.Context, c kolomoq.Conn) (*kolomoq.Session, error) {
			return kolomoq.NewClient(ctx, c, nil)
		})
}

func newOrigin(t *testing.T, cfg *kolomoq.OriginConfig) *kolomoq.Origin {
	t.Helper()
	o, err := kolomoq.NewOrigin(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func newBroadcast(t *testing.T, o *kolomoq.Origin) *Broadcast {
	t.Helper()
	b, err := NewBroadcast(o, testNS)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func addTrack(t *testing.T, b *Broadcast, tr Track) *TrackPublisher {
	t.Helper()
	p, err := b.AddTrack(tr, PackagerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// source writes generated frames to a track in real time until stopped. Each payload starts with
// the capture time since the clock's start, so a receiver can measure capture to play.
type source struct {
	stop chan struct{}
	wg   sync.WaitGroup
}

func (s *source) Stop() {
	close(s.stop)
	s.wg.Wait()
}

// startSource writes a frame every period; for video every gop-th frame is a keyframe.
func startSource(t *testing.T, p *TrackPublisher, c *Clock, period time.Duration, gop int) *source {
	t.Helper()
	s := &source{stop: make(chan struct{})}
	tr := p.Track()
	s.wg.Go(func() {
		tick := time.NewTicker(period)
		defer tick.Stop()
		for i := 0; ; i++ {
			now := time.Now()
			ts, err := c.At(now, tr.Timescale)
			if err != nil {
				t.Error(err)
				return
			}
			data := make([]byte, 8, 200)
			putCapture(data, now.Sub(c.Start()))
			if tr.Kind == Video {
				data = append(data, make([]byte, 100)...)
			}
			if err := p.WriteFrame(Frame{Timestamp: ts, Keyframe: tr.Kind == Audio || i%gop == 0, Data: data}); err != nil && !isClosed(err) {
				t.Errorf("%s: %v", tr.Name, err)
				return
			}
			select {
			case <-s.stop:
				return
			case <-tick.C:
			}
		}
	})
	t.Cleanup(func() {
		select {
		case <-s.stop:
		default:
			s.Stop()
		}
	})
	return s
}

func isClosed(err error) bool {
	return errors.Is(err, ErrClosed)
}

func putCapture(data []byte, d time.Duration) {
	binary.BigEndian.PutUint64(data, uint64(d))
}

func captured(c *Clock, f Frame) time.Time {
	return c.Start().Add(time.Duration(binary.BigEndian.Uint64(f.Data)))
}

func lowVideo() Track {
	t := videoTrack()
	t.Name, t.Width, t.Height, t.Bitrate, t.FrameRate = "cam-low", 320, 180, 150_000, 30
	return t
}

func avCaps() Capabilities {
	return Capabilities{Codecs: []Codec{Opus, VP8, VP9}}
}

func watch(t *testing.T, s *kolomoq.Session, opts WatchOptions) *Watcher {
	t.Helper()
	w, err := Watch(testCtx(t), s, testNS, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}
