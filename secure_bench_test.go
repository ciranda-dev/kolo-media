package kolomedia

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolomoq "github.com/ciranda-dev/kolo-moq"
	"github.com/ciranda-dev/kolo-moq/secobj"
	kolosecure "github.com/ciranda-dev/kolo-secure"
)

func benchSender(b *testing.B) (*secobj.Sender, *kolomoq.Origin, *koloid.Identity) {
	b.Helper()
	id, err := koloid.Generate()
	if err != nil {
		b.Fatal(err)
	}
	origin, err := kolomoq.NewOrigin(nil)
	if err != nil {
		b.Fatal(err)
	}
	s, err := secobj.NewSender(origin, memberNS(id.PublicKey()), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	return s, origin, id
}

// benchKeyring returns a Keyring that holds the key of s, delivered over a real pairwise session on 127.0.0.1.
func benchKeyring(b *testing.B, s *secobj.Sender, origin *kolomoq.Origin, id *koloid.Identity) (*secobj.Keyring, *kolomoq.Session) {
	b.Helper()
	rid, err := koloid.Generate()
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	l, err := kolosecure.Listen(ctx, "127.0.0.1:0", id, &kolosecure.Config{Transport: secTransport})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { l.Close() })
	served := make(chan *kolomoq.Session, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			served <- nil
			return
		}
		srv, err := kolomoq.NewServer(ctx, c, &kolomoq.Config{Origin: origin})
		if err != nil {
			srv = nil
		}
		served <- srv
	}()
	dctx, dcancel := context.WithTimeout(ctx, 20*time.Second)
	defer dcancel()
	c, err := kolosecure.Dial(dctx, l.Addr().String(), rid, id.PublicKey(), &kolosecure.Config{Transport: secTransport})
	if err != nil {
		b.Fatal(err)
	}
	cli, err := kolomoq.NewClient(dctx, c, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { cli.Close() })
	if srv := <-served; srv != nil {
		b.Cleanup(func() { srv.Close() })
	}
	if err := s.SetMembers([]ed25519.PublicKey{rid.PublicKey()}); err != nil {
		b.Fatal(err)
	}
	k, err := secobj.NewKeyring(secobj.KeyringConfig{Authorize: ownerAuth})
	if err != nil {
		b.Fatal(err)
	}
	changed := k.Changed()
	done := make(chan struct{})
	go func() {
		defer close(done)
		k.Receive(ctx, cli, rid.PublicKey(), s.Namespace())
	}()
	b.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case <-changed:
	case <-time.After(20 * time.Second):
		b.Fatal("no key arrived")
	}
	return k, cli
}

func benchWrite(b *testing.B, bc *Broadcast, tr Track, size int) {
	b.Helper()
	tp, err := bc.AddTrack(tr, PackagerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	data := make([]byte, size)
	step := tr.Timescale / 50
	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := range b.N {
		if err := tp.WriteFrame(Frame{Timestamp: 1_000 + uint64(i)*step, Keyframe: true, Data: data}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSealAudio20ms is the cost of one 20 ms audio frame (80 bytes, 32 kbit/s) on the sender:
// the Sender alone, a protected WriteFrame, and an unprotected WriteFrame for comparison. Neither
// WriteFrame has a subscriber.
func BenchmarkSealAudio20ms(b *testing.B) {
	b.Run("seal", func(b *testing.B) {
		s, _, _ := benchSender(b)
		payload := make([]byte, scaleAudioBytes)
		props := []kolomoq.Property{{Type: PropertyTimestamp, Value: 1_000_000}}
		b.ReportAllocs()
		b.SetBytes(scaleAudioBytes)
		b.ResetTimer()
		for i := range b.N {
			if _, _, err := s.Seal("mic", uint64(i), 0, 48, payload, props); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("write protected", func(b *testing.B) {
		s, origin, _ := benchSender(b)
		bc, err := NewSecureBroadcast(origin, s)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { bc.Close() })
		benchWrite(b, bc, audioTrack(), scaleAudioBytes)
	})
	b.Run("write plain", func(b *testing.B) {
		origin, err := kolomoq.NewOrigin(nil)
		if err != nil {
			b.Fatal(err)
		}
		bc, err := NewBroadcast(origin, testNS)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { bc.Close() })
		benchWrite(b, bc, audioTrack(), scaleAudioBytes)
	})
}

// benchOpen measures Opener.Open of objects of the given size. The objects are sealed in batches
// off the clock, because the replay window refuses an object it has seen.
func benchOpen(b *testing.B, size int) {
	s, origin, id := benchSender(b)
	k, sess := benchKeyring(b, s, origin, id)
	o, err := k.Opener(sess, s.Namespace(), "cam")
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, size)
	props := []kolomoq.Property{{Type: PropertyTimestamp, Value: 1_000_000}}
	const batch = 64
	objs := make([]kolomoq.Object, batch)
	var group uint64
	b.ReportAllocs()
	b.SetBytes(int64(size))
	for done := 0; done < b.N; {
		n := min(batch, b.N-done)
		b.StopTimer()
		for j := range n {
			group++
			ct, pr, err := s.Seal("cam", group, 0, 68, payload, props)
			if err != nil {
				b.Fatal(err)
			}
			objs[j] = kolomoq.Object{Group: group, Priority: 68, Payload: ct, Properties: pr}
		}
		b.StartTimer()
		for j := range n {
			if _, _, err := o.Open(objs[j]); err != nil {
				b.Fatal(err)
			}
		}
		done += n
	}
}

// BenchmarkOpenVideoKeyframe is the cost of opening one video keyframe at a receiver: 32 KiB, the
// keyframe of the synthetic call, and 100 KiB.
func BenchmarkOpenVideoKeyframe(b *testing.B) {
	for _, size := range []int{scaleVideoKey, 100 << 10} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) { benchOpen(b, size) })
	}
}

func BenchmarkOpenAudio20ms(b *testing.B) { benchOpen(b, scaleAudioBytes) }
