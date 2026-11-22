# kolo-media

Catalog, LOC packaging, playout and A/V sync for Kolo broadcasts over MoQ.

[![status: experimental](https://img.shields.io/badge/status-experimental-orange)](#status)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![go: 1.26+](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)

## What it is

kolo-media packs audio and video frames into MoQ objects with the [Low Overhead Media Container, draft-ietf-moq-loc-04](https://datatracker.ietf.org/doc/html/draft-ietf-moq-loc-04), carries them with [kolo-moq](https://github.com/ciranda-dev/kolo-moq), and plays them out in sync. It sits between the codecs and kolo-moq. A sender describes its tracks in a catalog, stamps frames on one clock and publishes them as LOC objects. A receiver reads the catalog, subscribes to the tracks it can decode, and plays the frames through a jitter buffer in sync, following catalog updates.

It works on encoded frames only. Capture, codecs and output stay in kolo-audio, kolo-video or your application. It is pure Go, with no cgo, and there is nothing to install.

## Install

```sh
go get github.com/ciranda-dev/kolo-media
```

It depends on kolo-moq, which `go get` installs with it.

## Quick start

Publish one audio track and watch it on 127.0.0.1, with generated media. Save it outside the repository and run it from the kolo-media directory:

```sh
go run /path/to/quickstart.go
```

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolomedia "github.com/ciranda-dev/kolo-media"
	kolomoq "github.com/ciranda-dev/kolo-moq"
	kolosecure "github.com/ciranda-dev/kolo-secure"
)

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	alice, bob := must(koloid.Generate()), must(koloid.Generate())

	// Alice publishes a broadcast with one audio track and serves it.
	origin := must(kolomoq.NewOrigin(nil))
	ns := kolomoq.Namespace{"room", "alice"}
	broadcast := must(kolomedia.NewBroadcast(origin, ns))
	defer broadcast.Close()
	mic := must(broadcast.AddTrack(kolomedia.Track{
		Name: "mic", Kind: kolomedia.Audio, Codec: kolomedia.PCMS16LE, Timescale: 48000,
		SampleRate: 48000, Channels: 1, FrameDuration: 20 * time.Millisecond,
	}, kolomedia.PackagerOptions{}))
	ln := must(kolosecure.Listen(ctx, "127.0.0.1:0", alice, nil))
	defer ln.Close()
	go func() {
		if conn, err := ln.Accept(ctx); err == nil {
			if s, err := kolomoq.NewServer(ctx, conn, &kolomoq.Config{Origin: origin}); err == nil {
				<-s.Done()
			}
		}
	}()

	// Her encoder would produce these frames: 20 ms of silence each.
	clock := kolomedia.NewClock()
	go func() {
		for range time.Tick(20 * time.Millisecond) {
			ts, err := clock.At(time.Now(), 48000)
			if err != nil || mic.WriteFrame(kolomedia.Frame{Timestamp: ts, Data: make([]byte, 1920)}) != nil {
				return
			}
		}
	}()

	// Bob dials alice and plays what her catalog offers him.
	conn := must(kolosecure.Dial(ctx, ln.Addr().String(), bob, alice.PublicKey(), nil))
	session := must(kolomoq.NewClient(ctx, conn, nil))
	defer session.Close()
	caps := kolomedia.Capabilities{Codecs: []kolomedia.Codec{kolomedia.PCMS16LE}}
	watcher := must(kolomedia.Watch(ctx, session, ns, kolomedia.WatchOptions{Capabilities: caps}))
	defer watcher.Close()
	for range 5 {
		frame, track, err := watcher.Next(ctx, kolomedia.Audio)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("play", track.Name, len(frame.Data), "bytes")
	}
	fmt.Println("playout delay", watcher.Stats().Playout.Delay)
}
```

Output:

```text
play mic 1920 bytes
play mic 1920 bytes
play mic 1920 bytes
play mic 1920 bytes
play mic 1920 bytes
playout delay 100ms
```

[examples/publish](examples/publish) runs a relay and publishes a tone and a video track through it; [examples/watch](examples/watch) watches that broadcast and prints the frames, the tone's level and the A/V offset each second.

```sh
go run ./examples/publish -for 30s    # prints the relay's public key
go run ./examples/watch -peer <key> -for 5s
```

## API

| Symbol | What it does |
|---|---|
| `NewBroadcast(origin, ns, limits...)` | Publishes a catalog under a kolo-moq namespace. A `CatalogLimits` with a higher `MaxTracks` than 1024 lets it publish more tracks; watchers raise theirs in `WatchOptions.CatalogLimits`. `AddTrack`, `RemoveTrack`, `Catalog` and `Close` change and end it. Objects are not encrypted end to end: a relay reads and can change them. Use `NewSecureBroadcast` to protect them. |
| `TrackPublisher.WriteFrame` | Packages one frame and publishes it without waiting for subscribers. |
| `NewSecureBroadcast(origin, sender, limits...)` | `NewBroadcast` with every object, the catalog included, sealed end to end by a kolo-moq `secobj.Sender`. Protected audio carries no AUDIO_LEVEL. |
| `Watch(ctx, session, ns, opts)` | Subscribes to a broadcast, picks tracks, follows catalog updates and plays them in sync. Without `Keys`, a watch accepts unprotected objects. |
| `WatchOptions.Keys` | Opens every object with its sender's key from a `secobj.Keyring`, drops objects without one and refuses a session that neither owns the namespace nor is a listed relay (`secobj.ErrWrongSession`). `NoKey`, `Retired` and `Unauthenticated` in `Stats` count what it dropped. |
| `WatchOptions.RequestKeyframes`, `TrackPublisher.KeyframeRequests` | A watch asks the publisher for a keyframe when its video waits for one or goes quiet for 500 ms, within bounds; the publisher reads the requests from the channel. Off by default. |
| `Watcher.Next(ctx, kind)` | Returns the next due `Audio` or `Video` frame and its track. `Stats`, `Catalog`, `Tracks` and `Close` complete it. |
| `Catalog`, `Track`, `Select` | Describe a broadcast and pick the tracks a receiver can decode. `ParseCatalog` never panics on input. |
| `Frame`, `Packager`, `Unpackager`, `Clock` | Encoded frames, their LOC objects and one time base. Usable without kolo-moq. |
| `Playout` | Jitter buffers, adaptive delay, A/V sync and skip to live. Usable without kolo-moq. |
| `ErrBroadcastEnded`, `ErrSessionEnded`, `ErrClosed` | Sentinel errors, all starting with `kolomedia:`. |

## How a frame travels

1. The application captures and encodes a frame (kolo-audio, kolo-video or your own code).
2. `Clock` stamps it and `WriteFrame` packages it as a LOC object in a group.
3. kolo-moq publishes the object on the track, and a relay forwards it.
4. `Watch` receives it, restores the order inside a video group and hands it to the `Playout`.
5. `Next` returns it when it is due, with audio and video in sync.
6. The application decodes and renders it.

## Protected media

With `NewSecureBroadcast` and `WatchOptions.Keys`, a kolo-moq relay forwards media, catalogs and data tracks it cannot read or change unnoticed. Keys go from each sender to each member over their pairwise session, never through the relay. What stays visible and possible:

- The relay sees who is in the call, who watches whom, when each person sends and how much, chat message lengths, when microphones and cameras are off, key rotations and the room ID (use an opaque one). It can drop, delay or withhold media.
- Every member holds every sender's key and objects are not signed per sender, so a member who controls the relay, or colludes with its operator, can make audio, video or messages that seem to come from someone else.
- Anyone in the call can read everyone's media. There is no forward secrecy within a key period and no recovery after a device is compromised.
- Base keys are overwritten when replaced or removed, but AES key schedules and cached key messages are only released, and Go does not guarantee that no copy remains in memory.
- Go's AES-GCM is constant time only on amd64 and arm64 CPUs with AES instructions, ppc64x and s390x; on 386, arm, riscv64, loong64, mips and wasm it is not.

Details: [guide](docs/guide.md#protected-media), [formats](docs/formats.md#protected-objects), [limits](docs/limits.md#protected-media).

## Platforms

| Platform | Status |
|---|---|
| macOS arm64, Go 1.27.3 | `go vet`, `go test`, `go test -race` pass; the examples run |
| Linux | not run; cross-vetted for amd64 and 386 |
| Windows | not run; cross-vetted |

It builds with `CGO_ENABLED=0` and needs Go 1.26 or newer. Opus frames from kolo-audio and VP8 frames from kolo-video were published, watched and decoded on loopback. VIDEO_FRAME_MARKING is not written or read, because LOC leaves its bit layout to another RFC.

More: [guide](docs/guide.md), [formats](docs/formats.md), [limits and defaults](docs/limits.md).

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE) or the [MIT license](LICENSE-MIT), at your option.

## Status

Experimental. The API may change before v1.0.0.
