# Guide

Walkthrough of the API. Numbers and bounds are in [limits.md](limits.md), wire formats in [formats.md](formats.md). A complete program is in the README, and [examples/publish](../examples/publish) and [examples/watch](../examples/watch) run a relay on 127.0.0.1, publish a generated tone and video track through it, and watch it.

## Publish

1. `NewBroadcast(origin, ns)` creates the catalog track under `ns` in a kolo-moq `Origin` and publishes catalog version 0.
2. `AddTrack(track, opts)` adds a track and publishes the next catalog version. `RemoveTrack(name)` removes one. `Catalog` returns the current catalog. `Close` ends every track and the catalog.
3. `TrackPublisher.WriteFrame(frame)` packages the frame and publishes it without waiting for subscribers. kolo-moq queues per subscriber and drops whole groups for a slow one. `Close` removes the track.

Notes:

- Stamp every track with one `Clock`: `clock.At(time.Now(), timescale)` for a capture instant, `clock.After(d, timescale)` for an offset.
- A zero `Track.Priority` takes `AudioPriority` (48) or `VideoPriority` (68); the catalog uses `CatalogPriority` (28). All are ahead of a kolo-moq track at the default priority.
- After a failed write the next frame starts a group, and on a video track it must be a keyframe (`ErrNeedKeyframe`).
- `AddTrack` returns an error that wraps `ErrInvalidCatalog` and `kolomoq.ErrObjectTooLarge` when the catalog does not fit the origin's `MaxObjectSize`.
- Closing a `TrackPublisher` after its track was removed and re-added under the same name removes nothing of the new track. A second `Close` does nothing.
- The kolo-moq session and the relay stay yours: `Broadcast` only writes to the `Origin`.

## Watch

1. `Watch(ctx, session, ns, opts)` subscribes to the catalog, waits for the first version, plays the tracks `Select` picks for `opts.Capabilities` and follows catalog updates. A track that leaves stops; a better one is switched to, starting at a keyframe.
2. `Next(ctx, kind)` returns the next due frame of `Audio` or `Video` and its `Track`. It blocks while no track of that kind is selected.
3. `Catalog` and `Tracks` return a copy of the catalog in use and of the selection. `Stats` reports the playout, the catalog version, ignored catalogs and, per kind, the subscriptions, cut video groups, malformed objects, dropped groups and why the last subscription ended.
4. `Close` stops the watch and leaves the session usable. The session stays the caller's.
5. `WatchOptions.RequestKeyframes` asks the publisher for a new group, which starts with a keyframe, when the video track waits for one (at the start, or after frames it could not use), lost the rest of a group, or delivers no frame for 500 ms. The publisher reads `TrackPublisher.KeyframeRequests` and makes its next frame a keyframe, and should answer at most once a second, since every watcher asks. The asking is bounded: a quiet track (a still screen share, a paused camera) is asked at doubling intervals and then left alone, a request without a playable frame is repeated after 1 s, 2 s, 4 s and 8 s and then dropped until a frame plays, and a watch does not ask while the catalog lists no video track (remove the track when the camera is off). The watch asks through kolo-moq's `Subscription.RequestNewGroup`, which a kolo-moq relay forwards to the publisher; `Stats().Video.KeyframeRequests` counts the requests sent. It is off by default, because a publisher that does not serve the request gets a REQUEST_UPDATE it has no use for. Details: limits.md.

Ends:

- `ErrBroadcastEnded`: the publisher ended the catalog, or a relay ended it with a PUBLISH_DONE status. It wraps the `kolomoq.PublishDoneError` in that case. When the publisher's session ends behind a relay, the code is `DoneGoingAway` if a GOAWAY was sent or received on that session, and `DoneInternalError` if it ended without one.
- an error wrapping `ErrSessionEnded`: the session ended.
- `ErrClosed`: after `Close`.
- `ErrTrackEnded` appears in `Stats` as the reason the last subscription ended, when the publisher or a relay ended it before the watch ended; if the catalog ends first, the watch stops the tracks and `Stats` may show no track error.

`Watch` fails if the catalog selects tracks and none of their subscriptions starts.

Removing a track and adding it again under the same name (a camera toggle) needs no wait in between: viewers pick the new track at the next catalog version. A viewer that sees that version before the old track ended keeps its subscription until the end, then subscribes again, because the catalog in use is newer than the one the subscription started under. Media subscriptions ask for object and subgroup delivery timeouts of `MaxDelay + MaxBehind` (450 ms by default), so after the old track's PUBLISH_DONE a stream it counts that never arrives holds the end back by that much at most once the other streams closed. A kolo-moq relay gives a subscription the new track once the old track's End of Track or PUBLISH_DONE has reached the relay. Only a subscription that reaches the relay before both still joins the old track, which needs the catalog to overtake the old track's last objects on the way to the relay; when that track ends while the catalog the subscription started under still lists it, the viewer subscribes once more and gets the new track. A frame already played does not play again, whichever instance the new subscription gets.

## Protected media

`NewSecureBroadcast(origin, sender)` is `NewBroadcast` for the namespace of a kolo-moq `secobj.Sender`: every object, the catalog included, is sealed end to end before kolo-moq writes it, so a kolo-moq relay forwards it without reading it. `WatchOptions.Keys` (a `secobj.Keyring`) opens every object before anything uses it. The objects follow the Kolo profile of MoQ Secure Objects, described in kolo-moq's `secobj` package and in [formats.md](formats.md#protected-objects).

Publish:

1. Make a `secobj.Sender` for your namespace on the origin your peers read, and `NewSecureBroadcast(origin, sender)`. The Sender stays yours: `Broadcast.Close` does not close it.
2. `sender.SetMembers(members)` with the participants the application authorizes for the call. Each change rotates the key and sends the new key to every member over your pairwise session with it, before the key is used. A removed member opens nothing sealed after `SetMembers` returned; tell the user "X no longer receives your media", since there is no agreed list of members.
3. Write frames as usual. `Frame.AudioLevel` is checked and then dropped: on a protected track its 1 or 2 bytes would reveal speech through the ciphertext size. Receivers compute levels from decoded audio. Use Opus at a constant bitrate with DTX off, so packet sizes do not reveal speech either.
4. After a rotation, the broadcast publishes the catalog again under the new key at once, even while no frame is written, so a member who just joined can read it; if that fails, the next frame tries again. Force a keyframe on each video track when `sender.KeyID()` changes (`sender.Changed()` tells when), or a joiner waits for the next one.
5. Removing a track and adding it again (a camera toggle) restarts its group IDs; the Sender rotates before it seals the new track, so no nonce is used twice.
6. Data tracks of the application (reactions, raised hands, chat) go on their own tracks in the same namespace, written with `sender.Write` and read through `keys.Opener(session, ns, track)`, one per subscription: it fails with `secobj.ErrWrongSession` unless the session's peer owns `ns` or is a listed relay, as `Watch` does. Write each track's objects in increasing (Group ID, Object ID) order: an object out of order rotates the key.

Watch:

1. Make one `secobj.Keyring` with `Authorize` saying which peer owns which namespace (in a meeting, `{room, hex(peer)}`) and `Relays` listing the kolo-moq relays you use. Run `keys.Receive` for each sender on your pairwise session with that sender: keys never travel through a kolo-moq relay.
2. `Watch(ctx, session, ns, WatchOptions{..., Keys: keys})`. Watch fails with an error wrapping `secobj.ErrWrongSession` unless the session's peer owns `ns` or is a listed relay: a member cannot serve another member's namespace on its own direct session. It returns once a catalog that opens under the sender's key arrives.
3. Each object is opened before its metadata is used. LOC properties come only from the decrypted list; visible properties and track properties, which a relay can add, are ignored.
4. There is no downgrade: an object without a Key ID is dropped and counted. Decide whether a participant is protected only from keys received on the authenticated pairwise session, never from what a relay shows; in group-key mode play nothing of a participant without keys ("waiting for keys").
5. An object under a Key ID newer than every key held may wait for a key message in flight. It is held, at most `PlayoutConfig.MaxDelay` and within `MaxFrames` and `MaxBytes`, and tried again when the keyring changes; it is dropped only if its key is still missing when it is tried after `MaxDelay`. Object statuses and the end of the subscription wait behind held objects, so they never overtake them, and never push a held object out; at the end, held objects get the rest of `MaxDelay`. One catalog object, the newest version (the highest group, whatever the arrival order), waits for its key without a time bound, until a newer one replaces it, and is dropped when the catalog subscription ends; an older version that arrives later counts in `BadCatalogs`. An object under a retired key is dropped at once.
6. `Stats` counts per kind `NoKey` (the key did not arrive in time), `Retired` (the object came after its key was retired, or names a Key ID older than the keys held) and `Unauthenticated` (failed authentication, not protected, or a replay). Catalog objects that do not open count in `BadCatalogs`.
7. Each subscription opens its objects through its own `secobj.Opener`, with its own replay window: a replayed object within a subscription is refused, and several watches of one namespace (two windows, or a new watch after a relay reconnect) and a subscription restarted on a track it already read share one Keyring and read the current catalog and group at once. Across subscriptions, the playout drops frames older than the ones it played, and the catalog version only moves forward.
8. End of Track, End of Group and the PUBLISH_DONE behind `ErrBroadcastEnded` and `ErrTrackEnded` are not authenticated: show them as "media stopped", never as "left the call". Presence comes from the pairwise session.

What a kolo-moq relay still sees and does:

- Each participant's address, kolo-id public key and connect times; the namespaces, so the room identifier (use an opaque one) and who publishes which tracks; who watches whom; Group and Object IDs, priorities and Key IDs, so when each sender rotated and when people joined or left.
- The size and timing of every object: when each person sends, chat message lengths, video motion and keyframes. Mute and camera off show as tracks that stop or end.
- It can drop, delay, reorder or withhold objects, end tracks and serve objects to anyone; without keys that yields nothing readable, and every object it changes fails authentication.
- Every member holds every sender's key, and objects are not signed per sender. A member who controls the relay, runs it or colludes with its operator can therefore make audio, video, catalogs and data that seem to come from any other member, undetectably. Through an honest relay with `kolomoq.RoomPolicy`, and in direct mode, a member cannot.
- There is no forward secrecy within a key generation and no post-compromise security. Anyone in the call can read everyone's media.

Keys are never written to disk. Base keys and salts are overwritten when replaced, removed or closed; the AES key schedules and the key messages held in key track caches and transport buffers are released, not overwritten, and Go does not guarantee that no copy remains in memory. Go's AES-GCM is constant time only in its assembly for amd64 and arm64 CPUs with AES instructions, ppc64x and s390x; on 386, arm, riscv64, loong64, mips, wasm, and amd64 or arm64 CPUs without AES instructions, it is generic code that is not constant time, while Go's TLS then prefers ChaCha20, so object protection is the weakest link there.

## Building blocks

These work without kolo-moq.

- `Catalog` and `Track` describe a broadcast. `MarshalBinary` validates and encodes; `ParseCatalog(b, limits)` decodes, accepts only the canonical encoding and never panics. Codecs: `Opus`, `PCMS16LE`, `VP8`, `VP9`.
- `Clock` gives every track of a sender one time base. `Rescale`, `ToDuration` and `FromDuration` convert between timescales, rounding to the nearest unit and reporting overflow with `ErrTimestampRange`.
- `NewPackager(track, opts)` and `Pack(frame)` return an `Object`: group, object ID, properties in ascending type order and the frame data as payload. `NewUnpackager(track)` and `Unpack(id, properties, payload)` return the frame, or a `*PropertyError`.
- `NewPlayout(tracks, config)` plays the tracks of one broadcast, at most one of them audio. `Push(track, frame, arrival)` copies a received frame into its buffer; `Next(ctx, track)` blocks until the next frame is due. `SetOutputLatency` delays the other tracks by a device buffer. `Stats` counts played, late, duplicate, ahead, overflowing, skipped and keyframe-less frames, and how long the current run of skipped frames has lasted (`LateRun`). Stamp every track of one sender from one `Clock` (`Clock.At` of the capture instant): the tracks share one reference. `Close` wakes every blocked `Next` with `ErrClosed`. Video follows the audio that was actually played. A video track calls `OnKeyframeNeeded` when it drops a delta frame while it waits for a keyframe, so the application can ask the publisher for a new group.
- `Select(catalog, capabilities)` picks one audio and one video track the receiver decodes within its size, frame rate and bitrate limits (`Capabilities`: `Codecs`, `MaxWidth`, `MaxHeight`, `MaxFrameRate`, `MaxBitrate`, `VideoGroup`): lowest priority number first, then highest quality, then catalog order. It returns `ErrNoMatch` when nothing fits.

## Errors

Every error starts with `kolomedia:`; use `errors.Is` and `errors.As`. An error of kolo-moq keeps its cause in the chain under the prefix, so `errors.Is(err, kolomoq.ErrObjectTooLarge)` and `errors.As(err, &requestError)` still work. A bad catalog or track is `ErrInvalidCatalog`, `ErrCatalogFormat` or `ErrInvalidTrack`; a bad frame `ErrInvalidFrame`, `ErrTimestampOrder`, `ErrNeedKeyframe` or `ErrFrameTooLarge`; a bad object `ErrMissingProperty`, `ErrDuplicateProperty`, `ErrMalformedProperty` or `ErrPropertyMismatch` inside a `*PropertyError`; `ErrInvalidTimescale` and `ErrTimestampRange` for time conversions; `ErrInvalidConfig`, `ErrUnknownTrack`, `ErrNoMatch` and `ErrClosed` for the playout, broadcast and watcher; `ErrBroadcastEnded`, `ErrSessionEnded` and `ErrTrackEnded` for a watch. `ErrBroadcastNotFound` is `Watch` of a namespace the peer does not publish (it wraps the `kolomoq.RequestError` with `DOES_NOT_EXIST`), `ErrBroadcastExists` is `NewBroadcast` where the origin already has the catalog track, `ErrTooManyTracks` is an origin with no room for another track, and a frame whose object is over the origin's `MaxObjectSize` is `ErrFrameTooLarge`. `NewSecureBroadcast` with a nil Sender is `ErrInvalidConfig`, a protected `Watch` on a session that may not carry the namespace wraps `secobj.ErrWrongSession`, and a frame that cannot be sealed keeps the `secobj` or kolo-moq error in the chain (`kolomoq.ErrClosed` once the Sender is closed); nothing of it is written.
