# Limits and defaults

Every default and bound a user of kolo-media meets. A zero field in a config takes the default. Each number has a short neutral reason.

## Catalog

`CatalogLimits` bounds what `ParseCatalog` accepts and what `MarshalBinaryLimits` encodes. A field of 0 or less takes its default. `MaxTracks` can be raised without a ceiling, on both sides: a publisher sets `BroadcastOptions.CatalogLimits` (the last argument of `NewBroadcast` and `NewSecureBroadcast`, nil for the defaults), and a watcher sets `WatchOptions.CatalogLimits`. `MaxNameLen` and `MaxConfigLen` are MoQ bounds: a value above the default takes the default in `ParseCatalog`, and `NewBroadcast` refuses it with `ErrInvalidConfig`. `MarshalBinary` checks the defaults; a catalog parsed with raised limits needs `MarshalBinaryLimits` with the same limits.

| Limit | Default | Reason |
|---|---|---|
| `MaxTracks` | 1024 | the renditions per broadcast a player keeps; the memory of a parse (below) |
| `MaxNameLen` (track and group names) | 4096 bytes | the MoQ bound on a full track name |
| `MaxConfigLen` | 65508 bytes | the largest config that fits one object property |

- MoQ limits all properties of one object to 65535 bytes. The first object of a group also carries TIMESCALE, AUDIO_LEVEL and TIMESTAMP, 27 bytes at their longest, which leaves 65508 for the config.
- The track count is checked against the bytes left before anything is allocated (the parser assumes at least 15 bytes a track before it allocates; the smallest valid track is 23), so a raised `MaxTracks` costs nothing until the bytes are there.
- What a raised `MaxTracks` can cost. A catalog is at most the origin's `MaxObjectSize` (16 MiB by default), so at most about 730,000 tracks of the smallest kind (23 bytes: an audio track with a 3-byte name). Measured with `BenchmarkParseCatalog` on Apple M5, Go 1.27.4, darwin/arm64 (`ParseCatalog` of such catalogs, on a busy machine): 2,000 tracks (46 KB) take 0.18 ms and allocate 0.4 MB, of which 0.29 MB is kept; 700,000 tracks (16.1 MB) take 97 ms and allocate 129 MB (8.0 times the bytes read), of which 101 MB is kept (6.3 times). A Track is 136 bytes before its strings. At the default of 1024 a catalog costs under 0.3 MB. A publisher that raises `MaxTracks` needs receivers that raise theirs: a receiver with the default refuses a catalog above 1024 (`ErrInvalidCatalog`, counted in `WatchStats.BadCatalogs`). Each `AddTrack` encodes the whole catalog again, so adding n tracks one by one costs n squared in encoding: 2,000 take 0.75 s together with the watcher in `TestManyTracks`.
- A `Sender` of kolo-moq rotates its key after `SenderConfig.MaxTracks` (1024 by default) tracks; raise it with the catalog's.
- The whole catalog must fit the origin's `MaxObjectSize` (16 MiB by default in kolo-moq). When it does not, `AddTrack` fails with an error that wraps `ErrInvalidCatalog` and `kolomoq.ErrObjectTooLarge`. `AddTrack` checks the size before it opens a catalog group, so the failure leaves the previous catalog as the current group and a late joiner finds it.
- kolo-moq's `OriginConfig.MaxTracks` is 0 by default, no cap, so the catalog's `MaxTracks` is what stops `AddTrack` (`ErrInvalidCatalog`). If you set an origin cap it counts the catalog track and every media track, and `AddTrack` fails with `ErrTooManyTracks` at the cap.
- A relay's memory is bounded by kolo-moq, not by the catalog: `RelayConfig.MaxCacheBytes` (1 GiB by default) for the tracks it relays, with no count of tracks, and `Config.MaxSessionBufferedBytes` (8 times `MaxBufferedBytes`, 256 MiB) for what one session has queued. A watcher subscribes only to the tracks it selects, so a large catalog does not open a subscription per track.
- A rendition `Group` holds tracks of one kind and one timescale.

## Publishing

| Value | Setting | Reason |
|---|---|---|
| `CatalogPriority` | 28 | sent before audio |
| `AudioPriority` | 48 | sent before video |
| `VideoPriority` | 68 | sent before a track at the default priority |
| `MediaGroupTimeout` | 30 s | a group not sent 30 s after it ended is no use to a live subscriber and is dropped |

- A lower priority number is sent first. All three are ahead of a track at kolo-moq's default priority (128). A zero `Track.Priority` takes `AudioPriority` or `VideoPriority`; the catalog track uses `CatalogPriority`.
- Timestamps must increase strictly per track. `ErrTimestampOrder` otherwise.
- A video track must start with a keyframe, and after a failed `WriteFrame` the next frame must be one (`ErrNeedKeyframe`).
- An audio group holds one frame by default, or frames up to `PackagerOptions.AudioGroupDuration`.

## Playout

`PlayoutConfig`:

| Field | Default | Reason |
|---|---|---|
| `MinDelay` | 100 ms | the jitter allowance of an interactive call |
| `MaxDelay` | 400 ms, or `MinDelay` if larger | a Wi-Fi retransmission burst |
| `MaxBehind` | 50 ms | the slack an interactive player keeps above its jitter allowance |
| `MaxFrames` | 8192 per track | bounds the frames buffered |
| `MaxBytes` | 16 MiB per track | bounds the frame data buffered |

A negative value, or `MinDelay` over `MaxDelay`, is `ErrInvalidConfig`.

How the delay and the reference are measured:

- The reference is the earliest arrival minus media time over the last 2 s of arrivals (at most 8192), across all tracks. A frame stamped T is due at reference + T + delay.
- The delay is the 95th percentile of measured lateness over the same window, between `MinDelay` and `MaxDelay`. Outliers under 5% do not raise it.
- Video also waits for the audio lag of the last audio frame played, so audio paces the clock and video follows.
- `SetOutputLatency` delays the other tracks by a device buffer.

What the playout drops:

- Audio more than `MaxBehind` late is dropped (`Skipped`). Late video jumps to its newest due keyframe, or plays its late frames at once if there is none. `TrackStats.LateRun` shows how long the current run of such frames has lasted.
- An audio track whose frames are all too late for 2 s (the reference window) while another track plays on time has a timeline that fell behind the others', for example a microphone that lost samples and stamps by count: no delay covers that, so the track's due times move by its lateness and it plays again, out of sync by what it fell behind. Once its frames arrive in time without that move for 2 s, the move ends and the tracks are in sync again. Audio that is late while the other tracks are late too, or with no other track playing, is the path's delay and is not moved. A run of late frames ends when a frame plays or after 2 s with none skipped.
- A frame more than 2 s earlier than the reference allows is dropped (`Ahead`), so a later frame with a wrong timestamp costs a track at most about 2 s. If only such frames arrive for 2 s, the estimate starts over.
- A first frame stamped far ahead cannot be told from an honest one, because a timestamp has the sender's own epoch: it becomes the reference, and the frames that follow are dropped as `Late` until the 2 s stale window passes, so the track is silent for 2 to 3 s and then plays. Only the sender's own track is affected.
- A frame older than one already played is dropped as `Late`; a repeated timestamp as `Duplicate`. If only older frames arrive for 2 s, the last frame played is taken as the bad one and the older frames play: after a delivery stall that resumes from an older point, one frame can play after a later one.
- A full buffer evicts its oldest frame (`Overflow`). A frame larger than `MaxBytes` is `ErrFrameTooLarge`.
- A video track waits for a keyframe at start and after it evicted a frame, drops delta frames meanwhile (`NoKeyframe`) and calls `OnKeyframeNeeded` once.
- At most one track of a `Playout` is audio.

## Watching

- Held video objects. The group assembler holds objects that follow a missing one, bounded by the playout's `MaxFrames` and `MaxBytes` over all groups and counted separately from the playout buffer. A video track therefore holds up to twice each, 32 MiB by default. A group with a hole when a newer group starts, or over a bound, is cut and play resumes at the next keyframe. A malformed object cuts the rest of its group.
- Memory per remote participant. Each bound above is per track, and `PlayoutConfig.MaxBytes` sets all of them. An audio track holds up to 16 MiB in the playout, 32 MiB when protected (the hold for a key). A video track holds 32 MiB, 48 MiB when protected. A protected participant with audio and video is therefore up to 80 MiB in kolo-media, plus kolo-moq's `MaxBufferedBytes` (32 MiB by default) for each of its subscriptions. A hostile sender that keeps every bound full costs that much, so a room of 20 hostile senders is a few GB; honest media is a small fraction of it, and each hold is also bounded by age (`MaxDelay`).
- Requesting keyframes. With `WatchOptions.RequestKeyframes` a watch asks for a new group when its video track waits for a keyframe, loses the rest of a group, or delivers no frame for 500 ms. The requests are bounded so that a track that is only quiet (a still screen share, a paused camera) is not asked forever: a request for silence doubles the silence the next one needs (500 ms, 1 s, 2 s, 4 s, 8 s) and after five such requests silence no longer counts, until 30 frames have come in a row each within 500 ms of the one before, or a loss is reported. A request that gets no playable frame is repeated after 1 s, 2 s, 4 s and 8 s, and five of them in a row end the asking until a frame plays. A watch asks only while the catalog lists a video track, so a camera that is turned off by removing its track (`RemoveTrack`) is left alone. Each watcher is bounded, but watchers add up: the publisher should answer with at most one keyframe a second.
- Delivery timeout. Media subscriptions ask kolo-moq for an object and a subgroup delivery timeout of `MaxDelay + MaxBehind` (450 ms by default): a later frame can no longer play. The subgroup timeout also bounds how long an ended subscription waits after PUBLISH_DONE for a counted stream that never arrives. The sum saturates for huge values.
- Start. Audio subscribes from the next object; the catalog and video from the current group, so a late joiner starts at a keyframe.
- Catalogs. Only a version above the one in use is taken; an older or equal one is skipped. A catalog object that fails to parse, is not object 0 or whose version differs from its group ID is ignored and counted in `WatchStats.BadCatalogs`.
- `Watch` fails when the catalog selects tracks and none of their subscriptions starts. A partial failure shows in `Stats`.
- A subscription that ends while the catalog still lists its track is restarted once if the catalog in use is newer than the one it started under (the track was removed and added again under the same name), and otherwise only by a later catalog update. A new subscription to the same track, also after the track was removed and added again, plays on from the frame after the last one played; frames of a sender whose time base went back play only after the 2 s window of the playout.

## Protected media

| Bound | Value | Reason |
|---|---|---|
| Hold of an object whose key may be in flight | `PlayoutConfig.MaxDelay` (400 ms) | a later frame can no longer play |
| Held objects per track | `MaxFrames` (8192) and `MaxBytes` (16 MiB), oldest dropped first | the playout's bounds, counted separately from it |
| Held statuses per track | `MaxFrames`, oldest dropped first | statuses wait behind held objects, count no bytes and never push one out |
| Held catalog objects | 1, until its key arrives or a newer one replaces it | a catalog does not expire as a frame does |

- Held objects keep the arrival time taken before decryption. With the group assembler and the playout, a video track can therefore hold up to three times `MaxFrames` and `MaxBytes` (48 MiB by default).
- Each dropped held object counts in `TrackWatchStats.NoKey`, each object under a retired key in `Retired`, and each catalog object that does not open in `WatchStats.BadCatalogs`.
- The keys themselves are bounded by kolo-moq's `secobj`: a receiver keeps a sender's current key and the one it replaced for `KeyringConfig.Retain` (450 ms: `MaxDelay` plus `MaxBehind`) after the first object under the newer key, and for `KeyringConfig.RetainMax` (`Retain` plus 400 ms) at most after the newer key arrived; an object seen before in the same subscription, or more than `ReplayGroups` (64) groups behind the newest of its key, is refused, in a window of at most `ReplayObjects` (8192) entries per subscription; a sender rotates beyond `SenderConfig.MaxTracks` (1024, the catalog's default `MaxTracks`) tracks per key and after 2^24 units of 16 KiB of plaintext per track key.
- Calls of up to 20 participants are tested. This is a testing bound, not a cryptographic one; the library sets no limit.
- Overhead: a protected audio object is 24 bytes larger than an unprotected one (length prefix, list marker and length, tag and Key ID property), 9.6 kbit/s at 50 objects per second; under 1 percent for video.

## Protected group calls: measured

Measured on a laptop with 10 cores (arm64), 24 GB, loopback, without the race detector: one `kolo-moq` Relay with `RoomPolicy`, every participant in one process with its own identity, session, `Sender` and `Keyring`, publishing 32 kbit/s constant bitrate audio (20 ms, 80 bytes) and a 1 Mbit/s 30 fps video (4 KiB delta frames, 32 KiB keyframes every 2 s and after each rotation) and watching every other participant. The machine was shared with other jobs, so the figures are an order of magnitude, not a guarantee.

| Participants | 4 | 8 | 20 |
|---|---|---|---|
| Key delivery to members that already have a session with the sender (join, leave, camera toggle, ten join and leave rounds), p50 / p95 / max | 0.3 / 5.1 / 5.4 ms | 0.7 / 1.3 / 5.7 ms | 5.2 / 8.8 / 30.8 ms |
| Key delivery to a new member, session set-up included, p50 / max | 3.6 / 6.2 ms | 3.3 / 205 ms | 14.9 / 222 ms |
| First video frame of a joiner, from the start of its join, p50 / max | 126 / 127 ms | 117 / 117 ms | 137 / 142 ms |
| Frames lost in steady state, at a rotation on join or leave | 0 | 0 | 0 |
| Video frames lost at a camera toggle, per viewer | 3 | 3 | 3 |
| CPU busy in steady state (whole process, relay included) | 0.33 cores | 1.11 cores | 4.0 cores |
| Heap after a collection, per participant | 1.9 MiB | 2.5 MiB | 5.6 MiB |

- The hold limit of 400 ms is met with a wide margin at every size: the p95 of key delivery to a member that has a session is under 10 ms and its maximum 31 ms. The tests assert the 400 ms bound at the p95 for 4, 8 and 20 participants.
- A first video frame includes the playout's `MinDelay` of 100 ms. The 200 ms maxima belong to the session set-up of a new member; key delivery over an existing session never exceeded 31 ms.
- A video track that is removed and added again loses the 3 frames its playout still held (100 ms) and resumes at the next catalog; the new key is in place before the first frame under it. These figures were measured with the camera off for 1 s; the test now adds the track again at once.
- A joiner cannot open the objects the relay holds from before it joined (15 to 17 per sender: the rest of the current video group and the catalog); they count as `Retired` and `BadCatalogs` at the joiner and nowhere else. No member ever opened an object sealed after its removal, and no object the relay carried held plaintext.
- Per frame, on the same machine: sealing an 80-byte audio frame takes 143 ns (a protected `WriteFrame` 604 ns, 410 ns unprotected); opening one takes 300 ns, a 32 KiB keyframe 5.1 us and 100 KiB 12.3 us.
- After teardown no goroutine of a call is left.
- With the race detector (about ten times slower) the 20-participant call saturates every core and loses most of its video, so its test runs without it; 4 and 8 run under `-race`.

## Timestamps

- `Clock` reads the wall clock once and then adds monotonic time, so a clock step never shows. Timestamps count from the Unix epoch.
- `Rescale`, `ToDuration` and `FromDuration` round to the nearest unit with a 128-bit intermediate. A tick count survives a round trip through `time.Duration` for any timescale up to 1 GHz. Overflow is `ErrTimestampRange`, never a wrap.
- Arithmetic on peer-supplied timestamps saturates and never panics.
