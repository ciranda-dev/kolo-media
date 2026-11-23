# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `CatalogLimits.MaxTracks` can be raised without a ceiling, on both sides: a value above 1024 is kept instead of falling back to 1024. A publisher sets `BroadcastOptions.CatalogLimits`, a watcher sets `WatchOptions.CatalogLimits`. A parse allocates 8.0 times the catalog's bytes and keeps 6.3 times (`BenchmarkParseCatalog`, see docs/limits.md).
- `Catalog.MarshalBinaryLimits`: `MarshalBinary` checked against a caller's `CatalogLimits`.
- `TrackStats.LateRun`: how long the current run of frames skipped for being more than `MaxBehind` late has lasted.

### Changed

- **Breaking:** `NewBroadcast(origin, ns)` and `NewSecureBroadcast(origin, sender)` take a last argument `opts *BroadcastOptions`, nil for the defaults. Pass `nil` where you passed nothing; pass `&kolomedia.BroadcastOptions{CatalogLimits: lim}` where you passed `lim`.
- **Breaking:** `NewBroadcast` and `NewSecureBroadcast` return `ErrInvalidConfig` for `CatalogLimits.MaxNameLen` or `MaxConfigLen` above their MoQ bound; they used to take the bound.
- Release step: go.mod still requires kolo-moq v0.1.0, whose origin caps tracks at 1000 by default, so `TestBroadcastTrackLimit` and `TestManyTracks` fail with `GOWORK=off`. Bump it to the new kolo-moq tag and run `go mod tidy` before tagging.
- **Breaking:** `CatalogLimits.MaxTracks` above 1024 now raises the limit; it used to take 1024. A receiver that passed a higher value and relied on the clamp now accepts catalogs of more tracks. The default is still 1024, and the catalog encoding is unchanged.
- **Breaking:** with kolo-moq's origin now uncapped by default (`OriginConfig.MaxTracks` 0), a default broadcast stops `AddTrack` at 1024 tracks with `ErrInvalidCatalog`, no longer at 999 with `ErrTooManyTracks`. `ErrTooManyTracks` is returned only when you set an origin cap.

### Fixed

- `TrackStats.LateRun` kept growing after the frames stopped; it is 0 once 2 s pass without a skipped frame.
- An audio track whose timeline fell behind the other tracks' (a sender's microphone that lost samples and stamps by count) was dropped as late until the call ended while the video played. Once every audio frame has been too late for 2 s while another track plays on time, its due times move by its lateness and it plays again; the move ends once its frames arrive in time without it for 2 s.

## [0.1.0] - 2026-11-20

### Added

- First release of kolo-media.
