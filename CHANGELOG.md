# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `TrackStats.LateRun`: how long the current run of frames skipped for being more than `MaxBehind` late has lasted.

### Fixed

- An audio track whose timeline fell behind the other tracks' (a sender's microphone that lost samples and stamps by count) was dropped as late until the call ended while the video played. Once every audio frame has been too late for 2 s while another track plays on time, its due times move by its lateness and it plays again; the move ends once its frames arrive in time without it for 2 s.

## [0.1.0] - 2026-11-20

### Added

- First release of kolo-media.
