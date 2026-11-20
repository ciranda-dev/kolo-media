package kolomedia

import (
	"errors"
	"fmt"
	"strings"

	kolomoq "github.com/ciranda-dev/kolo-moq"
)

var (
	// ErrInvalidFrame means a frame is empty, out of range or does not fit its track.
	ErrInvalidFrame = errors.New("kolomedia: invalid frame")
	// ErrInvalidTrack means a track description is not valid.
	ErrInvalidTrack = errors.New("kolomedia: invalid track")
	// ErrInvalidCatalog means a catalog is malformed, over a limit or not valid.
	ErrInvalidCatalog = errors.New("kolomedia: invalid catalog")
	// ErrCatalogFormat means an encoded catalog has an unknown format version.
	ErrCatalogFormat = errors.New("kolomedia: unsupported catalog format")
	// ErrInvalidTimescale means a timescale of zero.
	ErrInvalidTimescale = errors.New("kolomedia: invalid timescale")
	// ErrTimestampRange means a timestamp or duration does not fit the target unit.
	ErrTimestampRange = errors.New("kolomedia: timestamp out of range")
	// ErrTimestampOrder means a frame's timestamp is not after the previous frame's.
	ErrTimestampOrder = errors.New("kolomedia: timestamp not after the previous frame")
	// ErrNeedKeyframe means a video track was given a delta frame where a group must start: before
	// its first keyframe, or after a failed TrackPublisher.WriteFrame.
	ErrNeedKeyframe = errors.New("kolomedia: video track needs a keyframe")
	// ErrMissingProperty means an object lacks a property its track requires.
	ErrMissingProperty = errors.New("kolomedia: missing property")
	// ErrDuplicateProperty means an object carries a property more than once.
	ErrDuplicateProperty = errors.New("kolomedia: duplicate property")
	// ErrMalformedProperty means a property has the wrong form or value, or does not belong to the track's kind.
	ErrMalformedProperty = errors.New("kolomedia: malformed property")
	// ErrPropertyMismatch means a property disagrees with the track's catalog entry.
	ErrPropertyMismatch = errors.New("kolomedia: property does not match the catalog")
	// ErrInvalidConfig means a playout configuration is not valid.
	ErrInvalidConfig = errors.New("kolomedia: invalid configuration")
	// ErrUnknownTrack means a track name the playout or broadcast does not have.
	ErrUnknownTrack = errors.New("kolomedia: unknown track")
	// ErrFrameTooLarge means a frame is larger than a whole track buffer, or its object is larger than
	// the origin's MaxObjectSize (the error then also wraps kolomoq.ErrObjectTooLarge).
	ErrFrameTooLarge = errors.New("kolomedia: frame too large")
	// ErrClosed means the playout, broadcast, track publisher or watcher was closed.
	ErrClosed = errors.New("kolomedia: closed")
	// ErrNoMatch means no track of the catalog fits the receiver's capabilities.
	ErrNoMatch = errors.New("kolomedia: no track matches the capabilities")
	// ErrBroadcastEnded means the broadcast's catalog track ended: the publisher ended it, or a relay
	// ended it with a kolomoq.PublishDoneError, which the error then wraps.
	ErrBroadcastEnded = errors.New("kolomedia: broadcast ended")
	// ErrSessionEnded means the MoQ session ended; the error wraps the session's reason when it has one.
	ErrSessionEnded = errors.New("kolomedia: session ended")
	// ErrTrackEnded means a track the watcher was playing ended: the publisher ended it, or a relay
	// ended it with a kolomoq.PublishDoneError, which the error then wraps.
	ErrTrackEnded = errors.New("kolomedia: track ended")
	// ErrBroadcastNotFound means the peer has no broadcast under the namespace; the error wraps the kolomoq request error.
	ErrBroadcastNotFound = errors.New("kolomedia: broadcast not found")
	// ErrBroadcastExists means the origin already has a broadcast under the namespace; the error wraps kolomoq.ErrTrackExists.
	ErrBroadcastExists = errors.New("kolomedia: broadcast already exists")
	// ErrTooManyTracks means the origin has no room for another track; the error wraps kolomoq.ErrTooManyTracks.
	ErrTooManyTracks = errors.New("kolomedia: too many tracks")
)

// moqError gives an error of kolo-moq a kolomedia: prefix and keeps it in the chain, with a sentinel
// where a caller needs one.
func moqError(op string, err error) error {
	var re *kolomoq.RequestError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &re) && re.Code == kolomoq.ReqDoesNotExist:
		return fmt.Errorf("%w: %w", ErrBroadcastNotFound, err)
	case errors.Is(err, kolomoq.ErrTooManyTracks):
		return fmt.Errorf("%w: %w", ErrTooManyTracks, err)
	case errors.Is(err, kolomoq.ErrObjectTooLarge):
		return fmt.Errorf("%w: %w", ErrFrameTooLarge, err)
	}
	return fmt.Errorf("kolomedia: %s: %w", op, err)
}

// PropertyError reports a malformed, missing, duplicate or mismatched LOC property of an object.
type PropertyError struct {
	Type uint64
	Err  error
}

// Error returns the message with the property type in hex.
func (e *PropertyError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("kolomedia: property 0x%x", e.Type)
	}
	return fmt.Sprintf("kolomedia: property 0x%x: %s", e.Type, strings.TrimPrefix(e.Err.Error(), "kolomedia: "))
}

// Unwrap returns the sentinel error.
func (e *PropertyError) Unwrap() error { return e.Err }
