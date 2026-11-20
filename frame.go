package kolomedia

import (
	"fmt"
	"math"
)

// Frame is one encoded audio or video frame of a track, in decode order.
type Frame struct {
	// Timestamp is the capture time in the track's timescale.
	Timestamp uint64
	// Duration is the frame's length in the track's timescale, 0 if unknown.
	Duration uint64
	// Keyframe marks a video frame that decodes without earlier frames. Every audio frame is one.
	Keyframe bool
	// AudioLevel is the optional level of an audio frame.
	AudioLevel *AudioLevel
	// Data is the codec's frame as is. It must not be empty.
	Data []byte
}

// AudioLevel is the level of an audio frame as RFC 6464 defines it.
type AudioLevel struct {
	// Voice reports voice activity.
	Voice bool
	// Level is the magnitude in -dBov, from 0 (loudest) to 127 (silence).
	Level uint8
}

func validateFrame(t *Track, f *Frame) error {
	if len(f.Data) == 0 {
		return fmt.Errorf("%w: data is empty", ErrInvalidFrame)
	}
	if f.Timestamp > math.MaxUint64-f.Duration {
		return fmt.Errorf("%w: timestamp plus duration overflows", ErrInvalidFrame)
	}
	if f.AudioLevel != nil {
		if t.Kind != Audio {
			return fmt.Errorf("%w: audio level on a %s track", ErrInvalidFrame, t.Kind)
		}
		if f.AudioLevel.Level > 127 {
			return fmt.Errorf("%w: audio level %d over 127", ErrInvalidFrame, f.AudioLevel.Level)
		}
	}
	if t.Codec == PCMS16LE && len(f.Data)%(2*int(t.Channels)) != 0 {
		return fmt.Errorf("%w: %d bytes is not a whole number of %d-channel samples", ErrInvalidFrame, len(f.Data), t.Channels)
	}
	return nil
}
