// Package kolomedia describes, packages, publishes and plays out the encoded
// audio and video of a Kolo broadcast on kolo-moq: a binary catalog, LOC
// objects (draft-ietf-moq-loc-04), a shared media clock, a receive-side
// playout with jitter buffers and A/V sync, rendition selection, and
// Broadcast and Watch to tie them to MoQ sessions. It never captures,
// encodes, decodes or renders, and it does not use cgo.
//
// Experimental: the API may change before v1.0.0.
package kolomedia
