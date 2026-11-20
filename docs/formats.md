# Formats

## Broadcast layout

A broadcast is a kolo-moq namespace. Its catalog is the track named `catalog` (`CatalogTrack`, a reserved name): one group per catalog version, whose group ID is the version, with the encoded catalog as object 0. A new version repeats the whole catalog; there are no delta updates. Each media track is the track named by its `Track.Name` in the same namespace.

## Catalog encoding

Canonical binary encoding on kolo-codec: one catalog value has exactly one encoding.

- Format byte 1, then the version and the track count as uvarints.
- Each track: name, kind (1 audio, 2 video), codec, timescale, priority, group, config, bitrate.
- Then for audio: sample rate, channels and frame duration in nanoseconds.
- Then for video: width, height and frame rate as IEEE 754 bits (8 bytes, little-endian).
- Strings and bytes are length-prefixed; integers are minimal uvarints.

`ParseCatalog` rejects non-minimal varints, trailing bytes, unknown kinds and codecs, an unknown format byte (`ErrCatalogFormat`) and any field the kind does not use. It never panics.

Rules for tracks:

- Codecs are a closed set: `opus`, `pcm-s16le` (audio), `vp8`, `vp9` (video). Opus sample rates and packet durations, and VP8 and VP9 frame sizes, are checked against the codec specifications.
- `Timescale` is required, with no implicit default, so that 0 and 1000000 never encode the same track twice.
- `FrameRate` is a float64. NaN, infinities, negative values and negative zero are rejected.
- Tracks with the same non-empty `Group` are alternatives of one source: one kind, one timescale. A receiver can switch between them at group boundaries.
- Names are not empty and unique; `catalog` is reserved.
- Changing a track's codec config means a new catalog version.

## LOC objects

Objects follow draft-ietf-moq-loc-04. The payload is the codec's frame as is.

| Property | Type | When |
|---|---|---|
| TIMESTAMP | 0x10 | every object |
| TIMESCALE | 0x08 | the track's timescale is not 1000000 |
| AUDIO_LEVEL | 0x0C | the frame has one, as RFC 6464: voice flag 0x80, level 0 to 127 in -dBov |
| VIDEO_CONFIG | 0x0D | first object of each group, when a video track has a config |
| AUDIO_CONFIG | 0x0F | first object of each group, when an audio track has a config |

- Properties are written in ascending type order, as the MoQ key-value delta encoding needs.
- A track at timescale 1000000 carries microseconds since the Unix epoch, as LOC defines for an object without TIMESCALE; `Clock` produces exactly that.
- Groups: a video group starts at every keyframe, and object 0 of a video group is a keyframe. An audio group holds one frame, or the frames of `AudioGroupDuration`.
- VIDEO_FRAME_MARKING (0x09) is not written or read: LOC defers its bit layout to RFC 9626, which is not in the draft text. Keyframes are known from the group structure.

`Unpack` (receiving):

- requires TIMESTAMP;
- requires TIMESCALE to equal the catalog's, and to be absent only when the catalog says 1000000;
- requires a config property to equal the catalog's, and to be present on object 0 when the catalog has one;
- rejects properties of the other kind and repeated known properties;
- ignores unknown property types; kolo-moq hands Immutable Properties (0x0B) over as one property holding their serialized contents, which `Unpack` ignores like any unknown type, so a LOC property inside them is not read;
- fills `Duration` from the catalog: the audio frame duration, or one frame at the video frame rate, 0 if unknown.

Errors are a `*PropertyError` wrapping `ErrMissingProperty`, `ErrDuplicateProperty`, `ErrMalformedProperty` or `ErrPropertyMismatch`.

## Protected objects

A broadcast made with `NewSecureBroadcast` seals every object, the catalog included, with kolo-moq's `secobj` package: the Kolo profile of draft-ietf-moq-secure-objects-01, cipher suite AES_128_GCM_SHA256_128 (0x0004), with draft-ietf-moq-transport-22 encodings. Objects do not interoperate with other Secure Objects implementations.

| Part | What a relay sees |
|---|---|
| Namespace, track name, Group ID, Object ID, publisher priority | as above; authenticated |
| Visible properties | exactly one: Immutable Properties (0x0B) holding the Key ID at type 0x78 (`78` then the vi64 Key ID); authenticated |
| Payload | the AEAD ciphertext and its 16-byte tag |

The plaintext is the vi64 length of the payload, the payload (the codec frame or the encoded catalog) and, when there are properties, the marker `00 0A`, a vi64 length and the LOC properties of the table above in ascending type order. AUDIO_LEVEL is not written on a protected track: its vi64 takes 1 byte for silence and 2 for voice, so ciphertext sizes would reveal speech. A protected object is therefore 1 to 9 length bytes and 16 tag bytes larger than its plaintext parts; with LOC properties the marker and list length add 3 or more. The catalog has no properties: its sealed payload is the encoded catalog plus its vi64 length and the tag.

A protected watch opens each object first and takes LOC properties only from the decrypted list. Visible properties other than 0x0B, track properties and object status are not authenticated: visible properties and track properties are ignored, and object status (End of Group, End of Track) is only a sign that media stopped.

## References

- [Low Overhead Media Container, draft-ietf-moq-loc-04](https://datatracker.ietf.org/doc/html/draft-ietf-moq-loc-04)
  (Internet-Draft, July 2026, expires 21 January 2027).
  Takes: the LOC object properties.
  Not implemented: VIDEO_FRAME_MARKING. Revision 05 (2 November 2026) exists and is not followed.
- [RFC 6464, RTP Header Extension for Client-to-Mixer Audio Level Indication](https://www.rfc-editor.org/info/rfc6464)
  (Proposed Standard, December 2011).
  Takes: the AUDIO_LEVEL value: voice flag 0x80, level 0 to 127 in -dBov.
  Not implemented: the RTP header extension.
- [RFC 9626, Video Frame Marking RTP Header Extension](https://www.rfc-editor.org/info/rfc9626)
  (Experimental, March 2025).
  Not implemented: not written or read.
- [Media over QUIC Transport, draft-ietf-moq-transport-22](https://datatracker.ietf.org/doc/html/draft-ietf-moq-transport-22)
  (Internet-Draft, October 2026, expires 4 April 2027).
  Takes: the transport, through kolo-moq.
- [End-to-End Secure Objects for Media over QUIC Transport, draft-ietf-moq-secure-objects-01](https://datatracker.ietf.org/doc/html/draft-ietf-moq-secure-objects-01)
  (Internet-Draft, July 2026, expires 7 January 2027, still the current revision).
  Takes: the Kolo profile of protected objects, through kolo-moq.
- [RFC 6716, Definition of the Opus Audio Codec](https://www.rfc-editor.org/info/rfc6716)
  (Proposed Standard, September 2012, updated by RFC 8251).
  Takes: the Opus codec identity in the catalog.
