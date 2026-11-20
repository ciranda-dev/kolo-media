package kolomedia

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	kolomoq "github.com/ciranda-dev/kolo-moq"
	"github.com/ciranda-dev/kolo-moq/secobj"
)

// NewSecureBroadcast is NewBroadcast with every object, the catalog included, sealed by s under
// s.Namespace(). Protected audio tracks carry no AUDIO_LEVEL.
func NewSecureBroadcast(origin *kolomoq.Origin, s *secobj.Sender) (*Broadcast, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: nil sender", ErrInvalidConfig)
	}
	return startBroadcast(origin, s.Namespace(), s)
}

// sealedLen is the payload length of a protected object without private properties: the vi64
// length prefix (draft-ietf-moq-transport-22 section 8.1), n bytes and the 16-byte tag.
func sealedLen(n int) int {
	l := 1
	for l < 8 && uint64(n) >= 1<<(7*l) {
		l++
	}
	return l + n + 16
}

// opener reads the objects of one subscription and, under keys, opens each with op, the
// subscription's own Opener. Visible properties are dropped: LOC properties come only from the
// decrypted list. An object whose Key ID is newer than every key held may wait for a key message in
// flight: it is held for at most maxAge (no bound when 0), within maxFrames and maxBytes, and retried
// when the keyring changes. Statuses and the end of the subscription wait behind held objects. drop
// counts an object not delivered, with secobj.ErrNoKey when its key did not arrive in time.
type opener struct {
	keys      changes // the *secobj.Keyring of op
	op        *secobj.Opener
	maxAge    time.Duration
	maxFrames int
	maxBytes  int
	drop      func(err error)
	// newest keeps only the held object of the highest group, whatever the arrival order: catalog
	// versions are group IDs, and groups travel on separate streams.
	newest bool

	held  []heldObject
	ready []heldObject
	bytes int
	// statuses counts the statuses in held.
	statuses int
	wake     <-chan struct{}
	end      error // why the subscription ended while objects were held
}

// changes is the part of a secobj.Keyring an opener uses: a channel closed at the next key installed.
type changes interface{ Changed() <-chan struct{} }

// catalogOpener reads a catalog subscription: one catalog object, the newest, waits for its key
// without a time bound.
func catalogOpener(keys *secobj.Keyring, op *secobj.Opener, drop func(error)) *opener {
	return &opener{keys: keys, op: op, maxFrames: 1, maxBytes: math.MaxInt, newest: true, drop: drop}
}

type heldObject struct {
	obj kolomoq.Object
	at  time.Time
}

var errWake = errors.New("kolomedia: wake")

// next returns the next object of sub and its arrival time, taken before decryption. Under keys,
// objects that do not open are counted and skipped.
func (o *opener) next(ctx context.Context, sub *kolomoq.Subscription) (kolomoq.Object, time.Time, error) {
	for {
		if len(o.ready) > 0 {
			h := o.ready[0]
			o.ready[0] = heldObject{}
			o.ready = o.ready[1:]
			return h.obj, h.at, nil
		}
		if o.end != nil && len(o.held) == 0 {
			return kolomoq.Object{}, time.Time{}, o.end
		}
		obj, err := o.read(ctx, sub)
		if err == errWake {
			o.retry()
			continue
		}
		if err != nil {
			if len(o.held) == 0 || ctx.Err() != nil {
				return kolomoq.Object{}, time.Time{}, err
			}
			o.end = err
			if o.maxAge == 0 {
				o.giveUp()
			}
			continue
		}
		at := time.Now()
		if o.op == nil {
			return obj, at, nil
		}
		if obj.Status != kolomoq.StatusNormal {
			if len(o.held) == 0 {
				return obj, at, nil
			}
			o.hold(obj, at, nil)
			continue
		}
		changed := o.keys.Changed()
		opened, err := o.open(obj)
		switch {
		case err == nil:
			return opened, at, nil
		case errors.Is(err, secobj.ErrNoKey):
			o.hold(obj, at, changed)
		default:
			o.drop(err)
		}
	}
}

// read reads the next object of sub; while objects are held it returns errWake when the keyring
// changes or the oldest held object expires, and after the end of sub it only waits for those.
func (o *opener) read(ctx context.Context, sub *kolomoq.Subscription) (kolomoq.Object, error) {
	if len(o.held) == 0 {
		return sub.ReadObject(ctx)
	}
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if o.maxAge > 0 {
		var stop context.CancelFunc
		rctx, stop = context.WithDeadline(rctx, o.held[0].at.Add(o.maxAge))
		defer stop()
	}
	wake, done := o.wake, make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-wake:
			cancel()
		case <-done:
		}
	}()
	if o.end != nil {
		<-rctx.Done()
		if err := ctx.Err(); err != nil {
			return kolomoq.Object{}, err
		}
		return kolomoq.Object{}, errWake
	}
	obj, err := sub.ReadObject(rctx)
	if err != nil && ctx.Err() == nil && err == rctx.Err() {
		return kolomoq.Object{}, errWake
	}
	return obj, err
}

func (o *opener) open(obj kolomoq.Object) (kolomoq.Object, error) {
	payload, props, err := o.op.Open(obj)
	if err != nil {
		return kolomoq.Object{}, err
	}
	obj.Payload, obj.Properties = payload, props
	return obj, nil
}

// hold keeps obj until its key arrives, or a status until the objects before it are resolved. The
// oldest held objects are dropped to stay within maxFrames and maxBytes. Statuses count no bytes,
// have their own bound of maxFrames, and never push an object out.
func (o *opener) hold(obj kolomoq.Object, at time.Time, changed <-chan struct{}) {
	if obj.Status != kolomoq.StatusNormal {
		if len(o.held) == 0 {
			o.ready = append(o.ready, heldObject{obj, at})
			return
		}
		if o.statuses >= o.maxFrames {
			i := slices.IndexFunc(o.held, func(h heldObject) bool { return h.obj.Status != kolomoq.StatusNormal })
			o.held = slices.Delete(o.held, i, i+1)
			o.statuses--
		}
		o.held = append(o.held, heldObject{obj, at})
		o.statuses++
		return
	}
	if o.newest && len(o.held) > o.statuses && obj.Group <= o.held[0].obj.Group {
		o.drop(secobj.ErrNoKey)
		return
	}
	n := objectBytes(&obj)
	if n > o.maxBytes {
		o.drop(secobj.ErrNoKey)
		return
	}
	// The head of held is always an object: release moves the statuses behind it out.
	for len(o.held) > 0 && (len(o.held)-o.statuses >= o.maxFrames || n > o.maxBytes-o.bytes) {
		o.bytes -= objectBytes(&o.held[0].obj)
		o.held[0] = heldObject{}
		o.held = o.held[1:]
		o.drop(secobj.ErrNoKey)
		o.release()
	}
	if len(o.held) == 0 {
		o.wake = changed
	}
	o.held = append(o.held, heldObject{obj, at})
	o.bytes += n
}

// release moves the statuses at the head of held, which wait for nothing now, to ready.
func (o *opener) release() {
	for len(o.held) > 0 && o.held[0].obj.Status != kolomoq.StatusNormal {
		o.ready = append(o.ready, o.held[0])
		o.held[0] = heldObject{}
		o.held = o.held[1:]
		o.statuses--
	}
}

// retry opens the held objects again, in arrival order, and drops those still without a key after
// maxAge. A key that arrived opens its objects even if this runs after their maxAge. A status stays
// held while an object before it does.
func (o *opener) retry() {
	now := time.Now()
	o.wake = o.keys.Changed()
	kept := o.held[:0]
	for _, h := range o.held {
		if h.obj.Status != kolomoq.StatusNormal {
			if len(kept) > 0 {
				kept = append(kept, h)
			} else {
				o.ready = append(o.ready, h)
				o.statuses--
			}
			continue
		}
		opened, err := o.open(h.obj)
		switch {
		case errors.Is(err, secobj.ErrNoKey) && (o.maxAge == 0 || now.Before(h.at.Add(o.maxAge))):
			kept = append(kept, h)
			continue
		case errors.Is(err, secobj.ErrNoKey):
			o.drop(secobj.ErrNoKey)
		case err == nil:
			o.ready = append(o.ready, heldObject{opened, h.at})
		default:
			o.drop(err)
		}
		o.bytes -= objectBytes(&h.obj)
	}
	clear(o.held[len(kept):])
	o.held = kept
}

// giveUp drops the held objects and releases the held statuses, in order.
func (o *opener) giveUp() {
	for _, h := range o.held {
		if h.obj.Status != kolomoq.StatusNormal {
			o.ready = append(o.ready, h)
			continue
		}
		o.drop(secobj.ErrNoKey)
	}
	clear(o.held)
	o.held, o.bytes, o.statuses = o.held[:0], 0, 0
}
