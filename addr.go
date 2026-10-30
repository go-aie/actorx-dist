package actorxdist

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/go-aie/actorx-dist/internal/strictjson"
)

type addrState struct {
	mu         sync.Mutex
	generation string
	waiters    int
	resolving  *resolution
	exit       *actorx.Exit // one immutable committed result; never an execution Addr
}

// Addr holds a logical identity and shared, once-bound activation state. Copies
// share state; separate Addr/DecodeAddr calls allocate independent state.
// It retains no business object, handler or local execution address.
type Addr struct {
	key   ActorKey
	rt    *Runtime
	state *addrState
}

func newAddr(rt *Runtime, key ActorKey, generation string) Addr {
	return Addr{key: key, rt: rt, state: &addrState{generation: generation}}
}

// Key returns a pure identity copy, even after Runtime.Close.
func (addr Addr) Key() ActorKey { return addr.key }

// IsZero reports whether the handle is uninitialized, not target liveness.
func (addr Addr) IsZero() bool { return addr.state == nil || addr.rt == nil }

// Addr validates locally and creates an unbound handle without I/O. Omitting ID
// generates one new logical identity; an explicit empty ID is invalid.
func (rt *Runtime) Addr(actorType string, id ...string) (Addr, error) {
	if err := rt.checkOpen(); err != nil {
		return Addr{}, err
	}
	if len(id) > 1 {
		return Addr{}, actorx.ErrSetup
	}
	var value string
	if len(id) == 1 {
		value = id[0]
	} else {
		var err error
		value, err = identity.NewOpaque()
		if err != nil {
			return Addr{}, err
		}
	}
	if err := identity.Validate(actorType, value); err != nil {
		return Addr{}, err
	}
	return newAddr(rt, ActorKey{Type: actorType, ID: value}, ""), nil
}

// bind never replaces a selected generation, including after failure or exit.
func (addr Addr) bind(generation string) error {
	if addr.IsZero() || generation == "" {
		return actorx.ErrSetup
	}
	addr.state.mu.Lock()
	defer addr.state.mu.Unlock()
	if addr.state.generation != "" && addr.state.generation != generation {
		return ErrActivationUnknown
	}
	addr.state.generation = generation
	return nil
}

func (addr Addr) generation() string {
	addr.state.mu.Lock()
	defer addr.state.mu.Unlock()
	return addr.state.generation
}

// MarshalJSON exports an atomic identity snapshot without requiring a live Runtime.
func (addr Addr) MarshalJSON() ([]byte, error) {
	if addr.IsZero() {
		return nil, actorx.ErrSetup
	}
	return json.Marshal(struct {
		Version    int    `json:"version"`
		Type       string `json:"type"`
		ID         string `json:"id"`
		Generation string `json:"generation,omitempty"`
	}{
		1,
		addr.key.Type,
		addr.key.ID,
		addr.generation(),
	})
}

// UnmarshalJSON requires an explicit receiving Runtime; use Runtime.DecodeAddr.
func (*Addr) UnmarshalJSON([]byte) error { return actorx.ErrSetup }

// DecodeAddr imports a strict identity snapshot, without lookup or activation.
func (rt *Runtime) DecodeAddr(data []byte) (Addr, error) {
	if err := rt.checkOpen(); err != nil {
		return Addr{}, err
	}
	if err := strictjson.Validate(data, 2048, 4, 16); err != nil {
		return Addr{}, fmt.Errorf("%w: %v", actorx.ErrSetup, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return Addr{}, actorx.ErrSetup
	}
	for name := range fields {
		if name != "version" && name != "type" && name != "id" && name != "generation" {
			return Addr{}, actorx.ErrSetup
		}
	}
	var version *int
	if err := json.Unmarshal(fields["version"], &version); err != nil || version == nil {
		return Addr{}, actorx.ErrSetup
	}
	if *version != 1 {
		return Addr{}, ErrProtocol
	}
	var actorType, id, generation string
	for name, dest := range map[string]*string{"type": &actorType, "id": &id} {
		if err := json.Unmarshal(fields[name], dest); err != nil || string(fields[name]) == "null" {
			return Addr{}, actorx.ErrSetup
		}
	}
	if raw, ok := fields["generation"]; ok {
		if err := json.Unmarshal(raw, &generation); err != nil || generation == "" {
			return Addr{}, actorx.ErrSetup
		}

		// Generation is the canonical 128-bit opaque token fixed in AD00.
		if !validGeneration(generation) {
			return Addr{}, actorx.ErrSetup
		}
	}
	if err := identity.Validate(actorType, id); err != nil {
		return Addr{}, err
	}
	return newAddr(rt, ActorKey{Type: actorType, ID: id}, generation), nil
}

func validGeneration(value string) bool {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(value)
	return err == nil && len(raw) == 16 && base64.RawURLEncoding.EncodeToString(raw) == value
}

func (addr Addr) dispatch(ctx context.Context, kind actorx.Kind, msg any, result reflect.Type) (any, error) {
	if addr.IsZero() || ctx == nil {
		return nil, actorx.ErrSetup
	}
	if err := addr.rt.checkOpen(); err != nil {
		return nil, err
	}
	if addr.rt.invoke == nil {
		return nil, fmt.Errorf("%w: business network invocation is not installed (AD04)", actorx.ErrSetup)
	}
	return addr.rt.invoke(ctx, addr, kind, msg, result)
}

// Send waits for admission by the fixed generation through NATS.
func (addr Addr) Send(ctx context.Context, msg any) error {
	_, err := addr.dispatch(ctx, actorx.SendKind, msg, nil)
	return err
}

// Call waits for a validated final response from the fixed generation.
func (addr Addr) Call[R any](ctx context.Context, msg any) (R, error) {
	var zero R
	value, err := addr.dispatch(ctx, actorx.CallKind, msg, reflect.TypeFor[R]())
	if err != nil {
		return zero, err
	}
	if value == nil && reflect.TypeFor[R]().Kind() == reflect.Interface {
		return zero, nil
	}
	result, ok := value.(R)
	if !ok {
		return zero, actorx.ErrReplyType
	}
	return result, nil
}

// Stream returns a lazy, single-use sequence. A normal loop body acknowledges
// its chunk; break, return and panic cancel the request without acknowledging it.
// Keep msg and its referenced data stable until iteration starts.
func (addr Addr) Stream[C any](ctx context.Context, msg any) iter.Seq2[C, error] {
	var used atomic.Bool

	return func(yield func(C, error) bool) {
		var zero C
		if !used.CompareAndSwap(false, true) || addr.IsZero() || ctx == nil {
			yield(zero, actorx.ErrSetup)
			return
		}
		if err := addr.rt.checkOpen(); err != nil {
			yield(zero, err)
			return
		}
		if addr.rt.openStream == nil {
			yield(zero, actorx.ErrSetup)
			return
		}

		s, err := addr.rt.openStream(ctx, addr, msg, reflect.TypeFor[C]())
		if err != nil {
			yield(zero, err)
			return
		}
		defer s.close()

		fail := func(err error) {
			s.mu.Lock()
			s.err = err
			s.mu.Unlock()

			// Release request resources before the caller handles the error.
			s.close()
			yield(zero, err)
		}

		for {
			f, ok, err := s.next()
			if !ok {
				// next records local aborts itself; a drained producer failure
				// is reported without turning it into a local cancellation.
				s.close()
				if err != nil {
					yield(zero, err)
				}
				return
			}

			if !takeSlot(s.n.codecSlots) {
				fail(errors.Join(ErrOutcomeUnknown, actorx.ErrFull))
				return
			}
			value, err := decodeValue(s.n.rt, f.Payload, s.result)
			<-s.n.codecSlots
			if err != nil {
				fail(errors.Join(ErrOutcomeUnknown, err))
				return
			}

			chunk, err := streamValueAs[C](value)
			if err != nil {
				fail(err)
				return
			}
			if err := s.stopped(); err != nil {
				fail(err)
				return
			}

			if !yield(chunk, nil) {
				return
			}
			if err := s.acknowledge(f); err != nil {
				fail(err)
				return
			}
		}
	}
}

func nilable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return true
	}
	return false
}

func streamValueAs[C any](value any) (C, error) {
	var zero C
	if value == nil && nilable(reflect.TypeFor[C]()) {
		return zero, nil
	}
	if chunk, ok := value.(C); ok {
		return chunk, nil
	}
	return zero, actorx.ErrReplyType
}
