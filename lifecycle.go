package actorxdist

import (
	"context"
	"errors"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/errwire"
	"github.com/nats-io/nats.go"
)

// controlGeneration selects only an existing, conditionally confirmed generation.
// It never invokes Activate. Concurrent business selection still uses bind's
// single-generation fence, including for address copies.
func (addr Addr) controlGeneration(ctx context.Context) (string, error) {
	if gen := addr.generation(); gen != "" {
		return gen, nil
	}
	o, err := addr.rt.net.lookup(ctx, addr.key)
	if err != nil {
		return "", err
	}
	if err = addr.bind(o.value.Generation); err != nil {
		// A concurrent operation already fixed this shared handle; control that one.
		return addr.generation(), nil
	}
	return o.value.Generation, nil
}

// Stop closes admission for this handle's fixed generation. It does not wait for
// business cleanup. A held activation failure is released before Stop succeeds.
// The first accepted cause wins. The Runtime supplies a bounded control deadline.
func (addr Addr) Stop(cause error) error {
	if addr.IsZero() {
		return actorx.ErrSetup
	}
	if err := addr.rt.checkOpen(); err != nil {
		return err
	}
	if addr.rt.net == nil {
		return actorx.ErrSetup
	}
	ctx, cancel := managementContext()
	defer cancel()
	gen, err := addr.controlGeneration(ctx)
	if err != nil {
		return err
	}
	n := addr.rt.net
	p, terminalErr := n.registry.exit(ctx, addr.key, gen)
	if terminalErr == nil && p.value.Status == "COMMITTED" && p.value.Disposition == "RELEASED" {
		return nil
	}
	if errors.Is(terminalErr, ErrProtocol) {
		return terminalErr
	}

	// Route only through matching current or terminal metadata. In particular,
	// never borrow a newer generation's factory token after DecodeAddr.
	factory := ""
	if terminalErr == nil {
		factory = p.value.Factory
	}
	o, ownerErr := n.registry.owner(ctx, addr.key)
	if ownerErr == nil && o.value.Generation == gen {
		factory = o.value.Factory
	}
	if factory == "" {
		return errors.Join(ErrExitUnknown, terminalErr, ownerErr)
	}
	payload, err := errwire.Encode(cause, errwire.Source{Operation: "stop", Type: addr.key.Type, ID: addr.key.ID, Generation: gen})
	if err != nil {
		return err
	}
	_, err = n.factoryRequest(ctx, "stop", addr.key, gen, factory, "", payload)
	if err != nil {
		// Completion can race the ACK or removal of the original Factory entry.
		if result, readErr := n.registry.exit(ctx, addr.key, gen); readErr == nil && result.value.Status == "COMMITTED" && result.value.Disposition == "RELEASED" {
			return nil
		}
	}
	return err
}

// Wait observes the committed terminal evidence for one generation. It never
// stops or activates an object. Only this Addr state caches the immutable result;
// a cached result remains available after its observing Runtime closes.
func (addr Addr) Wait(ctx context.Context) (exit actorx.Exit, waitErr error) {
	if addr.IsZero() || ctx == nil {
		return actorx.Exit{}, actorx.ErrSetup
	}
	defer func() {
		if waitErr != nil && errors.Is(addr.rt.checkOpen(), actorx.ErrClosed) {
			waitErr = actorx.ErrClosed
		}
	}()
	s := addr.state
	s.mu.Lock()
	if s.exit != nil {
		result := *s.exit
		s.mu.Unlock()
		return result, nil
	}
	s.mu.Unlock()
	if err := addr.rt.checkOpen(); err != nil {
		return actorx.Exit{}, err
	}
	if addr.rt.net == nil {
		return actorx.Exit{}, actorx.ErrSetup
	}

	// Bound active observers independently of execution, without a polling worker
	// or retained history per handle. Canceled observers immediately return slots.
	s.mu.Lock()
	addr.rt.mu.Lock()
	if addr.rt.closed || s.waiters >= 16 || addr.rt.resolvers >= addr.rt.limits.waiters() {
		err := actorx.ErrFull
		if addr.rt.closed {
			err = actorx.ErrClosed
		}
		addr.rt.mu.Unlock()
		s.mu.Unlock()
		return actorx.Exit{}, err
	}
	s.waiters++
	addr.rt.resolvers++
	addr.rt.mu.Unlock()
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.waiters--
		s.mu.Unlock()
		addr.rt.mu.Lock()
		addr.rt.resolvers--
		addr.rt.mu.Unlock()
	}()
	ctx, cancel := requestBudget(ctx)
	defer cancel()
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	watch := context.AfterFunc(addr.rt.life, stop)
	defer watch()
	gen, err := addr.controlGeneration(ctx)
	if err != nil {
		if addr.rt.checkOpen() != nil {
			return actorx.Exit{}, actorx.ErrClosed
		}
		return actorx.Exit{}, err
	}
	for {
		p, readErr := addr.rt.net.registry.exit(ctx, addr.key, gen)
		if readErr == nil && p.value.Status == "COMMITTED" {
			reason, err := errwire.Decode(p.value.Cause)
			if err != nil {
				return actorx.Exit{}, err
			}
			result := actorx.Exit{Reason: reason}
			s.mu.Lock()
			s.exit = &result
			s.mu.Unlock()
			return result, nil
		}
		if addr.rt.checkOpen() != nil {
			return actorx.Exit{}, actorx.ErrClosed
		}
		if errors.Is(readErr, ErrProtocol) {
			return actorx.Exit{}, readErr
		}
		if readErr != nil && !errors.Is(readErr, nats.ErrKeyNotFound) {
			return actorx.Exit{}, readErr
		}
		o, ownerErr := addr.rt.net.registry.owner(ctx, addr.key)
		if ownerErr != nil && !errors.Is(ownerErr, nats.ErrKeyNotFound) {
			return actorx.Exit{}, errors.Join(ErrExitUnknown, ownerErr)
		}
		if readErr != nil && (ownerErr != nil || o.value.Generation != gen || o.value.State == "INACTIVE") {
			// PREPARED may have been written between the two reads above.
			if _, latestErr := addr.rt.net.registry.exit(ctx, addr.key, gen); latestErr == nil {
				continue
			}
			return actorx.Exit{}, ErrExitUnknown
		}
		if ownerErr == nil && o.value.Generation == gen && o.value.State != "INACTIVE" {
			if _, err := addr.rt.net.confirmOwner(ctx, o); err != nil {
				// Confirmation can race ordinary release. Re-read the same terminal
				// and ownership before classifying that race as an unreachable owner.
				latest, terminalErr := addr.rt.net.registry.exit(ctx, addr.key, gen)
				if terminalErr == nil && latest.value.Status == "COMMITTED" {
					continue
				}
				current, currentErr := addr.rt.net.registry.owner(ctx, addr.key)
				if errors.Is(currentErr, nats.ErrKeyNotFound) ||
					currentErr == nil && (current.value.Generation != gen || current.value.State == "INACTIVE") {
					continue
				}
				return actorx.Exit{}, errors.Join(ErrOwnerUnavailable, err)
			}
		}
		if !pause(ctx, 50*time.Millisecond) {
			if addr.rt.checkOpen() != nil {
				return actorx.Exit{}, actorx.ErrClosed
			}
			return actorx.Exit{}, errors.Join(ErrExitUnknown, ctx.Err())
		}
	}
}
