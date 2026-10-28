package actorxdist

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
)

// creation is the Runtime's resource ledger entry, not an execution object.
// done publishes the immutable result; it survives a dead Factory mailbox.
type creation struct {
	key          ActorKey
	registration *Registration
	generation   string
	done         chan struct{}
	once         sync.Once
	local        actorx.Addr
	err          error
	releasing    bool            // Factory mailbox only
	activation   *activationWork // nil for the local integration seam
}

type createRequest struct {
	key        ActorKey
	generation string
}

type factory struct {
	rt           *Runtime
	spec         registration
	registration *Registration
	entries      map[ActorKey]*creation // accessed only by its mailbox
}

func (f *factory) Bindings() []actorx.Binding {
	return []actorx.Binding{
		actorx.Handle(f.create),
		actorx.HandleDeferred(f.release),
		actorx.HandleDeferred(f.confirm),
		actorx.Handle(f.activate),
		actorx.HandleMessage(f.forget),
		actorx.HandleMessage(f.stopped),
		actorx.HandleDeferred(f.stopActivation),
		actorx.HandleMessage(f.stopping),
	}
}

func (f *factory) create(ctx actorx.Context, msg createRequest) (*creation, error) {
	if err := identity.Validate(msg.key.Type, msg.key.ID); err != nil || msg.key.Type != f.spec.name || !validGeneration(msg.generation) {
		return nil, actorx.ErrSetup
	}
	if old := f.entries[msg.key]; old != nil {
		if old.generation != msg.generation {
			return nil, ErrActivationUnknown
		}
		return old, nil
	}

	// Reserve before launching construction so Close also joins in-flight New calls.
	rt := f.rt
	spec := f.spec // immutable worker snapshot
	rt.mu.Lock()
	if rt.closed || f.registration.draining || !f.registration.ready {
		rt.mu.Unlock()
		return nil, actorx.ErrClosed
	}
	if len(rt.ledger) >= rt.limits.execution.Instances || rt.creating >= rt.limits.execution.Creating || rt.perType[f.spec.name] >= rt.limits.execution.CreatingPerType {
		rt.mu.Unlock()
		return nil, actorx.ErrFull
	}
	if rt.ledger[msg.generation] != nil {
		rt.mu.Unlock()
		return nil, actorx.ErrSetup
	}
	entry := &creation{registration: f.registration, key: msg.key, generation: msg.generation, done: make(chan struct{})}
	rt.ledger[msg.generation] = entry
	rt.creating++
	rt.perType[f.spec.name]++
	rt.work.Add(1)
	f.registration.work.Add(1)
	rt.mu.Unlock()
	f.entries[msg.key] = entry

	// Runtime life, never a caller's request, owns construction and startup.
	_, err := actorx.StartTask(ctx.WithContext(rt.life), func(context.Context) (actorx.Addr, error) {
		value, err := spec.construct(msg.key)
		if err != nil {
			return actorx.Addr{}, err
		}

		rt.mu.Lock()
		closed := rt.closed || f.registration.draining
		rt.mu.Unlock()
		if closed {
			return actorx.Addr{}, actorx.ErrClosed
		}

		self := newAddr(rt, msg.key, msg.generation)
		return rt.business.Spawn(&executionActor{Actor: value, self: self})
	}, func(local actorx.Addr, err error) bool {
		rt.settle(entry, local, err, false)
		return true
	}, func(local actorx.Addr) {
		rt.settle(entry, local, fmt.Errorf("%w: Factory result discarded", actorx.ErrStopped), true)
	})
	if err != nil {
		delete(f.entries, msg.key)
		rt.mu.Lock()
		delete(rt.ledger, msg.generation)
		rt.mu.Unlock()
		rt.settle(entry, actorx.Addr{}, err, false)
		return nil, err
	}
	return entry, nil
}

func (rt *Runtime) settle(entry *creation, local actorx.Addr, err error, discard bool) {
	entry.once.Do(func() {
		if discard && !local.IsZero() {
			_ = local.Stop(nil)
			_, _ = local.Wait(context.Background()) // Close may already have stopped it.
			if entry.activation == nil {
				local = actorx.Addr{}
			}
		}

		// Publish only after discarded instances have completed their local exit.
		entry.local, entry.err = local, err
		rt.mu.Lock()
		rt.creating--
		rt.perType[entry.key.Type]--
		rt.mu.Unlock()
		close(entry.done)
		rt.work.Done()
		entry.registration.work.Done()
	})
}

// createLocal is an internal integration seam, not distributed activation.
// A generation comes from the management path; local deduplication grants no
// ownership across processes. Tests supply it without a Registry.
func (rt *Runtime) createLocal(ctx context.Context, key ActorKey, generation string) (*creation, error) {
	if err := rt.checkOpen(); err != nil {
		return nil, err
	}
	factory, ok := rt.localFactory(key.Type)
	if !ok {
		return nil, ErrTypeUnavailable
	}
	entry, err := factory.Call[*creation](ctx, createRequest{key, generation})
	if err != nil {
		return nil, err
	}
	select {
	case <-entry.done:
		return entry, entry.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Local release is an integration probe. Production lifecycle uses the network
// activation ledger and terminal commits. Both require complete core exit.
type releaseRequest struct {
	key        ActorKey
	generation string
}

func (f *factory) release(ctx actorx.Context, msg releaseRequest, reply actorx.Reply[any]) error {
	entry := f.entries[msg.key]
	if entry == nil {
		return ErrNotActivated
	}
	if entry.generation != msg.generation {
		return ErrActivationUnknown
	}
	select {
	case <-entry.done:
	default:
		return actorx.ErrFull
	}
	if entry.releasing {
		return actorx.ErrFull
	}

	entry.releasing = true
	rt := f.rt
	rt.mu.Lock()
	if rt.closed || f.registration.draining || !f.registration.ready {
		rt.mu.Unlock()
		entry.releasing = false
		return actorx.ErrClosed
	}
	rt.work.Add(1)
	f.registration.work.Add(1)
	rt.mu.Unlock()

	var once sync.Once
	finish := func() { once.Do(func() { rt.work.Done(); f.registration.work.Done() }) }
	_, err := actorx.StartTask(ctx.WithContext(rt.life), func(context.Context) (actorx.Exit, error) {
		if entry.local.IsZero() {
			return actorx.Exit{Reason: entry.err}, nil
		}
		if err := entry.local.Stop(nil); err != nil && err != actorx.ErrClosed {
			return actorx.Exit{}, err
		}
		return entry.local.Wait(context.Background())
	}, func(exit actorx.Exit, err error) bool {
		defer finish()
		entry.releasing = false
		if err == nil {
			delete(f.entries, msg.key)
			rt.mu.Lock()
			delete(rt.ledger, msg.generation)
			rt.mu.Unlock()
		}
		reply.Complete(nil, err)
		return true
	}, func(actorx.Exit) { finish() })
	if err != nil {
		entry.releasing = false
		finish()
	}
	return err
}

func (rt *Runtime) releaseLocal(ctx context.Context, key ActorKey, generation string) error {
	if err := rt.checkOpen(); err != nil {
		return err
	}
	factory, ok := rt.localFactory(key.Type)
	if !ok {
		return ErrTypeUnavailable
	}
	_, err := factory.Call[any](ctx, releaseRequest{key, generation})
	return err
}

func (f *factory) stopping(actorx.Context, actorx.Stopping) error {
	f.rt.mu.Lock()
	expected := f.registration.draining || f.rt.closed
	f.rt.mu.Unlock()
	if !expected {
		f.rt.beginClose()
	} // no wait on this Factory or its workers
	return nil
}
