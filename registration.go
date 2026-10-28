package actorxdist

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/nats-io/nats.go"
)

type registration struct {
	name         string
	contract     wireContract
	descriptions []actorx.CallDescriptor
	messages     map[string]actorx.CallDescriptor
	construct    func(ActorKey) (actorx.Actor, error)
}

type bindingSnapshot []actorx.Binding

func (b bindingSnapshot) Bindings() []actorx.Binding { return b }

// describeActor validates metadata without invoking New or business lifecycle hooks.
func describeActor[T actorx.Actor](spec ActorSpec[T]) (out registration, err error) {
	defer func() {
		if v := recover(); v != nil {
			out = registration{}
			err = fmt.Errorf("%w: metadata panic: %v", actorx.ErrSetup, v)
		}
	}()

	t := reflect.TypeFor[T]()
	if spec.New == nil || t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct {
		return out, actorx.ErrSetup
	}
	if err = identity.Validate(spec.Type, "metadata"); err != nil {
		return out, err
	}

	var zero T
	descriptions, err := actorx.Describe(zero.Bindings())
	if err != nil {
		return out, err
	}
	if len(descriptions) > 64 {
		return out, actorx.ErrSetup
	}
	contract, err := buildContract(descriptions)
	if err != nil {
		return out, err
	}
	out = registration{
		name:         spec.Type,
		descriptions: descriptions,
		contract:     contract,
		messages:     map[string]actorx.CallDescriptor{},
	}
	for _, d := range descriptions {
		id, e := typeIdentity(d.MessageType())
		if e != nil {
			return registration{}, e
		}
		out.messages[id] = d
	}

	// The live binding snapshot must match metadata before core Spawn uses it.
	out.construct = func(key ActorKey) (value actorx.Actor, err error) {
		defer func() {
			if v := recover(); v != nil {
				value = nil
				err = fmt.Errorf("%w: New/Bindings panic: %v", actorx.ErrSetup, v)
			}
		}()

		live := spec.New(key)
		if reflect.ValueOf(live).IsNil() {
			return nil, actorx.ErrSetup
		}

		bindings := slices.Clone(live.Bindings())
		actual, err := actorx.Describe(bindings)
		if err != nil {
			return nil, err
		}

		// Bindings order has no contract meaning. Compare the entire set of exact
		// local types and modes; the static schema was generated at registration.
		if !sameDescriptions(actual, descriptions) {
			return nil, ErrContract
		}
		return bindingSnapshot(bindings), nil
	}
	return out, nil
}

func sameDescriptions(a, b []actorx.CallDescriptor) bool {
	if len(a) != len(b) {
		return false
	}
	for _, d := range a {
		if !slices.Contains(b, d) {
			return false
		}
	}
	return true
}

// Registration owns one local Type registration. Obtain it through Runtime.Register;
// a zero value is invalid. It must not be copied. A registration is independent
// of other Runtime registrations and of the same Type on other Runtimes.
type Registration struct {
	rt        *Runtime
	spec      registration
	token     string
	factory   actorx.Addr
	installed chan struct{}
	done      chan struct{}
	work      sync.WaitGroup
	// Runtime.mu protects admission, subscription pointers and result publication.
	ready, draining bool
	shared, control *nats.Subscription
	err             error
}

// Register publishes an immutable contract and installs this Runtime's creation
// capability. Success means the control and activation routes have been flushed;
// no business instance has been constructed. Startup and later registration use
// this same API. Duplicate local Types (including draining ones) return ErrSetup;
// exceeding ExecutionLimits.Types returns ErrFull.
//
// On an installation failure a non-nil handle may accompany the error. Cleanup
// starts automatically; Unregister can wait for it. The immutable contract may
// remain published. Runtime.Close always joins this cleanup.
func (rt *Runtime) Register[T actorx.Actor](ctx context.Context, spec ActorSpec[T]) (*Registration, error) {
	if ctx == nil {
		return nil, actorx.ErrSetup
	}
	if err := rt.checkOpen(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	declaration, err := describeActor(spec)
	if err != nil {
		return nil, err
	}
	token, err := identity.NewOpaque()
	if err != nil {
		return nil, err
	}
	r := &Registration{rt: rt, spec: declaration, token: token, installed: make(chan struct{}), done: make(chan struct{})}
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return nil, actorx.ErrClosed
	}
	if rt.registrations[spec.Type] != nil {
		rt.mu.Unlock()
		return nil, fmt.Errorf("%w: duplicate Type %q", actorx.ErrSetup, spec.Type)
	}
	if rt.limits.execution.Types > 0 && len(rt.registrations) >= rt.limits.execution.Types {
		rt.mu.Unlock()
		return nil, actorx.ErrFull
	}
	rt.registrations[spec.Type] = r
	rt.work.Add(1)
	rt.mu.Unlock()
	defer rt.work.Done()
	ctx, cancel := requestBudget(ctx)
	defer cancel()
	// Closing the Runtime also interrupts installation I/O.
	stop := context.AfterFunc(rt.life, cancel)
	defer stop()
	if rt.net != nil {
		err = rt.net.registry.publishType(ctx, spec.Type, declaration.contract)
	}
	if err == nil {
		rt.mu.Lock()
		if rt.closed || r.draining {
			err = actorx.ErrClosed
		}
		rt.mu.Unlock()
	}
	if err == nil {
		// Management stays alive until installation/cleanup work joins on Close.
		// Spawn may execute lifecycle hooks; do not hold the Runtime mutex across it.
		factory, spawnErr := rt.management.Spawn(&factory{rt: rt, spec: declaration, registration: r, entries: map[ActorKey]*creation{}})
		rt.mu.Lock()
		r.factory, err = factory, spawnErr
		if err == nil {
			rt.specs[spec.Type] = declaration
			rt.factories[spec.Type] = factory
		}
		rt.mu.Unlock()
	}

	if err == nil && rt.net != nil {
		err = rt.net.installRegistration(ctx, r)
	}
	rt.mu.Lock()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && (rt.closed || r.draining) {
		err = actorx.ErrClosed
	}
	if err == nil {
		r.ready = true
	}
	close(r.installed)
	rt.mu.Unlock()
	if err != nil {
		r.beginUnregister()
		return r, err
	}
	// Unexpected Factory loss retains the existing Runtime-wide fail-closed rule.
	go func() {
		_, _ = r.factory.Wait(context.Background())
		rt.mu.Lock()
		unexpected := !r.draining && !rt.closed
		rt.mu.Unlock()
		if unexpected {
			rt.beginClose()
		}
	}()
	return r, nil
}

// Unregister synchronously closes this registration's new-activation gate, then
// stops its instances and waits for construction, actual exit and required
// terminal commits before removing its control route and releasing Type capacity.
// It is idempotent. A timeout ends only this wait; cleanup continues and can be
// joined again. A stale handle cannot withdraw a subsequent registration.
func (r *Registration) Unregister(ctx context.Context) error {
	if r == nil || r.rt == nil || ctx == nil {
		return actorx.ErrSetup
	}
	r.beginUnregister()
	select {
	case <-r.done:
		return r.err
	default:
	}
	select {
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return fmt.Errorf("dist: Unregister: %w; cleanup continues", ctx.Err())
	}
}

func (r *Registration) beginUnregister() {
	rt := r.rt
	rt.mu.Lock()
	if r.draining {
		rt.mu.Unlock()
		return
	}
	r.draining = true
	r.ready = false
	rt.work.Add(1)
	rt.mu.Unlock()
	go r.cleanup()
}

func (r *Registration) cleanup() {
	defer r.rt.work.Done()
	<-r.installed
	rt := r.rt
	rt.mu.Lock()
	shared := r.shared
	entries := make([]*creation, 0)
	for _, e := range rt.ledger {
		if e.registration == r {
			entries = append(entries, e)
			if e.activation != nil {
				e.activation.requestStop()
			}
		}
	}
	rt.mu.Unlock()
	var errs []error
	if shared != nil {
		if err := shared.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrBadSubscription) {
			errs = append(errs, err)
		}
	}
	// Local-only probes have no terminal monitor; production entries are joined by
	// registration.work below, including unknown or failed-held terminal evidence.
	for _, e := range entries {
		if e.activation != nil {
			continue
		}
		<-e.done
		if !e.local.IsZero() {
			_ = e.local.Stop(nil)
			_, err := e.local.Wait(context.Background())
			errs = append(errs, err)
		}
		rt.mu.Lock()
		delete(rt.ledger, e.generation)
		rt.mu.Unlock()
	}
	r.work.Wait()
	if !r.factory.IsZero() {
		_ = r.factory.Stop(nil)
		_, err := r.factory.Wait(context.Background())
		errs = append(errs, err)
	}
	if r.control != nil {
		if err := r.control.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrBadSubscription) {
			errs = append(errs, err)
		}
	}
	rt.mu.Lock()
	if rt.registrations[r.spec.name] == r {
		delete(rt.registrations, r.spec.name)
		delete(rt.factories, r.spec.name)
		delete(rt.specs, r.spec.name)
		delete(rt.perType, r.spec.name)
	}
	r.err = errors.Join(errs...)
	close(r.done)
	rt.mu.Unlock()
}

func (rt *Runtime) registrationToken(typ string) string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if r := rt.registrations[typ]; r != nil {
		return r.token
	}
	return ""
}

func (rt *Runtime) localFactory(typ string) (actorx.Addr, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	r := rt.registrations[typ]
	if r == nil || !r.ready || r.draining {
		return actorx.Addr{}, false
	}
	return r.factory, true
}
