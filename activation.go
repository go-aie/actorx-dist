package actorxdist

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/errwire"
	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/nats-io/nats.go"
)

// The ledger owns this work before any CAS. selected and creation.done publish
// separate immutable results: selection does not imply a started business Actor.
type activationWork struct {
	selected chan struct{}
	owner    owned
	err      error
	stop     chan struct{}
	stopOnce sync.Once
	cause    error // published by stop channel closure
	released chan struct{}
	sub      *nats.Subscription // written before creation.done, removed by its one monitor
	ready    atomic.Bool
	claimed  bool
	evidence string
}

func (w *activationWork) requestStop() { w.requestStopCause(nil) }

func (w *activationWork) requestStopCause(cause error) {
	w.stopOnce.Do(func() {
		w.cause = cause
		close(w.stop)
	})
}

func (w *activationWork) stopCause() error {
	if w.stopping() {
		return w.cause
	}
	return nil
}

func (w *activationWork) stopping() bool {
	select {
	case <-w.stop:
		return true
	default:
		return false
	}
}

type activateRequest struct{ key ActorKey }

func (f *factory) activate(ctx actorx.Context, msg activateRequest) (*creation, error) {
	if identity.Validate(msg.key.Type, msg.key.ID) != nil || msg.key.Type != f.spec.name {
		return nil, actorx.ErrSetup
	}

	// Reap completed monitors before applying the per-Runtime generation budget.
	for key, old := range f.entries {
		if old.activation != nil {
			select {
			case <-old.activation.released:
				delete(f.entries, key)
			default:
			}
		}
	}
	if old := f.entries[msg.key]; old != nil {
		if old.activation == nil {
			return nil, ErrActivationUnknown
		}
		select {
		case <-old.activation.released:
			delete(f.entries, msg.key)
		default:
			return old, nil
		}
	}

	rt := f.rt
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed || f.registration.draining || !f.registration.ready {
		return nil, actorx.ErrClosed
	}
	var limit string
	switch {
	case len(rt.ledger) >= rt.limits.execution.Instances:
		limit = "Instances"
	case rt.creating >= rt.limits.execution.Creating:
		limit = "Creating"
	case rt.perType[f.spec.name] >= rt.limits.execution.CreatingPerType:
		limit = "CreatingPerType"
	}
	if limit != "" {
		err := fmt.Errorf("%w: activation %s limit", actorx.ErrFull, limit)
		rt.net.report("activate-rejected", err)
		return nil, err
	}
	gen, err := identity.NewOpaque()
	if err != nil {
		return nil, err
	}
	e := &creation{
		key:          msg.key,
		registration: f.registration,
		generation:   gen,
		done:         make(chan struct{}),
		activation:   &activationWork{selected: make(chan struct{}), stop: make(chan struct{}), released: make(chan struct{})},
	}
	rt.ledger[gen] = e
	rt.creating++
	rt.perType[f.spec.name]++
	rt.work.Add(1)
	f.registration.work.Add(1)
	spec := f.spec

	_, err = actorx.StartTask(ctx.WithContext(rt.life), func(context.Context) (actorx.Addr, error) {
		return rt.net.startActivation(e, spec)
	}, func(local actorx.Addr, err error) bool {
		rt.settle(e, local, err, false)
		return true
	}, func(local actorx.Addr) {
		rt.settle(e, local, actorx.ErrStopped, true)
	})
	if err != nil {
		delete(rt.ledger, gen)
		rt.creating--
		rt.perType[f.spec.name]--
		rt.work.Done()
		f.registration.work.Done()
		return nil, err
	}
	f.entries[msg.key] = e

	// This bounded ledger monitor also survives a dead Factory completion mailbox.
	rt.work.Add(1)
	f.registration.work.Add(1)
	go rt.net.monitorActivation(e)
	return e, nil
}

func managementContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

func newOperation() string {
	s, err := identity.NewOpaque()
	if err != nil {
		panic(err)
	}
	return s
}

// A Get is only a candidate. Every successful selection passes through the
// owner's dedicated control route, or the creator's conditional write.
func (n *network) lookup(ctx context.Context, key ActorKey) (owned, error) {
	o, err := n.registry.owner(ctx, key)
	if errors.Is(err, nats.ErrKeyNotFound) || err == nil && o.value.State == "INACTIVE" {
		return owned{}, ErrNotActivated
	}
	if err != nil {
		return owned{}, errors.Join(ErrActivationUnknown, err)
	}
	return n.confirmOwner(ctx, o)
}

func (n *network) confirmOwner(ctx context.Context, o owned) (owned, error) {
	if o.value.Factory == n.rt.registrationToken(o.value.Key.Type) {
		return n.confirmCurrent(ctx, o.value.Key, o.value.Generation, o.value.Factory)
	}
	return n.factoryRequest(ctx, "confirm", o.value.Key, o.value.Generation, o.value.Factory, o.value.Fingerprint, nil)
}

func (n *network) confirmCurrent(ctx context.Context, key ActorKey, gen, factory string) (owned, error) {
	if n.rt.registrationToken(key.Type) != factory {
		return owned{}, ErrActivationUnknown
	}
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		var o owned
		o, err = n.registry.owner(ctx, key)
		if err != nil {
			return owned{}, errors.Join(ErrActivationUnknown, err)
		}
		if o.value.Generation != gen || o.value.Factory != factory || o.value.State == "INACTIVE" {
			return owned{}, ErrActivationUnknown
		}
		o, err = n.registry.confirm(ctx, o, factory)
		if err == nil {
			return o, nil
		}
		if !pause(ctx, time.Duration(20<<attempt)*time.Millisecond) {
			break
		}
	}
	return owned{}, errors.Join(ErrActivationUnknown, err, ctx.Err())
}

func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (n *network) choose(e *creation, spec registration) (owned, error) {
	w := e.activation
	candidate := ownership{
		Version:     1,
		Operation:   newOperation(),
		Key:         e.key,
		Generation:  e.generation,
		Factory:     e.registration.token,
		Fingerprint: spec.contract.fingerprint,
		State:       "STARTING",
	}

	// Rechecks after a lost write always use this same generation/operation.
	attempted := false
	for {
		ctx, cancel := managementContext()
		o, err := n.registry.owner(ctx, e.key)
		if err == nil && o.value.State != "INACTIVE" {
			if o.value.Fingerprint != spec.contract.fingerprint {
				cancel()
				return owned{}, ErrContract
			}
			if sameGeneration(o.value, candidate) {
				w.claimed = true
				o, err = n.confirmCurrent(ctx, e.key, e.generation, candidate.Factory)
			} else {
				o, err = n.confirmOwner(ctx, o)
			}
			cancel()
			if err == nil {
				return o, nil
			}
			if !w.claimed {
				return owned{}, err
			}
		} else if err == nil || errors.Is(err, nats.ErrKeyNotFound) {
			if w.stopping() || n.rt.checkOpen() != nil {
				cancel()
				return owned{}, actorx.ErrClosed
			}
			var previous *owned
			if err == nil {
				previous = &o
			}
			attempted = true
			o, err = n.registry.claim(ctx, candidate, previous)
			if err == nil {
				w.claimed = true
				o, err = n.confirmCurrent(ctx, e.key, e.generation, candidate.Factory)
				if err == nil {
					cancel()
					return o, nil
				}
			} else if !errors.Is(err, ErrActivationUnknown) {
				cancel()
				return owned{}, err
			}
			cancel()
		} else {
			cancel()
			if !w.claimed && !attempted {
				return owned{}, errors.Join(ErrActivationUnknown, err)
			}
		}

		// The ledger, not a request waiter, retains uncertain ownership.
		n.report("activate-reconcile", err)
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *network) startActivation(e *creation, spec registration) (local actorx.Addr, err error) {
	w := e.activation
	o, err := n.choose(e, spec)
	w.owner, w.err = o, err
	close(w.selected)
	if err != nil || !w.claimed {
		return actorx.Addr{}, err
	}

	// One owned timer requests cancellation; application callbacks cannot be killed.
	timer := time.AfterFunc(30*time.Second, w.requestStop)
	defer timer.Stop()
	if w.stopping() || n.rt.checkOpen() != nil {
		return local, actorx.ErrStopped
	}
	value, err := spec.construct(e.key)
	w.evidence = "no_instance"
	if err != nil {
		return local, err
	}
	if w.stopping() || n.rt.checkOpen() != nil {
		return local, actorx.ErrStopped
	}
	self := newAddr(n.rt, e.key, e.generation)
	local, err = n.rt.business.Spawn(&executionActor{Actor: value, self: self})
	w.evidence = "spawn_rollback"
	if err != nil {
		return local, err
	}
	w.evidence = "local_wait"
	if w.stopping() || n.rt.checkOpen() != nil {
		_ = local.Stop(w.stopCause())
		return local, actorx.ErrStopped
	}

	// Export only the public local target identity; no core execution state escapes.
	raw, exportErr := actorx.ExportAddr(local)
	if exportErr != nil {
		return local, exportErr
	}
	var target struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(raw, &target) != nil {
		return local, actorx.ErrSetup
	}
	receiver := &businessTarget{key: e.key, generation: e.generation, coreID: target.ID, spec: spec, work: w}
	w.sub, err = n.nc.Subscribe(n.actorSubject(e.key), n.businessCallback(receiver))
	if err == nil {
		err = w.sub.SetPendingLimits(2, 256<<10)
	}
	if err == nil {
		ctx, cancel := managementContext()
		err = n.nc.FlushWithContext(ctx)
		cancel()
	}
	if err != nil {
		_ = local.Stop(err)
		return local, err
	}
	for {
		if w.stopping() || n.rt.checkOpen() != nil {
			_ = local.Stop(w.stopCause())
			return local, actorx.ErrStopped
		}
		ctx, cancel := managementContext()
		_, err = n.transition(ctx, e, "READY")
		cancel()
		if err == nil {
			w.ready.Store(true)
			return local, nil
		}

		// A lost READY acknowledgement never authorizes another New or drops local.
		n.report("ready-reconcile", err)
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *network) actorSubject(key ActorKey) string {
	return n.rt.names.ActorSubject(key.Type, key.ID)
}

func (n *network) transition(ctx context.Context, e *creation, state string) (owned, error) {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		var o owned
		o, err = n.registry.owner(ctx, e.key)
		if err != nil {
			return owned{}, errors.Join(ErrActivationUnknown, err)
		}
		if o.value.Generation != e.generation || o.value.Factory != e.registration.token {
			return owned{}, ErrActivationUnknown
		}
		if o.value.State == state {
			return o, nil
		}
		if state == "STOPPING" && o.value.State == "FAILED" {
			return o, nil // the monitor releases FAILED without changing its evidence
		}
		next := o.value
		next.State = state
		next.Operation = newOperation()
		o, err = n.registry.change(ctx, o, next)
		if err == nil {
			return o, nil
		}
		if !pause(ctx, time.Duration(20<<attempt)*time.Millisecond) {
			break
		}
	}
	return owned{}, errors.Join(ErrActivationUnknown, err)
}

// One monitor per ledger entry owns full cleanup and terminal receipts. No retry
// spawns additional workers. FAILED retains its generation budget until Stop/Close.
func (n *network) monitorActivation(e *creation) {
	defer n.rt.work.Done()
	defer e.registration.work.Done()
	w := e.activation
	<-e.done
	defer n.finishActivation(e)
	if !w.claimed {
		return
	}
	var exit actorx.Exit
	if !e.local.IsZero() {
		// Local Wait is the sole execution completion authority, including startup
		// races and spontaneous exit. Close already stops the business engine.
		wait := make(chan actorx.Exit, 1)
		go func() {
			result, _ := e.local.Wait(context.Background())
			wait <- result
		}()
		select {
		case exit = <-wait:
		case <-w.stop:
			_ = e.local.Stop(w.stopCause())
			exit = <-wait
		case <-n.rt.life.Done():
			w.requestStop()
			_ = e.local.Stop(w.stopCause())
			exit = <-wait
		}
	} else {
		exit.Reason = e.err
		if errors.Is(e.err, actorx.ErrStopped) && w.stopCause() != nil {
			exit.Reason = w.stopCause()
		}
	}
	if w.sub != nil {
		_ = w.sub.Unsubscribe()
	}

	// Failed Spawn has already rolled back. A successful local Addr must finish
	// Wait before any terminal evidence is written.
	failed := e.err != nil && !w.stopping() && n.rt.checkOpen() == nil
	intended := "RELEASED"
	if failed {
		intended = "FAILED_HELD"
	}
	cause, _ := errwire.Encode(exit.Reason, errwire.Source{Operation: "activate", Type: e.key.Type, ID: e.key.ID, Generation: e.generation})
	evidence := w.evidence
	if evidence == "" {
		evidence = "no_instance"
	}
	p := terminal{
		Version:         1,
		Operation:       newOperation(),
		Key:             e.key,
		Generation:      e.generation,
		Factory:         e.registration.token,
		Cause:           cause,
		CreatedInstance: !e.local.IsZero(),
		Evidence:        evidence,
		Intended:        intended,
		Status:          "PREPARED",
	}
	p.Digest = cleanupDigest(p)

	// Persist cleanup evidence before changing ownership or releasing its budget.
	var prepared terminalEntry
	n.reconcile("prepare", func(ctx context.Context) error {
		var err error
		prepared, err = n.registry.prepare(ctx, p)
		return err
	})

	if failed {
		n.reconcile("failed", func(ctx context.Context) error {
			o, err := n.registry.owner(ctx, e.key)
			if err != nil {
				return err
			}
			if o.value.State == "STOPPING" {
				failed = false
				return nil
			}
			if o.value.State == "FAILED" && matchesPrepared(o, prepared) && o.value.Digest == prepared.value.Digest {
				return nil
			}
			_, err = n.registry.fail(ctx, o, prepared, newOperation())
			return err
		})
	}

	if failed {
		heldOperation := newOperation()
		n.reconcile("failed-held", func(ctx context.Context) error {
			existing, readErr := n.registry.exit(ctx, e.key, e.generation)
			if readErr == nil &&
				sameTerminal(existing.value, prepared.value) &&
				existing.value.Status == "COMMITTED" &&
				existing.value.Disposition == "FAILED_HELD" {
				prepared = existing
				return nil
			}
			if prepared.value.Status == "COMMITTED" {
				return nil
			}
			o, err := n.registry.owner(ctx, e.key)
			if err != nil {
				return err
			}
			committed, err := n.registry.commitHeld(ctx, prepared, o, heldOperation)
			if err == nil {
				prepared = committed
			}
			return err
		})
		select {
		case <-w.stop:
		case <-n.rt.life.Done():
			w.requestStop()
		}
	}

	// Retain both operation IDs and the acknowledged receipt across retries.
	var receipt releaseReceipt
	releaseOperation, commitOperation := newOperation(), newOperation()
	n.reconcile("release", func(ctx context.Context) error {
		o, err := n.registry.owner(ctx, e.key)
		if err != nil {
			return err
		}
		if o.value.State == "INACTIVE" {
			receipt, err = n.registry.recoverRelease(ctx, prepared, releaseOperation)
			return err
		}
		if o.value.State != "FAILED" {
			o, err = n.transition(ctx, e, "STOPPING")
			if err != nil {
				return err
			}
		}
		receipt, err = n.registry.release(ctx, o, prepared, releaseOperation)
		return err
	})
	n.reconcile("commit-release", func(ctx context.Context) error {
		_, err := n.registry.commitReleased(ctx, receipt, commitOperation)
		return err
	})
}

// reconcile retains cleanup work until durable confirmation, including during
// Close. A request deadline cannot release an uncertain generation's resources.
func (n *network) reconcile(op string, work func(context.Context) error) {
	delay := time.Second
	for {
		ctx, cancel := managementContext()
		err := work(ctx)
		cancel()
		if err == nil {
			return
		}
		n.report(op, err)
		time.Sleep(delay)
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}

func (n *network) stopActivations() {
	n.rt.mu.Lock()
	defer n.rt.mu.Unlock()
	for _, e := range n.rt.ledger {
		if e.activation != nil {
			e.activation.requestStop()
		}
	}
}

type stopActivationRequest struct {
	key                 ActorKey
	generation, factory string
	cause               error
}

func (f *factory) stopActivation(ctx actorx.Context, msg stopActivationRequest, reply actorx.Reply[any]) error {
	e := f.entries[msg.key]
	if e == nil || e.generation != msg.generation || e.activation == nil || msg.factory != f.registration.token {
		return ErrActivationUnknown
	}
	_, err := actorx.StartTask(ctx, func(taskCtx context.Context) (any, error) {
		o, err := f.rt.net.confirmCurrent(taskCtx, msg.key, msg.generation, msg.factory)
		if err != nil {
			return nil, err
		}
		e.activation.requestStopCause(msg.cause)

		// Stop ACK also fences requests that already passed the network gate.
		select {
		case <-e.done:
			if !e.local.IsZero() {
				_ = e.local.Stop(e.activation.stopCause())
			}
		default:
		}
		if o.value.State == "FAILED" {
			select {
			case <-e.activation.released:
				return nil, nil
			case <-taskCtx.Done():
				return nil, errors.Join(ErrActivationUnknown, taskCtx.Err())
			}
		}

		// The monitor alone publishes FAILED; it may have advanced since confirm.
		// It observes stop and releases that failure without rewriting its cause.
		stopped, err := f.rt.net.transition(taskCtx, e, "STOPPING")
		if err == nil && stopped.value.State == "FAILED" {
			select {
			case <-e.activation.released:
				return nil, nil
			case <-taskCtx.Done():
				return nil, errors.Join(ErrActivationUnknown, taskCtx.Err())
			}
		}
		if err != nil {
			p, readErr := f.rt.net.registry.exit(taskCtx, msg.key, msg.generation)
			if readErr == nil && p.value.Status == "COMMITTED" && p.value.Disposition == "RELEASED" {
				return nil, nil
			}
		}
		return nil, err
	}, func(_ any, err error) bool {
		reply.Complete(nil, err)
		return true
	}, nil)
	return err
}

// resolution shares one Runtime-owned selection attempt among Addr copies.
type resolution struct {
	done  chan struct{}
	owner owned
	err   error
}

// resolve confirms and fixes a generation for public business invocation.
func (addr Addr) resolve(ctx context.Context) (owned, error) {
	if addr.IsZero() || ctx == nil {
		return owned{}, actorx.ErrSetup
	}
	if err := ctx.Err(); err != nil {
		return owned{}, err
	}
	if err := addr.rt.checkOpen(); err != nil {
		return owned{}, err
	}
	if addr.rt.net == nil {
		return owned{}, actorx.ErrSetup
	}
	ctx, cancel := requestBudget(ctx)
	defer cancel()

	s := addr.state
	s.mu.Lock()
	if s.generation != "" {
		gen := s.generation
		s.mu.Unlock()
		o, err := addr.rt.net.registry.owner(ctx, addr.key)
		if err != nil {
			if errors.Is(err, nats.ErrKeyNotFound) {
				return owned{}, addr.retired(ctx, gen)
			}
			return owned{}, errors.Join(ErrOwnerUnavailable, err)
		}
		if o.value.Generation != gen || o.value.State == "INACTIVE" {
			return owned{}, addr.retired(ctx, gen)
		}
		return addr.rt.net.confirmOwner(ctx, o)
	}

	// The Addr lock is taken before the Runtime lock, as in Wait's observer budget.
	addr.rt.mu.Lock()
	if addr.rt.closed {
		addr.rt.mu.Unlock()
		s.mu.Unlock()
		return owned{}, actorx.ErrClosed
	}
	if s.waiters >= 16 || addr.rt.resolvers >= addr.rt.limits.waiters() || s.resolving == nil && addr.rt.resolutionJobs >= addr.rt.limits.transport.Requests {
		addr.rt.mu.Unlock()
		s.mu.Unlock()
		return owned{}, actorx.ErrFull
	}
	addr.rt.resolvers++
	s.waiters++
	if s.resolving == nil {
		// Caller cancellation releases a waiter; Runtime life owns the shared attempt.
		work := &resolution{done: make(chan struct{})}
		s.resolving = work
		addr.rt.work.Add(1)
		addr.rt.resolutionJobs++
		go func() {
			defer addr.rt.work.Done()
			defer func() {
				addr.rt.mu.Lock()
				addr.rt.resolutionJobs--
				addr.rt.mu.Unlock()
			}()
			attempt, stop := context.WithTimeout(addr.rt.life, 5*time.Second)
			defer stop()
			o, err := addr.rt.net.lookup(attempt, addr.key)
			if errors.Is(err, ErrNotActivated) {
				entry, readErr := addr.rt.net.registry.get(attempt, addr.rt.net.registry.names.TypesBucket, "t."+identity.Token(addr.key.Type), false)
				if readErr != nil {
					err = errors.Join(ErrTypeUnavailable, readErr)
				} else {
					sum := sha256.Sum256(entry.Value())
					o, err = addr.rt.net.factoryRequest(attempt, "activate", addr.key, "", "", hex.EncodeToString(sum[:]), json.RawMessage(`{"create":true}`))
				}
			}
			s.mu.Lock()
			work.owner, work.err = o, err
			if err == nil {
				if s.generation != "" && s.generation != o.value.Generation {
					work.err = ErrActivationUnknown
				} else {
					s.generation = o.value.Generation
				}
			}
			s.resolving = nil
			close(work.done)
			s.mu.Unlock()
		}()
	}

	work := s.resolving
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

	select {
	case <-work.done:
		return work.owner, work.err
	case <-ctx.Done():
		return owned{}, errors.Join(ErrActivationUnknown, ctx.Err())
	}
}

// awaitReady separates a confirmed selection from startup completion. A failed
// activation keeps the original generation and returns its committed cause.
func (addr Addr) awaitReady(ctx context.Context) (owned, error) {
	if ctx == nil {
		return owned{}, actorx.ErrSetup
	}
	ctx, cancel := requestBudget(ctx)
	defer cancel()
	selected, err := addr.resolve(ctx)
	if err != nil {
		return owned{}, err
	}
	for {
		o, err := addr.rt.net.registry.owner(ctx, addr.key)
		if err != nil {
			return owned{}, errors.Join(ErrActivationUnknown, err)
		}
		if o.value.Generation != selected.value.Generation || o.value.State == "INACTIVE" || o.value.State == "STOPPING" {
			return owned{}, actorx.ErrStopped
		}
		switch o.value.State {
		case "READY":
			return o, nil
		case "FAILED":
			p, readErr := addr.rt.net.registry.exit(ctx, addr.key, o.value.Generation)
			if readErr == nil && p.value.Status == "COMMITTED" {
				reason, decodeErr := errwire.Decode(p.value.Cause)
				if decodeErr != nil {
					return owned{}, decodeErr
				}
				return o, reason
			}
		}
		if !pause(ctx, 20*time.Millisecond) {
			return owned{}, errors.Join(ErrActivationUnknown, ctx.Err())
		}
	}
}

// Replacement or missing current metadata alone cannot prove the original Exit.
func (addr Addr) retired(ctx context.Context, generation string) error {
	p, err := addr.rt.net.registry.exit(ctx, addr.key, generation)
	if err != nil {
		return err
	}
	if p.value.Status == "COMMITTED" && p.value.Disposition == "RELEASED" {
		return actorx.ErrStopped
	}
	return ErrExitUnknown
}

type forgetActivation struct {
	key        ActorKey
	generation string
}

func (f *factory) forget(_ actorx.Context, msg forgetActivation) error {
	if e := f.entries[msg.key]; e != nil && e.generation == msg.generation {
		delete(f.entries, msg.key)
	}
	return nil
}

func (f *factory) stopped(actorx.Context, actorx.Stopped) error {
	f.entries = nil
	return nil
}

func (n *network) finishActivation(e *creation) {
	n.rt.mu.Lock()
	delete(n.rt.ledger, e.generation)
	n.rt.mu.Unlock()
	close(e.activation.released)

	// Serial removal drops the last Factory reference to the local execution Addr.
	// If Factory has died, its Stopped hook drops the whole map instead.
	for {
		ctx, cancel := managementContext()
		err := e.registration.factory.Send(ctx, forgetActivation{e.key, e.generation})
		cancel()
		if err == nil || errors.Is(err, actorx.ErrStopped) || errors.Is(err, actorx.ErrClosed) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// requestBudget supplies a default deadline and caps explicit deadlines without
// extending an existing budget when nested calls apply it again.
func requestBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	limit := time.Now().Add(5 * time.Second)
	if requested, ok := ctx.Deadline(); ok {
		limit = time.Now().Add(60 * time.Second)
		if requested.Before(limit) {
			limit = requested
		}
	}
	return context.WithDeadline(ctx, limit)
}
