package actorxdist

import (
	"context"
	"errors"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
)

type discoverRequest struct {
	ctx   context.Context // plain operation context, never an actorx.Context
	query DiscoveryQuery
}

type discoveryCollect struct{ queryID string }

type snapshotRequest struct {
	ctx context.Context
}

type discoveryNodeReply struct {
	queryID  string
	snapshot discoverySnapshot
	err      error
}

type discoveryActor struct {
	rt        *Runtime
	snapshot  func(context.Context) (discoverySnapshot, error)
	transport discoveryTransporter // optional internal test seam; otherwise rt.net

	// queries holds unfinished requests. querySlots also counts finished requests
	// until their Task completion is consumed; cancellation alone frees no slot.
	queries    map[string]*discoveryCollection
	querySlots int
}

type discoveryTransporter interface {
	observeDiscovery() (discoveryTransport, error)
	checkDiscovery(discoveryTransport) error
	publishDiscovery(context.Context, discoveryQueryFrame, discoveryTransport) error
}

type discoveryCollection struct {
	id    string
	ctx   context.Context
	reply actorx.Reply[DiscoveryReport]

	builder *discoveryBuilder

	transport discoveryTransporter
	state     discoveryTransport
	window    time.Duration
	deadline  time.Time // zero while preparing; monotonic during collection

	cancel      context.CancelFunc
	taskPending bool // worker or queued completion still owns the query slot
	finished    bool
}

func (d *discoveryActor) Bindings() []actorx.Binding {
	return []actorx.Binding{
		actorx.HandleDeferred(d.discover),
		actorx.HandleDeferred(d.nodeSnapshot),
		actorx.HandleMessage(d.nodeReply),
		actorx.HandleMessage(d.collect),
		actorx.HandleMessage(d.stopping),
	}
}

func (d *discoveryActor) nodeSnapshot(ctx actorx.Context, msg snapshotRequest, reply actorx.Reply[discoverySnapshot]) error {
	rt, snapshot := d.rt, d.snapshot
	_, err := actorx.StartTask(ctx, snapshot, func(value discoverySnapshot, err error) bool {
		if stopped := rt.discoveryStatus(msg.ctx); stopped != nil {
			err = stopped
		}
		if err != nil {
			value = discoverySnapshot{}
		}
		reply.Complete(value, err)
		return true
	}, nil)
	return err
}

func (d *discoveryActor) nodeReply(_ actorx.Context, msg discoveryNodeReply) error {
	q := d.queries[msg.queryID]
	if q == nil || q.deadline.IsZero() {
		return nil
	}
	if !time.Now().Before(q.deadline) {
		// The reserved Task completion decides the outcome, including publication
		// failures. A late node must neither merge nor race that outcome to success.
		return nil
	}
	if msg.err != nil {
		d.finish(q, "", msg.err)
		return nil
	}
	if msg.snapshot.RuntimeID == d.rt.runtimeID {
		return nil // The accepted local snapshot wins over our own broadcast.
	}

	accepted, err := q.builder.merge(msg.snapshot)
	if err != nil {
		d.finish(q, "", err)
	} else if !accepted {
		d.finish(q, DiscoveryEndLimit, nil)
	}

	return nil
}

func (d *discoveryActor) discover(ctx actorx.Context, msg discoverRequest, reply actorx.Reply[DiscoveryReport]) error {
	if msg.query.Scope == DiscoveryCluster {
		return d.cluster(ctx, msg, reply)
	}
	rt, snapshot := d.rt, d.snapshot // immutable worker dependencies
	_, err := actorx.StartTask(ctx, snapshot, func(value discoverySnapshot, err error) bool {
		var report DiscoveryReport
		if err == nil {
			report = value.localReport()
		}

		// Cancellation and Runtime closure take precedence even if a successful
		// snapshot completion was already queued in the mailbox.
		if stopped := rt.discoveryStatus(msg.ctx); stopped != nil {
			err = stopped
		}
		if err != nil {
			report = DiscoveryReport{}
		}
		reply.Complete(report, err)
		return true
	}, nil) // snapshots contain plain values; core joins/discards tasks on shutdown
	return err
}

func (d *discoveryActor) cluster(ctx actorx.Context, msg discoverRequest, reply actorx.Reply[DiscoveryReport]) error {
	if err := d.rt.discoveryStatus(msg.ctx); err != nil {
		return err
	}
	if d.querySlots >= maxDiscoveryQueries {
		return actorx.ErrFull
	}

	transport := d.transport
	if transport == nil {
		if d.rt.net == nil {
			return ErrOwnerUnavailable
		}
		transport = d.rt.net
	}

	state, err := transport.observeDiscovery()
	if err != nil {
		return err
	}

	id, err := identity.NewOpaque()
	if err != nil {
		return err
	}

	// Preparing reserves a query slot before starting the local scan. The slot
	// remains owned until the query finishes and its Task completion is consumed.
	q := &discoveryCollection{
		id:        id,
		ctx:       msg.ctx,
		reply:     reply,
		builder:   newDiscoveryBuilder(),
		transport: transport,
		state:     state,
		window:    msg.query.Window,
	}
	if d.queries == nil {
		d.queries = make(map[string]*discoveryCollection)
	}
	d.queries[id] = q
	d.querySlots++
	q.taskPending = true

	snapshot := d.snapshot
	q.cancel, err = actorx.StartTask(ctx, snapshot, func(value discoverySnapshot, err error) bool {
		if !d.taskCompleted(q) {
			return true
		}
		if err == nil {
			_, err = q.builder.merge(value)
		}
		if err != nil {
			d.finish(q, "", err)
			return true
		}
		if err := d.status(q); err != nil {
			d.finish(q, "", err)
			return true
		}

		// StartTask completions have no fresh actorx.Context. A bounded phase
		// message obtains one without retaining the calling handler's context.
		if err := d.rt.discovery.Send(d.rt.life, discoveryCollect{id}); err != nil {
			d.finish(q, "", err)
		}
		return true
	}, nil)
	if err != nil {
		q.taskPending = false
		d.finish(q, "", err)
	}

	return nil // finish completes even a StartTask admission failure exactly once
}

func (d *discoveryActor) collect(ctx actorx.Context, msg discoveryCollect) error {
	q := d.queries[msg.queryID]
	if q == nil || !q.deadline.IsZero() {
		return nil
	}
	if err := d.status(q); err != nil {
		d.finish(q, "", err)
		return nil
	}

	window := q.window
	if window == 0 {
		window = 200 * time.Millisecond
	}

	// Collecting begins after the local scan, before publication can produce a
	// reply. Round the wire deadline up; the Actor keeps the monotonic deadline.
	q.deadline = time.Now().Add(window)
	frame := discoveryQueryFrame{1, q.id, d.rt.runtimeID, q.deadline.Add(time.Millisecond - 1).UnixMilli()}
	deadline, state, transport := q.deadline, q.state, q.transport

	q.taskPending = true
	var err error
	q.cancel, err = actorx.StartTask(ctx.WithContext(q.ctx), func(ctx context.Context) (struct{}, error) {
		if err := transport.publishDiscovery(ctx, frame, state); err != nil {
			// Scheduling can consume a sub-millisecond window before publication.
			// Its expiry is still a successful observation; parent expiry is not.
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil && !time.Now().Before(deadline) {
				return struct{}{}, nil
			}
			return struct{}{}, err
		}

		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return struct{}{}, ctx.Err()
		case <-state.lost:
			return struct{}{}, ErrOwnerUnavailable
		case <-timer.C:
			return struct{}{}, nil
		}
	}, func(_ struct{}, err error) bool {
		if d.taskCompleted(q) {
			d.finish(q, DiscoveryEndWindow, err)
		}
		return true
	}, nil)
	if err != nil {
		q.taskPending = false
		d.finish(q, "", err)
	}

	return nil
}

// taskCompleted consumes a Task completion in the Actor and reports whether
// the query is still active. An already finished query releases its slot here.
func (d *discoveryActor) taskCompleted(q *discoveryCollection) bool {
	q.taskPending = false
	if q.finished {
		d.querySlots--
		return false
	}

	return true
}

func (d *discoveryActor) status(q *discoveryCollection) error {
	if err := d.rt.discoveryStatus(q.ctx); err != nil {
		return err
	}

	return q.transport.checkDiscovery(q.state)
}

func (d *discoveryActor) finish(q *discoveryCollection, reason DiscoveryEndReason, err error) {
	if q.finished {
		return
	}

	// A timer or Limit completion queued before a fault must not mask that fault.
	if stopped := d.status(q); stopped != nil {
		err = stopped
	}

	q.finished = true
	delete(d.queries, q.id)
	if q.cancel != nil {
		q.cancel()
	}
	if !q.taskPending {
		d.querySlots--
	}

	var report DiscoveryReport
	if err == nil {
		report = q.builder.report(reason)
	}
	q.reply.Complete(report, err)
}

func (d *discoveryActor) stopping(actorx.Context, actorx.Stopping) error {
	// Unexpected loss of a management Actor closes Runtime admission, as with Factory.
	d.rt.beginClose()
	for _, q := range d.queries {
		d.finish(q, "", actorx.ErrClosed)
	}
	d.queries = nil
	d.querySlots = 0 // shutdown joins/discards remaining Tasks without Actor access
	return nil
}
