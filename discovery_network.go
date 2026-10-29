package actorxdist

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-aie/actorx"
	"github.com/nats-io/nats.go"
)

// Both endpoints are permanent for this Runtime lifetime. There is no network
// query table, per-query subscription or discovery-specific worker pool.
func (n *network) subscribeDiscovery(ctx context.Context) error {
	n.discoveryInbox = n.rt.names.DiscoveryReplySubject(n.rt.runtimeID)
	sub, err := n.nc.Subscribe(n.rt.names.DiscoveryQuerySubject(), n.discoveryQueryCallback)
	if err != nil {
		return err
	}

	n.discoveryQueries = sub
	frames := n.rt.limits.replyFrames()
	if err = sub.SetPendingLimits(frames, frames*maxDiscoveryQueryBytes); err == nil {
		n.discoveryReplies, err = n.nc.Subscribe(n.discoveryInbox, n.discoveryReplyCallback)
	}
	if err == nil {
		err = n.discoveryReplies.SetPendingLimits(frames, frames*maxDiscoverySnapshotBytes)
	}
	if err == nil {
		err = n.nc.FlushWithContext(ctx)
	}
	if err != nil {
		n.closeDiscovery()
	}

	return err
}

// Admission and callback joining prevent a callback from retaining a buffer or
// enqueueing new work after quiesce. No lock is held while calling an Actor.
func (n *network) enterDiscovery() bool {
	n.discoveryMu.Lock()
	defer n.discoveryMu.Unlock()
	if n.discoveryClosed || n.rt.life.Err() != nil {
		return false
	}

	n.discoveryCallbacks.Add(1)
	return true
}

func (n *network) closeDiscovery() {
	n.discoveryMu.Lock()
	n.discoveryClosed = true
	queries, replies := n.discoveryQueries, n.discoveryReplies
	n.discoveryMu.Unlock()

	if queries != nil {
		_ = queries.Unsubscribe()
	}
	if replies != nil {
		_ = replies.Unsubscribe()
	}
	n.discoveryCallbacks.Wait()
}

func (n *network) discoveryReceiveFailure() {
	n.discoveryFailures.Add(1)
	n.report("discovery-receive", actorx.ErrFull)
}

func (n *network) discoveryQueryCallback(msg *nats.Msg) {
	if !n.enterDiscovery() {
		return
	}
	defer n.discoveryCallbacks.Done()

	if len(msg.Data) > maxDiscoveryQueryBytes {
		n.report("discovery-query", ErrProtocol)
		return
	}
	if !takeSlot(n.payloadSlots) {
		n.report("discovery-dispatch", actorx.ErrFull)
		return
	}

	select {
	case n.ordinary <- incoming{msg: msg, discovery: true}:
	default:
		<-n.payloadSlots
		n.report("discovery-dispatch", actorx.ErrFull)
	}
}

func (n *network) handleDiscovery(in incoming) {
	defer func() { <-n.payloadSlots }()
	if !takeSlot(n.codecSlots) {
		n.report("discovery-query", actorx.ErrFull)
		return
	}
	query, err := decodeDiscoveryQuery(in.msg.Data)
	<-n.codecSlots

	if err != nil || in.msg.Reply != n.rt.names.DiscoveryReplySubject(query.OriginRuntimeID) {
		n.report("discovery-query", ErrProtocol)
		return
	}
	if query.OriginRuntimeID == n.rt.runtimeID {
		return // The initiator supplies its own local snapshot, once.
	}

	end := time.UnixMilli(query.DeadlineUnixMS)
	if !time.Now().Before(end) {
		return
	}

	// An untrusted deadline cannot hold one of the shared dispatch workers forever.
	ctx, cancel := context.WithDeadline(n.rt.life, minTime(end, time.Now().Add(2*time.Second)))
	defer cancel()

	var snapshot discoverySnapshot
	if !n.ready.Load() || !n.nc.IsConnected() {
		err = ErrOwnerUnavailable
	} else {
		snapshot, err = n.rt.discovery.Call[discoverySnapshot](ctx, snapshotRequest{ctx: ctx})
	}
	if ctx.Err() != nil || !time.Now().Before(end) {
		return
	}

	// Recheck availability after the asynchronous scan. An unavailable node
	// reports an error instead of presenting its directory as a valid snapshot.
	if !n.ready.Load() || !n.nc.IsConnected() {
		err = ErrOwnerUnavailable
	}
	if takeSlot(n.codecSlots) {
		defer func() { <-n.codecSlots }()
	} else {
		err = actorx.ErrFull // a fixed-size error needs no snapshot encode buffer
	}
	if err != nil {
		snapshot = discoverySnapshot{RuntimeID: n.rt.runtimeID}
	}

	data, err := encodeDiscoveryReply(query.QueryID, snapshot, err)
	if ctx.Err() != nil || !time.Now().Before(end) {
		return
	}
	if err == nil {
		err = n.nc.Publish(in.msg.Reply, data)
	}
	if err != nil {
		n.report("discovery-reply", err)
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (n *network) discoveryReplyCallback(msg *nats.Msg) {
	if !n.enterDiscovery() {
		return
	}
	defer n.discoveryCallbacks.Done()

	if len(msg.Data) > maxDiscoverySnapshotBytes || !takeSlot(n.payloadSlots) {
		n.discoveryReceiveFailure()
		return
	}
	defer func() { <-n.payloadSlots }()
	if !takeSlot(n.codecSlots) {
		n.discoveryReceiveFailure()
		return
	}
	defer func() { <-n.codecSlots }()

	frame, queryID, err := decodeDiscoveryReply(msg.Data)
	if queryID == "" {
		n.report("discovery-reply", ErrProtocol)
		return
	}
	if err == nil {
		switch frame.Error {
		case "full":
			err = actorx.ErrFull
		case "unavailable":
			err = ErrOwnerUnavailable
		}
	}
	node := discoveryNodeReply{
		queryID: queryID,
		err:     err,
		snapshot: discoverySnapshot{
			RuntimeID: frame.RuntimeID,
			Entries:   frame.Entries,
			Truncated: frame.Truncated,
		},
	}
	if err := n.rt.discovery.Send(n.rt.life, node); err != nil {
		n.discoveryReceiveFailure()
	}
}

// Capture the signal and epoch together. Faults close this signal even if a
// reconnect races with the caller; a later query captures the new epoch instead.
type discoveryTransport struct {
	epoch    uint64
	lost     <-chan struct{}
	failures uint64
}

func (n *network) observeDiscovery() (discoveryTransport, error) {
	epoch, lost := n.ready.observe()
	state := discoveryTransport{epoch, lost, n.discoveryFailures.Load()}
	return state, n.checkDiscovery(state)
}

func (n *network) checkDiscovery(state discoveryTransport) error {
	if n.rt.life.Err() != nil {
		return actorx.ErrClosed
	}

	// Receive loss cannot always be attributed to one query. Fail every query
	// using an older baseline rather than silently returning an incomplete report.
	if state.failures != n.discoveryFailures.Load() {
		return actorx.ErrFull
	}
	if state.epoch&1 == 0 || n.ready.epoch() != state.epoch || !n.nc.IsConnected() {
		return ErrOwnerUnavailable
	}

	return nil
}

func (n *network) publishDiscovery(ctx context.Context, query discoveryQueryFrame, state discoveryTransport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !takeSlot(n.codecSlots) {
		return actorx.ErrFull
	}
	defer func() { <-n.codecSlots }()

	if !validDiscoveryQuery(query) || query.OriginRuntimeID != n.rt.runtimeID {
		return ErrProtocol
	}
	data, err := json.Marshal(query)
	if err != nil {
		return err
	}
	if len(data) > maxDiscoveryQueryBytes {
		return ErrProtocol
	}

	if time.Now().UnixMilli() >= query.DeadlineUnixMS {
		return context.DeadlineExceeded
	}
	if err := n.checkDiscovery(state); err != nil {
		return err
	}
	if err := n.nc.PublishRequest(n.rt.names.DiscoveryQuerySubject(), n.discoveryInbox, data); err != nil {
		return ErrOwnerUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return n.checkDiscovery(state)
}
