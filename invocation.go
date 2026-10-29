package actorxdist

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/errwire"
	"github.com/go-aie/actorx-dist/internal/strictjson"
	"github.com/nats-io/nats.go"
)

type businessFrame struct {
	Version     int             `json:"version"`
	Kind        string          `json:"kind"`
	Operation   string          `json:"operation"`
	Type        string          `json:"type"`
	ID          string          `json:"id"`
	Generation  string          `json:"generation"`
	Correlation string          `json:"correlation"`
	Deadline    int64           `json:"deadline"`
	Message     string          `json:"message"`
	Mode        string          `json:"mode"`
	Fingerprint string          `json:"fingerprint"`
	Reply       string          `json:"reply,omitempty"`
	Control     string          `json:"control,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Status      string          `json:"status,omitempty"`
	Error       json.RawMessage `json:"error,omitempty"`
	Seq         uint64          `json:"seq"`
	Items       int             `json:"items,omitempty"`
	Bytes       int             `json:"bytes,omitempty"`
}

func decodeBusiness(data []byte, f *businessFrame) error {
	if strictjson.Validate(data, 128<<10, 40, 32768) != nil {
		return ErrProtocol
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(f) != nil {
		return ErrProtocol
	}
	if f.Version != 1 ||
		f.Operation != "invoke" ||
		!opaque(f.Generation) ||
		!opaque(f.Correlation) ||
		f.Deadline <= 0 ||
		len(f.Message) > 1024 ||
		f.Message == "" ||
		!validDigest(f.Fingerprint) ||
		(f.Mode != "send" && f.Mode != "call" && f.Mode != "stream") {
		return ErrProtocol
	}

	if f.Mode == "stream" && (f.Kind == "chunk" || f.Kind == "complete" || f.Kind == "window") {
		var required struct {
			Seq *uint64 `json:"seq"`
		}
		if json.Unmarshal(data, &required) != nil || required.Seq == nil {
			return ErrProtocol
		}
	}
	return nil
}

func (f businessFrame) matches(g businessFrame) bool {
	return f.Version == g.Version &&
		f.Operation == g.Operation &&
		f.Type == g.Type &&
		f.ID == g.ID &&
		f.Generation == g.Generation &&
		f.Correlation == g.Correlation &&
		f.Deadline == g.Deadline &&
		f.Message == g.Message &&
		f.Mode == g.Mode &&
		f.Fingerprint == g.Fingerprint
}

func takeSlot(slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (n *network) invokeBusiness(
	parent context.Context,
	addr Addr,
	kind actorx.Kind,
	msg any,
	result reflect.Type,
	yield func(any) error,
) (any, error) {
	ctx, cancel := requestBudget(parent)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Join Close before taking any resources. Close cannot pass a late Add.
	n.rt.mu.Lock()
	if n.rt.closed {
		n.rt.mu.Unlock()
		return nil, actorx.ErrClosed
	}
	n.rt.work.Add(1)
	n.rt.mu.Unlock()
	defer n.rt.work.Done()
	if kind == actorx.StreamKind {
		if !n.reserveStream() {
			return nil, actorx.ErrFull
		}
		defer n.releaseStream()
	}

	n.businessMu.Lock()
	if n.outgoing >= n.rt.limits.transport.Requests || kind == actorx.SendKind && n.acks >= n.rt.limits.sendACKs() {
		n.businessMu.Unlock()
		return nil, actorx.ErrFull
	}
	n.outgoing++
	if kind == actorx.SendKind {
		n.acks++
	}
	n.businessMu.Unlock()
	defer func() {
		n.businessMu.Lock()
		n.outgoing--
		if kind == actorx.SendKind {
			n.acks--
		}
		n.businessMu.Unlock()
	}()

	// Bound encoding scratch separately from the request and reply lifetimes.
	if !takeSlot(n.payloadSlots) {
		return nil, actorx.ErrFull
	}
	held := true
	defer func() {
		if held {
			<-n.payloadSlots
		}
	}()
	if !takeSlot(n.codecSlots) {
		return nil, actorx.ErrFull
	}

	// Validate and encode before activation can run application code.
	contract, id, payload, err := func() (wireContract, string, []byte, error) {
		defer func() { <-n.codecSlots }()
		c, err := n.typeContract(ctx, addr.key.Type)
		if err != nil {
			return c, "", nil, err
		}
		id, err := checkInvocation(c, msg, kind, result)
		if err != nil {
			return c, id, nil, err
		}
		b, err := encodeValue(msg)
		return c, id, b, err
	}()
	if err != nil {
		return nil, err
	}

	// Resolve only after local validation has succeeded.
	o, err := addr.awaitReady(ctx)
	if err != nil {
		return nil, err
	}
	if o.value.Fingerprint != contract.fingerprint {
		return nil, ErrContract
	}
	correlation := newOperation()
	reply, ch, err := n.reserveReply(ctx, correlation)
	if err != nil {
		return nil, err
	}
	defer n.releaseReply(correlation)
	mode := "call"
	if kind == actorx.StreamKind {
		mode = "stream"
	}
	if kind == actorx.SendKind {
		mode = "send"
	}
	f := businessFrame{
		Version:     1,
		Kind:        "request",
		Operation:   "invoke",
		Type:        addr.key.Type,
		ID:          addr.key.ID,
		Generation:  addr.generation(),
		Correlation: correlation,
		Deadline:    deadline(ctx).UnixNano(),
		Message:     id,
		Mode:        mode,
		Fingerprint: contract.fingerprint,
		Reply:       reply,
		Payload:     payload,
	}
	if kind != actorx.SendKind {
		f.Control = reply
	}

	var stream *receiveStream
	if kind == actorx.StreamKind {
		f.Items, f.Bytes = streamItems, streamBytes
		stream = newReceiveStream(n, ctx, f, result, yield)
		n.rpcMu.Lock()
		n.streamReplies[correlation] = stream
		n.rpcMu.Unlock()
	}
	if !takeSlot(n.codecSlots) {
		return nil, actorx.ErrFull
	}
	data, err := json.Marshal(f)
	if err != nil {
		<-n.codecSlots
		return nil, err
	}
	if len(data) > 128<<10 {
		<-n.codecSlots
		return nil, actorx.ErrChunkSize
	}
	if err = ctx.Err(); err != nil {
		<-n.codecSlots
		return nil, err
	}
	if !n.ready.Load() || !n.nc.IsConnected() {
		<-n.codecSlots
		return nil, ErrOwnerUnavailable
	}

	// No replay, Flush-as-ACK, or cancellation hook can overtake this attempt.
	if n.businessFault != nil {
		if e := n.businessFault("before_publish", f); e != nil {
			<-n.codecSlots
			return nil, e
		}
	}
	err = n.nc.PublishRequest(n.actorSubject(addr.key), reply, data)
	<-n.codecSlots
	if err == nil && n.businessFault != nil {
		err = n.businessFault("after_publish", f)
	}
	f.Payload = nil
	payload = nil
	data = nil
	<-n.payloadSlots
	held = false

	// After a publish attempt, loss of the final response leaves the outcome unknown.
	unknown := func(cause error) (any, error) {
		if kind != actorx.SendKind {
			n.cancelBusiness(f, cause)
		}
		return nil, errors.Join(ErrOutcomeUnknown, cause)
	}
	if err != nil {
		return unknown(err)
	}
	if stream != nil {
		err := stream.run()
		if err != nil {
			stream.cancelRemote(err)
		}
		return nil, err
	}

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case response := <-ch:
			if response.Header.Get("Status") == "503" {
				return unknown(ErrOwnerUnavailable)
			}
			var res businessFrame
			if decodeBusiness(response.Data, &res) != nil ||
				!res.matches(f) ||
				res.Kind != "complete" ||
				res.Reply != "" ||
				res.Control != "" ||
				res.Seq != 0 ||
				res.Items != 0 ||
				res.Bytes != 0 {
				return unknown(ErrProtocol)
			}
			if res.Status == "error" {
				var evidence struct {
					Admission string `json:"admission"`
				}
				if decodeRecord(res.Payload, &evidence) != nil || (evidence.Admission != "not_admitted" && evidence.Admission != "unknown") {
					return unknown(ErrProtocol)
				}
				remote, e := errwire.Decode(res.Error)
				if e != nil || remote == nil {
					return unknown(ErrProtocol)
				}
				return nil, remote
			}
			if res.Status != "ok" || len(res.Error) != 0 {
				return unknown(ErrProtocol)
			}
			if kind == actorx.SendKind || result == reflect.TypeFor[any]() {
				if string(res.Payload) != "null" {
					return unknown(ErrProtocol)
				}
				return nil, nil
			}
			if !takeSlot(n.codecSlots) {
				return nil, errors.Join(ErrOutcomeUnknown, actorx.ErrFull)
			}
			v, e := decodeValue(n.rt, res.Payload, result)
			<-n.codecSlots
			if e != nil {
				return unknown(e)
			}
			return v, nil
		case <-ctx.Done():
			return unknown(ctx.Err())
		case <-n.rt.life.Done():
			return unknown(actorx.ErrClosed)
		case <-ticker.C:
			if !n.ready.Load() || !n.nc.IsConnected() {
				return unknown(ErrOwnerUnavailable)
			}
		}
	}
}

type businessTarget struct {
	key                ActorKey
	generation, coreID string
	spec               registration
	work               *activationWork
}

func (n *network) businessCallback(target *businessTarget) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		if len(msg.Data) > 128<<10 {
			n.report("business-size", actorx.ErrChunkSize)
			return
		}
		in := incoming{msg: msg, business: target}
		var header struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(msg.Data, &header) != nil {
			return
		}
		if header.Kind == "cancel" {
			select {
			case n.control <- in:
			default:
				n.report("cancel-dispatch", actorx.ErrFull)
			}
			return
		}
		select {
		case n.ordinary <- in:
		default:
			in.overloaded = true
			select {
			case n.control <- in:
			default:
				n.report("business-dispatch", actorx.ErrFull)
			}
		}
	}
}

func validBusinessReply(reply, id string) bool {
	return safeReply(reply) && strings.HasPrefix(reply, "_INBOX.") && strings.HasSuffix(reply, "."+id)
}

func (n *network) handleBusiness(in incoming) {
	var f businessFrame
	if decodeBusiness(in.msg.Data, &f) != nil {
		return
	}
	target := in.business

	// A nonmatching generation must never provide admission evidence for it.
	if f.Type != target.key.Type ||
		f.ID != target.key.ID ||
		f.Generation != target.generation ||
		in.msg.Subject != n.actorSubject(target.key) ||
		f.Reply != in.msg.Reply ||
		!validBusinessReply(f.Reply, f.Correlation) {
		return
	}
	if f.Mode != "send" && f.Control != f.Reply || f.Mode == "send" && f.Control != "" {
		return
	}
	if f.Status != "" {
		return
	}
	key := f.Reply
	if f.Kind == "cancel" {
		if f.Mode == "send" || len(f.Payload) != 0 || f.Items != 0 || f.Bytes != 0 || f.Seq != 0 {
			return
		}
		if cause, err := errwire.Decode(f.Error); err != nil || cause == nil {
			return
		}
		n.businessMu.Lock()
		p := n.leases[key]
		n.businessMu.Unlock()
		if p != nil && p.f.matches(f) {
			p.cancel()
		}
		return
	}

	if f.Kind != "request" || len(f.Payload) == 0 || len(f.Error) != 0 || f.Seq != 0 {
		return
	}

	reject := func(err error) { n.respondBusiness(f, nil, err, true) }
	if f.Mode == "stream" {
		if f.Items != streamItems || f.Bytes != streamBytes {
			reject(ErrProtocol)
			return
		}
	} else if f.Items != 0 || f.Bytes != 0 {
		return
	}
	if f.Fingerprint != target.spec.contract.fingerprint {
		reject(ErrContract)
		return
	}
	if in.overloaded {
		reject(actorx.ErrFull)
		return
	}
	if !target.work.ready.Load() {
		reject(actorx.ErrFull)
		return
	}
	if target.work.stopping() || n.rt.checkOpen() != nil {
		reject(actorx.ErrStopped)
		return
	}
	if !n.ready.Load() {
		reject(ErrOwnerUnavailable)
		return
	}

	descriptor, ok := target.spec.messages[f.Message]
	if !ok {
		reject(actorx.ErrUnhandled)
		return
	}
	kind := actorx.CallKind
	if f.Mode == "stream" {
		kind = actorx.StreamKind
	}
	if f.Mode == "send" {
		kind = actorx.SendKind
	}
	if err := descriptor.Validate(kind); err != nil {
		reject(err)
		return
	}

	// Work retains the frame in the bounded dispatch queue/worker, not a new lease.
	if !takeSlot(n.codecSlots) {
		reject(actorx.ErrFull)
		return
	}
	query, err := decodeValue(n.rt, f.Payload, descriptor.MessageType())
	<-n.codecSlots
	if err != nil {
		reject(err)
		return
	}
	if v := reflect.ValueOf(query); v.Kind() == reflect.Pointer && v.IsNil() {
		reject(actorx.ErrUnhandled)
		return
	}
	if err = descriptor.Check(query, kind); err != nil {
		reject(err)
		return
	}

	limit := time.Unix(0, f.Deadline)
	if max := time.Now().Add(60 * time.Second); limit.After(max) {
		limit = max
	}
	ctx, cancel := context.WithDeadline(n.rt.life, limit)
	if err = ctx.Err(); err != nil {
		cancel()
		reject(err)
		return
	}

	p := &businessReply{n: n, f: f, cancel: cancel, key: key}
	p.f.Payload = nil
	if kind == actorx.SendKind {
		defer cancel()
		err = n.rt.execution.Deliver(ctx, actorx.Delivery{Target: target.coreID, Correlation: f.Correlation, Kind: kind, Message: query}, p)
		n.respondBusiness(f, nil, err, err != nil)
		return
	}

	if kind == actorx.StreamKind {
		if !n.reserveStream() {
			cancel()
			reject(actorx.ErrFull)
			return
		}
		n.rt.mu.Lock()
		if n.rt.closed {
			n.rt.mu.Unlock()
			n.releaseStream()
			cancel()
			reject(actorx.ErrClosed)
			return
		}
		n.rt.work.Add(1)
		n.rt.mu.Unlock()

		// Initialize the two Runtime inbox subscriptions before advertising credit routing.
		setupID := newOperation()
		_, _, setupErr := n.reserveReply(ctx, setupID)
		if setupErr != nil {
			n.releaseStream()
			n.rt.work.Done()
			cancel()
			reject(setupErr)
			return
		}
		n.releaseReply(setupID)
		p.stream = newSendStream(p, ctx)
	}

	n.businessMu.Lock()
	if len(n.leases) >= n.rt.limits.transport.Requests {
		err = actorx.ErrFull
	} else if n.leases[key] != nil {
		err = ErrProtocol
	} else {
		n.leases[key] = p
	}
	n.businessMu.Unlock()
	if err != nil {
		if p.stream != nil {
			n.releaseStream()
			n.rt.work.Done()
		}
		cancel()
		reject(err)
		return
	}

	// Receive and Deferred completion remain exclusively in actorx. This lease
	// stores only routing/cancel capabilities and never a query or Actor pointer.
	err = n.rt.execution.Deliver(ctx, actorx.Delivery{Target: target.coreID, Correlation: f.Correlation, Kind: kind, Message: query}, p)
	if err != nil {
		p.finish(nil, err, true)
	}
}

type businessReply struct {
	n      *network
	f      businessFrame
	key    string
	cancel context.CancelFunc
	once   sync.Once
	stream *sendStream
}

func (p *businessReply) Addresses() actorx.AddressImporter { return nil }

func (p *businessReply) Write(ctx context.Context, value any, size int) error {
	if p.stream == nil {
		return actorx.ErrMode
	}
	return p.stream.write(ctx, value, size, false)
}

func (p *businessReply) WriteConsumed(ctx context.Context, value any, size int) error {
	if p.stream == nil {
		return actorx.ErrMode
	}
	return p.stream.write(ctx, value, size, true)
}

func (p *businessReply) Complete(value any, err error) bool { return p.finish(value, err, false) }

func (p *businessReply) finish(value any, err error, rejected bool) bool {
	won := false
	p.once.Do(func() {
		won = true
		p.cancel()

		// Cancel before stream completion joins a writer waiting for window credit.
		if p.stream != nil {
			p.stream.complete(err, rejected)
		} else {
			p.n.respondBusiness(p.f, value, err, rejected)
		}

		p.n.businessMu.Lock()
		delete(p.n.leases, p.key)
		p.n.businessMu.Unlock()
		if p.stream != nil {
			p.n.releaseStream()
			p.n.rt.work.Done()
		}
	})
	return won
}

func (n *network) respondBusiness(req businessFrame, value any, err error, rejected bool) {
	f := req
	f.Kind = "complete"
	f.Reply = ""
	f.Control = ""
	f.Items, f.Bytes = 0, 0
	f.Payload = json.RawMessage("null")
	f.Status = "ok"
	if err == nil && req.Mode == "call" {
		if !takeSlot(n.payloadSlots) {
			err = actorx.ErrFull
		} else {
			defer func() { <-n.payloadSlots }()
			if !takeSlot(n.codecSlots) {
				err = actorx.ErrFull
			} else {
				defer func() { <-n.codecSlots }()
				f.Payload, err = encodeValue(value)
			}
		}
	}
	if err != nil {
		f.Status = "error"
		admission := "unknown"
		if rejected {
			admission = "not_admitted"
		}
		f.Payload, _ = json.Marshal(struct {
			Admission string `json:"admission"`
		}{admission})
		f.Error, _ = errwire.Encode(err, errwire.Source{Operation: req.Mode, Type: req.Type, ID: req.ID, Generation: req.Generation})
	}
	if n.businessFault != nil {
		if e := n.businessFault("reply", f); e != nil {
			n.report("business-reply", e)
			return
		}
	}
	data, e := json.Marshal(f)
	if e == nil && len(data) <= 128<<10 {
		if e = n.nc.Publish(req.Reply, data); e != nil {
			n.report("business-reply", e)
		}
	} else {
		n.report("business-reply", ErrProtocol)
	}
}
