package actorxremote

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

type providerInstance struct {
	lock  chan struct{}
	actor any // accessed only with lock held
}

type providerWork struct {
	request  wire.Message
	address  Address
	instance *providerInstance
	deadline time.Time
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	settled  chan struct{} // RPC terminal is fixed; user code may still be running
	mu       sync.Mutex
	started  bool
	terminal *wire.Message
	stream   *providerStream
}

func providerFailure(id json.RawMessage, code int, address *Address) wire.Message {
	data := map[string]any{"kind": wire.Kind(code)}
	if address != nil {
		data["address"] = *address
	}
	raw, _ := wire.Encode(data)
	return wire.Message{JSONRPC: "2.0", ID: id, Error: &wire.RPCError{
		Code: code, Message: "Remote request failed", Data: raw,
	}}
}

func (p *registration) dispatch(m wire.Message) error {
	if !m.Request {
		return protocolError()
	}
	if p.ctx.Err() != nil {
		return p.ctx.Err()
	}
	if m.Method == "stream/ack" || m.Method == "stream/cancel" {
		return p.control(m)
	}

	if m.ID != nil {
		// Decode string escapes for correlation, retaining numeric precision.
		id, _ := wire.Decode(m.ID)
		key := "null"
		switch v := id.(type) {
		case string:
			key = "s:" + v
		case json.Number:
			key = "n:" + v.String()
		}
		if _, exists := p.seen[key]; exists {
			return protocolError()
		}
		p.seen[key] = struct{}{}
	}

	if m.Method != "actor/create" && m.Method != "actor/call" && m.Method != "actor/stream" {
		return p.enqueue(providerFailure(m.ID, -32601, nil), time.Now().Add(callTimeout))
	}
	required := []string{"address", "deadline_unix_ms"}
	if m.Method == "actor/call" || m.Method == "actor/stream" {
		required = append(required, "input")
	}
	if m.Method == "actor/stream" {
		required = append(required, "stream_token", "window")
	}
	f, err := wire.Fields(m.Params, required, required...)
	var a wire.Address
	var ms int64
	if err == nil {
		a, err = wire.ParseAddress(f["address"], true)
	}
	if err != nil || a.Type != p.actorType || string(f["deadline_unix_ms"]) == "null" || json.Unmarshal(f["deadline_unix_ms"], &ms) != nil {
		return p.enqueue(providerFailure(m.ID, -32602, nil), time.Now().Add(callTimeout))
	}
	if m.Method == "actor/stream" {
		if m.ID == nil {
			return protocolError()
		}
		if _, e := wire.Token(f["stream_token"]); e != nil || wire.CheckWindow(f["window"]) != nil {
			return p.enqueue(providerFailure(m.ID, -32602, nil), time.Now().Add(callTimeout))
		}
	}
	address := Address(a)
	reject := func(code int, a *Address) wire.Message {
		out := providerFailure(m.ID, code, a)
		if m.Method == "actor/stream" {
			out = streamFailure(out, 0)
		}
		return out
	}

	// Convert UTC once, then keep the monotonic component for every local wait.
	now := time.Now()
	deadline := now.Add(time.UnixMilli(ms).Sub(now))
	if !now.Before(deadline) {
		return p.enqueue(reject(1005, &address), deadline)
	}

	// Admission reserves both the execution slot and any new instance before
	// starting a job. The SSE reader never waits for user code or a reply POST.
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.active) >= providerCapacity || (m.Method == "actor/stream" && len(p.streams) >= providerCapacity) {
		return p.enqueue(reject(1001, &address), deadline)
	}
	instance := p.instances[address]
	if instance == nil {
		if m.Method != "actor/create" {
			return p.enqueue(reject(-32602, nil), deadline)
		}
		if len(p.instances) >= providerCapacity {
			return p.enqueue(reject(1001, &address), deadline)
		}
		instance = &providerInstance{lock: make(chan struct{}, 1)}
		p.instances[address] = instance
	}
	ctx, cancel := context.WithDeadline(p.ctx, deadline)
	w := &providerWork{
		request:  m,
		address:  address,
		instance: instance,
		deadline: deadline,
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		settled:  make(chan struct{}),
	}
	if m.Method == "actor/stream" {
		token, _ := wire.Token(f["stream_token"])
		w.stream = newProviderStream(p, w, token)
		key, _ := wire.IDKey(m.ID)
		p.streams[key] = w.stream
		p.jobs.Go(w.stream.expire)
	}
	p.active[w] = struct{}{}
	p.jobs.Go(func() { p.execute(w) })
	return nil
}

// finish is the single terminal publication point. Expiry cannot become a late
// success even when user code ignores cancellation and eventually returns.
func (w *providerWork) finish(result wire.Message) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminal != nil {
		return
	}
	if w.ctx.Err() != nil {
		code := 1009
		if w.started {
			code = 1004
		} else if w.ctx.Err() == context.DeadlineExceeded {
			code = 1005
		}
		result = providerFailure(w.request.ID, code, &w.address)
	}
	if w.stream != nil {
		w.stream.mu.Lock()
		w.stream.finished = true
		if result.Error != nil {
			result = streamFailure(result, w.stream.seq)
		}
		empty := len(w.stream.sizes) == 0
		w.stream.mu.Unlock()
		if empty {
			w.stream.remove()
		}
	}
	w.terminal = &result
	close(w.settled)
}

func (p *registration) execute(w *providerWork) {
	defer w.cancel()
	defer close(w.done)
	defer func() {
		p.mu.Lock()
		delete(p.active, w)
		p.mu.Unlock()
	}()

	// Separate RPC settlement from user execution: expiry may publish a
	// terminal immediately, but this job still joins the running operation.
	operationDone := make(chan struct{})
	go func() {
		defer close(operationDone)
		w.finish(p.operate(w))
	}()
	select {
	case <-operationDone:
	case <-w.ctx.Done():
		w.finish(wire.Message{})
	}

	w.mu.Lock()
	terminal := *w.terminal
	w.mu.Unlock()
	if w.stream != nil {
		w.stream.sendMu.Lock()
		p.reply(terminal, w.deadline)
		w.stream.sendMu.Unlock()
	} else {
		p.reply(terminal, w.deadline)
	}
	// RPC settlement never releases ownership of still-running user code.
	<-operationDone
}

func (p *registration) operate(w *providerWork) wire.Message {
	fail := func(code int) wire.Message { return providerFailure(w.request.ID, code, &w.address) }
	select {
	case w.instance.lock <- struct{}{}:
		defer func() { <-w.instance.lock }()
	case <-w.ctx.Done():
		return fail(1009)
	}

	w.mu.Lock()
	if w.ctx.Err() != nil {
		w.mu.Unlock()
		return fail(1009)
	}
	w.started = true
	w.mu.Unlock()

	result := map[string]any{"address": w.address}
	if w.request.Method == "actor/create" {
		if w.instance.actor == nil {
			actor, ok := construct(p.factory)
			if !ok {
				return fail(1000)
			}
			w.instance.actor = actor
		}
	} else if w.stream != nil {
		actor, ok := w.instance.actor.(StreamActor)
		if !ok {
			return fail(1013)
		}
		var f map[string]json.RawMessage
		_ = json.Unmarshal(w.request.Params, &f)
		input, err := wire.Decode(f["input"])
		if err != nil {
			return fail(1000)
		}

		ok = handleStream(w.ctx, actor, input, w.stream)
		w.stream.mu.Lock()
		failed := w.stream.failure != nil
		seq := w.stream.seq
		w.stream.finished = true // writer expires when the method returns
		w.stream.mu.Unlock()
		if !ok || failed {
			return fail(1000)
		}
		result["final_seq"] = seq
	} else {
		// Capability belongs to the real instance and is checked under the same
		// lock as execution. A missing operation never calls another method.
		actor, ok := w.instance.actor.(Actor)
		if !ok {
			return fail(1013)
		}
		var f map[string]json.RawMessage
		_ = json.Unmarshal(w.request.Params, &f)
		input, err := wire.Decode(f["input"])
		if err != nil {
			return fail(1000)
		}
		value, ok := handle(w.ctx, actor, input)
		if !ok {
			return fail(1000)
		}
		raw, err := snapshot(value)
		if err != nil {
			return fail(1000)
		}
		result["value"] = json.RawMessage(raw)
	}

	raw, err := wire.Encode(result)
	if err != nil {
		return fail(1000)
	}
	reply := wire.Message{JSONRPC: "2.0", ID: w.request.ID, Result: raw}
	if _, err := wire.Encode(reply); err != nil {
		return fail(1000)
	}
	return reply
}

// Each user entry point recovers locally. Error and panic values never leave
// this boundary, including custom JSON serialization (handled by wire.Encode).
func construct(factory ActorFactory) (actor any, ok bool) {
	defer func() {
		if recover() != nil {
			actor, ok = nil, false
		}
	}()

	actor, err := factory()
	if err != nil || actor == nil {
		return nil, false
	}
	v := reflect.ValueOf(actor)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return nil, false
		}
	}
	_, call := actor.(Actor)
	_, stream := actor.(StreamActor)
	return actor, call || stream
}

func handle(ctx context.Context, actor Actor, message any) (value any, ok bool) {
	defer func() {
		if recover() != nil {
			value, ok = nil, false
		}
	}()
	value, err := actor.Handle(ctx, message)
	return value, err == nil
}
