//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen
package remote

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

// proxyActor serializes admission while provider waits run as owned tasks.
//
//actorx:actor
type proxyActor struct {
	session *session
	address protocol.Address
	busy    bool
}

func (a *proxyActor) Started(ctx actorx.Context, _ actorx.Started) error {
	self, err := actorxdist.MustFrom(ctx).Self()
	if err != nil {
		return err
	}
	raw, err := self.MarshalJSON()
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &a.address); err != nil {
		return err
	}

	// Started waits on the session, whose HTTP reader runs outside this mailbox.
	createCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return a.session.create(createCtx, a.address)
}

func (a *proxyActor) Invoke(ctx actorx.Context, input protocol.Invoke, reply actorx.Reply[protocol.Result]) error {
	if input.Validate() != nil {
		return actorxdist.ErrProtocol
	}
	if a.busy {
		return actorx.ErrFull
	}

	a.busy = true
	address, s := a.address, a.session
	// The worker uses snapshots; the serial completion callback releases busy.
	_, err := actorx.StartTask(ctx, func(work context.Context) (protocol.Result, error) {
		return s.invoke(work, address, input)
	}, func(result protocol.Result, err error) bool {
		a.busy = false
		reply.Complete(result, err)
		return true
	}, nil)
	if err != nil {
		// Rejected tasks have no completion callback to release admission.
		a.busy = false
	}
	return err
}

func (a *proxyActor) InvokeStream(ctx actorx.Context, i protocol.InvokeStream, r actorx.StreamReply[protocol.Chunk]) error {
	if i.Validate() != nil {
		return actorxdist.ErrProtocol
	}
	if a.busy {
		return actorx.ErrFull
	}
	a.busy = true
	address, s := a.address, a.session
	// Call and Stream share busy; cancellation of this task leaves the Actor alive.
	_, err := actorx.StartTask(ctx, func(work context.Context) (struct{}, error) {
		for c, err := range s.invokeStream(work, address, i) {
			if err != nil {
				return struct{}{}, err
			}
			if err := r.Write(work, c, len(c.Content)); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	}, func(_ struct{}, err error) bool {
		a.busy = false
		r.Complete(err)
		return true
	}, nil)
	if err != nil {
		a.busy = false
	}
	return err
}
