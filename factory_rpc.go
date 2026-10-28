package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/errwire"
	"github.com/nats-io/nats.go"
)

type selection struct {
	State      string `json:"state"`
	Revision   uint64 `json:"revision"`
	Generation string `json:"generation"`
	Factory    string `json:"factory"`
}

// Two lazy Runtime inbox subscriptions share reply and ordered credit routing.
// Reply correlations share the Requests ceiling; callbacks never execute user code.
func (n *network) reserveReply(ctx context.Context, id string) (string, chan *nats.Msg, error) {
	n.rpcMu.Lock()
	defer n.rpcMu.Unlock()
	if n.pending == nil {
		n.pending = make(map[string]chan *nats.Msg)
		n.replyHeld = map[string]bool{}
		n.streamReplies = map[string]*receiveStream{}
	}
	if len(n.pending) >= n.rt.limits.transport.Requests {
		return "", nil, actorx.ErrFull
	}

	if n.replies == nil {
		n.inbox = nats.NewInbox() + "."
		sub, err := n.nc.Subscribe(n.inbox+"*", func(msg *nats.Msg) {
			if len(msg.Data) > 128<<10 {
				return
			}
			id := strings.TrimPrefix(msg.Subject, n.inbox)
			n.rpcMu.Lock()
			defer n.rpcMu.Unlock()
			if stream := n.streamReplies[id]; stream != nil {
				stream.accept(msg)
				return
			}
			ch := n.pending[id]
			if ch != nil && !n.replyHeld[id] {
				if !takeSlot(n.payloadSlots) {
					n.report("reply-buffer", actorx.ErrFull)
					return
				}
				select {
				case ch <- msg:
					n.replyHeld[id] = true
				default:
					<-n.payloadSlots
				}
			}
		})
		if err != nil {
			return "", nil, err
		}
		frames := n.rt.limits.replyFrames()
		if err = sub.SetPendingLimits(frames, frames*(128<<10)); err != nil {
			_ = sub.Unsubscribe()
			return "", nil, err
		}
		if err = n.nc.FlushWithContext(ctx); err != nil {
			_ = sub.Unsubscribe()
			return "", nil, err
		}

		control, err := n.nc.Subscribe(n.inbox+"control.*", n.streamCredit)
		if err != nil {
			_ = sub.Unsubscribe()
			return "", nil, err
		}
		if err = control.SetPendingLimits(frames, frames*(128<<10)); err == nil {
			err = n.nc.FlushWithContext(ctx)
		}
		if err != nil {
			_ = control.Unsubscribe()
			_ = sub.Unsubscribe()
			return "", nil, err
		}
		n.streamControls = control
		n.replies = sub
	}

	ch := make(chan *nats.Msg, 1)
	n.pending[id] = ch
	return n.inbox + id, ch, nil
}

func (n *network) factoryRequest(
	ctx context.Context,
	operation string,
	key ActorKey,
	gen, factory, fingerprint string,
	payload json.RawMessage,
) (owned, error) {
	if err := ctx.Err(); err != nil {
		return owned{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	id := newOperation()
	reply, ch, err := n.reserveReply(ctx, id)
	if err != nil {
		return owned{}, err
	}
	defer n.releaseReply(id)
	req := factoryFrame{
		Version:     1,
		Kind:        "request",
		Operation:   operation,
		Type:        key.Type,
		ID:          key.ID,
		Generation:  gen,
		Factory:     factory,
		Fingerprint: fingerprint,
		Correlation: id,
		Deadline:    deadline(ctx).UnixNano(),
		Reply:       reply,
		Payload:     payload,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return owned{}, err
	}
	subject := factorySubject(key.Type)
	if operation != "activate" {
		subject = strings.TrimSuffix(controlSubject(key.Type, factory), "*") + operation
	}
	if !n.nc.IsConnected() {
		return owned{}, ErrOwnerUnavailable
	}
	if err = n.nc.PublishRequest(subject, reply, data); err != nil {
		return owned{}, errors.Join(ErrActivationUnknown, err)
	}
	var msg *nats.Msg
	select {
	case msg = <-ch:
	case <-ctx.Done():
		return owned{}, errors.Join(ErrActivationUnknown, ctx.Err())
	}
	if msg.Header.Get("Status") == "503" {
		if operation == "activate" {
			return owned{}, ErrTypeUnavailable
		}
		return owned{}, ErrOwnerUnavailable
	}
	var res factoryFrame
	if decodeFactoryFrame(msg.Data, &res) != nil ||
		res.Version != 1 ||
		res.Kind != "complete" ||
		res.Operation != operation ||
		res.Type != key.Type ||
		res.ID != key.ID ||
		res.Correlation != id ||
		res.Deadline != req.Deadline ||
		res.Fingerprint != fingerprint ||
		res.Reply != "" {
		return owned{}, ErrProtocol
	}
	if operation != "activate" && (res.Generation != gen || res.Factory != factory) {
		return owned{}, ErrProtocol
	}
	if res.Status == "error" {
		if len(res.Error) == 0 {
			return owned{}, ErrProtocol
		}
		remote, err := errwire.Decode(res.Error)
		if err != nil {
			return owned{}, err
		}
		if remote == nil {
			return owned{}, ErrProtocol
		}
		return owned{}, remote
	}
	if res.Status != "ok" || len(res.Error) != 0 {
		return owned{}, ErrProtocol
	}
	if operation == "stop" {
		return owned{}, nil
	}
	var selected selection
	if decodeRecord(res.Payload, &selected) != nil ||
		selected.Revision == 0 ||
		!opaque(selected.Generation) ||
		!opaque(selected.Factory) ||
		selected.Generation != res.Generation ||
		selected.Factory != res.Factory {
		return owned{}, ErrProtocol
	}
	switch selected.State {
	case "STARTING", "READY", "STOPPING", "FAILED":
	default:
		return owned{}, ErrProtocol
	}

	// This is a confirmation result, not a fabricated Registry record. It is used
	// only for selection; subsequent updates always load the full durable record.
	return owned{value: ownership{
		Version:     1,
		Key:         key,
		Generation:  selected.Generation,
		Factory:     selected.Factory,
		Fingerprint: fingerprint,
		State:       selected.State,
	}, revision: selected.Revision}, nil
}

// The reply buffer slot survives transfer to a waiting caller until its decode
// finishes, including cancellation racing the SDK callback.
func (n *network) releaseReply(id string) {
	n.rpcMu.Lock()
	defer n.rpcMu.Unlock()
	delete(n.pending, id)
	delete(n.streamReplies, id)
	if n.replyHeld[id] {
		delete(n.replyHeld, id)
		<-n.payloadSlots
	}
}
