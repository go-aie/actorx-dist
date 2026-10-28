package actorxremote

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

const callTimeout = 5 * time.Second

// Addr serializes Call and Stream and pins the first trustworthy generation.
// Different handles have independent operation locks.
type Addr struct {
	runtime   *Runtime
	operation chan struct{}
	mu        sync.Mutex
	address   Address
	uncertain bool
}

func newAddr(r *Runtime, actorType, id string) (*Addr, error) {
	if err := wire.Identity(actorType, id); err != nil {
		return nil, err
	}
	return &Addr{
		runtime:   r,
		operation: make(chan struct{}, 1),
		address:   Address{Version: 1, Type: actorType, ID: id},
	}, nil
}

// Address returns an independent, concurrency-safe snapshot.
func (a *Addr) Address() Address {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.address
}

func (a *Addr) unknown() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.address.Generation == "" {
		a.uncertain = true
	}
}

func (a *Addr) save(raw []byte) error {
	selected, err := wire.ParseAddress(raw, true)
	if err != nil {
		return protocolError()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if selected.Type != a.address.Type || selected.ID != a.address.ID ||
		(a.address.Generation != "" && selected.Generation != a.address.Generation) {
		return protocolError()
	}
	a.address = Address(selected)
	return nil
}

// Call encodes a JSON snapshot and uses one budget of at most five seconds for
// queueing, dispatch and response. A caller's earlier deadline takes precedence.
// Returned JSON values use json.Number rather than float64.
func (a *Addr) Call(ctx context.Context, message any) (any, error) {
	budget, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	ctx, end, err := a.runtime.begin(budget)
	if err != nil {
		return nil, err
	}
	defer end()

	input, err := snapshot(message)
	if err != nil {
		return nil, err
	}

	// Queueing consumes the same budget as dispatch and response handling.
	select {
	case a.operation <- struct{}{}:
		defer func() { <-a.operation }()
	case <-ctx.Done():
		return nil, beforeDispatch(ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return nil, beforeDispatch(err)
	}

	a.mu.Lock()
	uncertain := a.uncertain
	a.mu.Unlock()
	if uncertain {
		return nil, &RemoteError{Code: "address_uncertain"}
	}

	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, &TransportError{Code: "transport"}
	}
	id := "c:" + hex.EncodeToString(random[:])
	deadline, _ := ctx.Deadline()
	raw, err := wire.Request("actor/call", id, struct {
		Address  Address         `json:"address"`
		Input    json.RawMessage `json:"input"`
		Deadline int64           `json:"deadline_unix_ms"`
	}{a.Address(), input, deadline.UnixMilli()})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, beforeDispatch(err)
	}

	response, err := a.runtime.post(ctx, raw, false, "")
	if err != nil {
		if connectionFailed(err) {
			if ctx.Err() != nil {
				return nil, beforeDispatch(ctx.Err())
			}
			return nil, transportError(errConnection)
		}
		return nil, a.noEvidence(ctx, transportError(err))
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		err := statusError(response.StatusCode)
		if response.StatusCode >= 500 {
			return nil, a.noEvidence(ctx, err)
		}
		return nil, err
	}
	if mediaType(response.Header) != "application/json" {
		return nil, a.noEvidence(ctx, protocolError())
	}
	body, err := readBody(response.Body)
	if err != nil {
		return nil, a.noEvidence(ctx, err)
	}
	reply, err := wire.Response(body, id)
	if err != nil {
		return nil, a.noEvidence(ctx, protocolError())
	}
	if reply.Error != nil {
		if wire.ValidateError(reply.Error) != nil {
			return nil, a.noEvidence(ctx, protocolError())
		}
		e := reply.Error
		remote := &RemoteError{Code: wire.Kind(e.Code), RPCCode: e.Code, Data: e.Data}
		var data map[string]json.RawMessage
		_ = json.Unmarshal(e.Data, &data)
		if e.Code == 1013 && data["address"] == nil {
			return nil, a.noEvidence(ctx, protocolError())
		}
		// A valid failure can still prove which generation handled the request.
		if address := data["address"]; address != nil {
			if err := a.save(address); err != nil {
				return nil, a.noEvidence(ctx, err)
			}
		}
		if a.Address().Generation == "" && (e.Code == 1000 || e.Code == -32603 || e.Code == 1012) {
			return nil, a.noEvidence(ctx, remote)
		}
		if e.Code == 1003 || e.Code == 1004 {
			a.unknown()
		}
		return nil, remote
	}

	result, err := wire.Fields(reply.Result, []string{"address", "value"}, "address", "value")
	if err != nil {
		return nil, a.noEvidence(ctx, protocolError())
	}
	if err := a.save(result["address"]); err != nil {
		return nil, a.noEvidence(ctx, err)
	}
	value, err := wire.Decode(result["value"])
	if err != nil {
		return nil, a.noEvidence(ctx, protocolError())
	}
	return value, nil
}

func beforeDispatch(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	return remoteError(1005, err)
}

func (a *Addr) noEvidence(ctx context.Context, cause error) error {
	a.unknown()
	if errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	if ctx.Err() != nil {
		cause = ctx.Err()
	}
	return remoteError(1004, cause)
}
