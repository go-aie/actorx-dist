package actorxremote

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

// ActorInfo describes a registered Remote type, not a running instance.
type ActorInfo struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	Protocol    string `json:"protocol"`
}

// ActorsResult is a finite directory observation, without a completeness guarantee.
type ActorsResult struct {
	Actors []ActorInfo `json:"actors"`
}

// Actors lists compatible Remote registrations. Cluster results do not imply
// that this gateway can Call or Stream every returned type. It never retries.
func (r *Runtime) Actors(ctx context.Context) (ActorsResult, error) {
	if ctx == nil {
		return ActorsResult{}, fmt.Errorf("actorxremote: invalid directory query")
	}

	budget, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	ctx, end, err := r.begin(budget)
	if err != nil {
		return ActorsResult{}, err
	}
	defer end()
	if err := ctx.Err(); err != nil {
		return ActorsResult{}, err
	}

	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return ActorsResult{}, &TransportError{Code: "transport"}
	}
	id := "d:" + hex.EncodeToString(random[:])
	raw, err := wire.Request("runtime/actors", id, struct{}{})
	if err != nil {
		return ActorsResult{}, err
	}

	response, err := r.post(ctx, raw, false, "")
	if err != nil {
		if ctx.Err() != nil {
			return ActorsResult{}, ctx.Err()
		}
		return ActorsResult{}, transportError(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return ActorsResult{}, statusError(response.StatusCode)
	}
	if mediaType(response.Header) != "application/json" {
		return ActorsResult{}, protocolError()
	}

	body, err := readBody(response.Body)
	if err != nil {
		return ActorsResult{}, err
	}
	reply, err := wire.Response(body, id)
	if err != nil {
		return ActorsResult{}, protocolError()
	}
	if reply.Error != nil {
		if wire.ValidateError(reply.Error) != nil {
			return ActorsResult{}, protocolError()
		}
		return ActorsResult{}, &RemoteError{
			Code:    wire.Kind(reply.Error.Code),
			RPCCode: reply.Error.Code,
			Data:    reply.Error.Data,
		}
	}
	return decodeActors(reply.Result)
}

func decodeActors(raw []byte) (ActorsResult, error) {
	bad := func() (ActorsResult, error) { return ActorsResult{}, protocolError() }

	fields, err := wire.Fields(raw, []string{"actors"}, "actors")
	if err != nil || len(fields["actors"]) == 0 || fields["actors"][0] != '[' {
		return bad()
	}
	var entries []json.RawMessage
	if json.Unmarshal(fields["actors"], &entries) != nil {
		return bad()
	}

	result := ActorsResult{Actors: make([]ActorInfo, 0, len(entries))}
	previous := ""
	for _, entry := range entries {
		f, err := wire.Fields(entry, []string{"type", "description", "protocol"}, "type", "description", "protocol")
		if err != nil {
			return bad()
		}

		var a ActorInfo
		for name, target := range map[string]*string{
			"type":        &a.Type,
			"description": &a.Description,
			"protocol":    &a.Protocol,
		} {
			if len(f[name]) == 0 || f[name][0] != '"' || json.Unmarshal(f[name], target) != nil {
				return bad()
			}
		}

		// Strict ordering also rejects duplicate types in the directory.
		if wire.Identity(a.Type, "directory") != nil ||
			a.Type <= previous ||
			len(a.Description) > 512 ||
			a.Protocol != wire.Version {
			return bad()
		}
		previous = a.Type
		result.Actors = append(result.Actors, a)
	}
	return result, nil
}
