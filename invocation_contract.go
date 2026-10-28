package actorxdist

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/go-aie/actorx-dist/internal/strictjson"
	"github.com/nats-io/nats.go"
)

func newSchemaBuilder() schemaBuilder {
	return schemaBuilder{ids: map[reflect.Type]string{}, nodes: map[string]any{}, visiting: map[reflect.Type]bool{}}
}

func typeIdentity(t reflect.Type) (string, error) {
	b := newSchemaBuilder()
	return b.typ(t, 0)
}

// Only immutable canonical bytes are cached. Caller validation uses its concrete
// DTOs, never an Actor implementation or a list supplied by the application.
func (n *network) typeContract(ctx context.Context, typ string) (wireContract, error) {
	if c, ok := n.contracts.get(typ); ok {
		return c, nil
	}
	e, err := n.registry.get(ctx, typesBucket, "t."+identity.Token(typ), false)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return wireContract{}, ErrTypeUnavailable
		}
		return wireContract{}, err
	}
	data := e.Value()
	if len(data) > maxCachedContractBytes {
		return wireContract{}, ErrContract
	}
	sum := sha256.Sum256(data)
	return n.contracts.put(typ, wireContract{data, hex.EncodeToString(sum[:])})
}

func checkInvocation(c wireContract, msg any, kind actorx.Kind, result reflect.Type) (string, error) {
	if msg == nil {
		return "", actorx.ErrUnhandled
	}
	b := newSchemaBuilder()
	id, err := b.typ(reflect.TypeOf(msg), 0)
	if err != nil {
		return "", err
	}

	var root []json.RawMessage
	if (strictjson.Validate(c.data, 64<<10, 40, 32768) != nil || json.Unmarshal(c.data, &root) != nil) ||
		len(root) != 3 ||
		string(root[0]) != "1" {
		return "", ErrContract
	}
	var handlers [][]json.RawMessage
	var definitions [][]json.RawMessage
	if json.Unmarshal(root[1], &handlers) != nil || json.Unmarshal(root[2], &definitions) != nil {
		return "", ErrContract
	}

	// Select by wire identity before checking the requested mode and result type.
	var selected []json.RawMessage
	for _, h := range handlers {
		var name string
		if len(h) != 3 || json.Unmarshal(h[0], &name) != nil {
			return "", ErrContract
		}
		if name == id {
			selected = h
		}
	}
	if selected == nil {
		return "", actorx.ErrUnhandled
	}

	var modes []string
	if json.Unmarshal(selected[2], &modes) != nil {
		return "", ErrContract
	}
	mode := "send"
	if kind == actorx.CallKind {
		mode = "call"
	}
	if kind == actorx.StreamKind {
		mode = "stream"
	}
	if !slices.Contains(modes, mode) {
		return "", actorx.ErrMode
	}
	if kind != actorx.SendKind {
		if string(selected[1]) == "null" {
			if result != reflect.TypeFor[any]() {
				return "", actorx.ErrReplyType
			}
		} else {
			rid, err := b.typ(result, 0)
			if err != nil {
				return "", errors.Join(actorx.ErrReplyType, err)
			}
			var remote string
			if json.Unmarshal(selected[1], &remote) != nil || remote != rid {
				return "", actorx.ErrReplyType
			}
		}
	}

	// Matching names alone are insufficient: all reachable schema nodes must agree.
	nodes := map[string]json.RawMessage{}
	for _, def := range definitions {
		var id string
		if len(def) != 2 || json.Unmarshal(def[0], &id) != nil {
			return "", ErrContract
		}
		nodes[id] = def[1]
	}
	for id, node := range b.nodes {
		raw, _ := json.Marshal(node)
		if !bytes.Equal(nodes[id], raw) {
			return "", ErrContract
		}
	}

	v := reflect.ValueOf(msg)
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return "", actorx.ErrUnhandled
	}
	return id, nil
}
