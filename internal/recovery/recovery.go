// Package recovery implements the offline operator tool, independent of a
// business Runtime. It never subscribes, activates, or inherits a Factory token.
package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/go-aie/actorx-dist/internal/errwire"
	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/go-aie/actorx-dist/internal/naming"
	"github.com/go-aie/actorx-dist/internal/strictjson"
	"github.com/nats-io/nats.go"
)

// Key has the existing wire shape; this package does not define a business API.
type Key struct{ Type, ID string }

// Snapshot retains the observed generation, revision and records for an exact Apply.
type Snapshot struct {
	Namespace  string          `json:"namespace"`
	Key        Key             `json:"key"`
	Generation string          `json:"generation"`
	Factory    string          `json:"factory"`
	Revision   uint64          `json:"revision"`
	Current    json.RawMessage `json:"current"`
	Terminal   json.RawMessage `json:"terminal,omitempty"`
}

type owner struct {
	Version     int    `json:"version"`
	Operation   string `json:"operation"`
	Key         Key    `json:"key"`
	Generation  string `json:"generation"`
	Factory     string `json:"factory"`
	Fingerprint string `json:"fingerprint"`
	State       string `json:"state"`
	ExitKey     string `json:"exit_key,omitempty"`
	Digest      string `json:"digest,omitempty"`
	Prepared    uint64 `json:"prepared_revision,omitempty"`
}

// Keep unknown fields rejected: administrative mutation must fail closed when
// run against a newer protocol, rather than discard fields it cannot interpret.
type terminal struct {
	Version          int             `json:"version"`
	Operation        string          `json:"operation"`
	Key              Key             `json:"key"`
	Generation       string          `json:"generation"`
	Factory          string          `json:"factory"`
	Cause            json.RawMessage `json:"cause"`
	Created          bool            `json:"created_instance"`
	Evidence         string          `json:"evidence"`
	Digest           string          `json:"digest"`
	Intended         string          `json:"intended"`
	Status           string          `json:"status"`
	Disposition      string          `json:"disposition,omitempty"`
	Prepared         uint64          `json:"prepared_revision,omitempty"`
	ReleaseOperation string          `json:"release_operation,omitempty"`
	ReleaseRevision  uint64          `json:"release_revision,omitempty"`
}

func decode(b []byte, out any) error {
	if strictjson.Validate(b, 16<<10, 16, 1024) != nil {
		return errors.New("invalid recovery record")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(out)
}

func opaque(s string) bool {
	return validToken(s)
}

func keyName(k Key) string { return "a." + identity.Token(k.Type) + "." + identity.Token(k.ID) }

func handles(ctx context.Context, nc *nats.Conn, namespace string) (nats.KeyValue, nats.KeyValue, error) {
	names, err := naming.New(namespace)
	if err != nil {
		return nil, nil, err
	}
	js, err := nc.JetStream(nats.Context(ctx))
	if err != nil {
		return nil, nil, err
	}
	c, err := js.KeyValue(names.CurrentBucket)
	if err != nil {
		return nil, nil, err
	}
	e, err := js.KeyValue(names.ExitsBucket)
	return c, e, err
}

// Inspect reads current ownership and any terminal evidence without modifying either.
func Inspect(ctx context.Context, nc *nats.Conn, namespace string, key Key) (Snapshot, error) {
	if identity.Validate(key.Type, key.ID) != nil {
		return Snapshot{}, errors.New("invalid key")
	}
	c, e, err := handles(ctx, nc, namespace)
	if err != nil {
		return Snapshot{}, err
	}
	v, err := c.Get(keyName(key))
	if err != nil {
		return Snapshot{}, err
	}
	var o owner
	if decode(v.Value(), &o) != nil ||
		o.Version != 1 ||
		o.Key != key ||
		!opaque(o.Generation) ||
		!opaque(o.Factory) ||
		!opaque(o.Operation) ||
		!digest(o.Fingerprint) {
		return Snapshot{}, errors.New("unsupported ownership record")
	}
	switch o.State {
	case "STARTING", "READY", "STOPPING":
		if o.ExitKey != "" || o.Digest != "" || o.Prepared != 0 {
			return Snapshot{}, errors.New("invalid active ownership evidence")
		}
	case "FAILED", "INACTIVE":
		if o.ExitKey != "e."+o.Generation || !digest(o.Digest) || o.Prepared == 0 {
			return Snapshot{}, errors.New("invalid terminal ownership evidence")
		}
	default:
		return Snapshot{}, errors.New("unsupported ownership state")
	}
	s := Snapshot{
		Namespace:  namespace,
		Key:        key,
		Generation: o.Generation,
		Factory:    o.Factory,
		Revision:   v.Revision(),
		Current:    append([]byte(nil), v.Value()...),
	}
	p, err := e.Get("e." + o.Generation)
	if err == nil {
		s.Terminal = append([]byte(nil), p.Value()...)
	} else if !errors.Is(err, nats.ErrKeyNotFound) {
		return Snapshot{}, err
	}
	return s, nil
}

func digest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

func validTerminal(p terminal, s Snapshot) bool {
	if p.Version != 1 || p.Key != s.Key || p.Generation != s.Generation || p.Factory != s.Factory || !opaque(p.Operation) {
		return false
	}
	b, _ := json.Marshal([]any{p.Version, p.Key, p.Generation, p.Factory, p.Cause, p.Created, p.Evidence, p.Intended})
	sum := sha256.Sum256(b)
	if p.Digest != hex.EncodeToString(sum[:]) {
		return false
	}
	if _, err := errwire.Decode(p.Cause); err != nil {
		return false
	}
	if p.Intended != "RELEASED" && p.Intended != "FAILED_HELD" {
		return false
	}
	if p.Intended == "FAILED_HELD" && string(p.Cause) == "null" {
		return false
	}
	if p.Evidence != "no_instance" && p.Evidence != "spawn_rollback" && p.Evidence != "local_wait" {
		return false
	}
	if p.Created != (p.Evidence == "local_wait") {
		return false
	}
	if p.Status == "PREPARED" {
		return p.Prepared == 0 && p.Disposition == "" && p.ReleaseOperation == "" && p.ReleaseRevision == 0
	}
	if p.Status != "COMMITTED" || p.Prepared == 0 {
		return false
	}
	if p.Disposition == "FAILED_HELD" {
		return p.Intended == "FAILED_HELD" && p.ReleaseOperation == "" && p.ReleaseRevision == 0
	}
	return p.Disposition == "RELEASED" && opaque(p.ReleaseOperation) && p.ReleaseRevision > 0
}

// Apply requires an exact current revision AND the operator's process fence.
// audit must be a durable append sink (the command fsyncs each event). The tool
// writes intent before mutation; an uncertain result requires a fresh Inspect.
func Apply(ctx context.Context, nc *nats.Conn, namespace string, expected Snapshot, stopped bool, reason string, audit io.Writer) error {
	if expected.Namespace != namespace {
		return errors.New("recovery namespace mismatch; inspect the selected namespace again")
	}
	if !stopped || len(reason) == 0 || len(reason) > 960 || audit == nil || expected.Revision == 0 || !opaque(expected.Generation) {
		return errors.New("recovery requires stopped-process evidence, reason, audit and exact generation/revision")
	}
	s, err := Inspect(ctx, nc, namespace, expected.Key)
	if err != nil {
		return err
	}
	if s.Generation != expected.Generation || s.Factory != expected.Factory || s.Revision != expected.Revision {
		return errors.New("stale recovery request; inspect again")
	}
	operation, err := identity.NewOpaque()
	if err != nil {
		return err
	}
	log := func(stage string, outcome any) error {
		return json.NewEncoder(audit).Encode(struct {
			Operation, Stage, Reason string
			Snapshot                 Snapshot
			Result                   any
		}{
			operation,
			stage,
			reason,
			s,
			outcome,
		})
	}
	if err = log("intent", nil); err != nil {
		return err
	}
	c, e, err := handles(ctx, nc, namespace)
	if err != nil {
		return err
	}
	var o owner
	_ = decode(s.Current, &o)
	pEntry, readErr := e.Get("e." + s.Generation)
	var p terminal
	if readErr != nil && !errors.Is(readErr, nats.ErrKeyNotFound) {
		return readErr
	}
	if readErr == nil && (decode(pEntry.Value(), &p) != nil || !validTerminal(p, s)) {
		return errors.New("conflicting terminal evidence; refusing mutation")
	}
	if readErr != nil {
		// No cleanup evidence: remove only this fenced ownership revision. The
		// durable audit records why. Do not create a terminal record or Exit.
		err = c.Delete(keyName(s.Key), nats.LastRevision(s.Revision))
		if err != nil {
			_ = log("unknown", err.Error())
			return err
		}
		return log("released-without-exit", nil)
	}
	prepared := pEntry.Revision()
	if p.Status == "COMMITTED" {
		prepared = p.Prepared
	}
	if o.State == "INACTIVE" {
		if o.Digest != p.Digest || o.Prepared != prepared || o.ExitKey != "e."+s.Generation {
			return errors.New("release evidence mismatch")
		}
	} else {
		if p.Status == "COMMITTED" && p.Disposition == "RELEASED" {
			return errors.New("released terminal conflicts with active ownership")
		}
		if o.State == "FAILED" && (o.Digest != p.Digest || o.Prepared != prepared) {
			return errors.New("failed evidence mismatch")
		}
		o.State = "INACTIVE"
		o.Operation = operation
		o.ExitKey = "e." + s.Generation
		o.Digest = p.Digest
		o.Prepared = prepared
		b, _ := json.Marshal(o)
		rev, writeErr := c.Update(keyName(s.Key), b, s.Revision)
		if writeErr != nil {
			_ = log("unknown", writeErr.Error())
			return writeErr
		}
		s.Revision = rev
	}
	if p.Status == "COMMITTED" && p.Disposition == "RELEASED" {
		if p.ReleaseOperation != o.Operation || p.ReleaseRevision != s.Revision {
			return errors.New("release receipt mismatch")
		}
		return log("already-committed", nil)
	}
	p.Status = "COMMITTED"
	p.Disposition = "RELEASED"
	p.Prepared = prepared
	p.ReleaseOperation = o.Operation
	p.ReleaseRevision = s.Revision
	p.Operation = operation
	b, _ := json.Marshal(p)
	if _, err = e.Update("e."+s.Generation, b, pEntry.Revision()); err != nil {
		_ = log("unknown", err.Error())
		return err
	}
	return log("committed-release", json.RawMessage(b))
}
