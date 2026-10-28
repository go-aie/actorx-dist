package actorxdist

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/errwire"
	"github.com/go-aie/actorx-dist/internal/identity"
)

type terminal struct {
	Version          int             `json:"version"`
	Operation        string          `json:"operation"`
	Key              ActorKey        `json:"key"`
	Generation       string          `json:"generation"`
	Factory          string          `json:"factory"`
	Cause            json.RawMessage `json:"cause"`
	CreatedInstance  bool            `json:"created_instance"`
	Evidence         string          `json:"evidence"`
	Digest           string          `json:"digest"`
	Intended         string          `json:"intended"`
	Status           string          `json:"status"`
	Disposition      string          `json:"disposition,omitempty"`
	Prepared         uint64          `json:"prepared_revision,omitempty"`
	ReleaseOperation string          `json:"release_operation,omitempty"`
	ReleaseRevision  uint64          `json:"release_revision,omitempty"`
}

type terminalEntry struct {
	value    terminal
	revision uint64
}

// releaseReceipt is retained by the original owner after an acknowledged write.
// A new generation or an INACTIVE Get alone cannot manufacture this evidence.
type releaseReceipt struct {
	owner    owned
	prepared terminalEntry
}

func validDigest(s string) bool {
	raw, err := hex.DecodeString(s)
	return err == nil && len(raw) == 32 && hex.EncodeToString(raw) == s
}

func cleanupDigest(t terminal) string {
	data, _ := json.Marshal([]any{t.Version, t.Key, t.Generation, t.Factory, t.Cause, t.CreatedInstance, t.Evidence, t.Intended})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validTerminal(v terminal) bool {
	if v.Version != 1 ||
		identity.Validate(v.Key.Type, v.Key.ID) != nil ||
		!opaque(v.Generation) ||
		!opaque(v.Factory) ||
		!opaque(v.Operation) ||
		v.Digest != cleanupDigest(v) {
		return false
	}
	if _, err := errwire.Decode(v.Cause); err != nil {
		return false
	}
	if v.Intended == "FAILED_HELD" && string(v.Cause) == "null" {
		return false
	}
	if v.Intended != "RELEASED" && v.Intended != "FAILED_HELD" {
		return false
	}
	if v.Evidence != "no_instance" && v.Evidence != "spawn_rollback" && v.Evidence != "local_wait" {
		return false
	}
	if v.CreatedInstance != (v.Evidence == "local_wait") {
		return false
	}
	switch v.Status {
	case "PREPARED":
		return v.Prepared == 0 && v.Disposition == "" && v.ReleaseOperation == "" && v.ReleaseRevision == 0
	case "COMMITTED":
		if v.Prepared == 0 {
			return false
		}
		if v.Disposition == "FAILED_HELD" {
			return v.Intended == "FAILED_HELD" && v.ReleaseOperation == "" && v.ReleaseRevision == 0
		}
		return v.Disposition == "RELEASED" && opaque(v.ReleaseOperation) && v.ReleaseRevision > 0
	}
	return false
}

func sameTerminal(a, b terminal) bool {
	return a.Key == b.Key && a.Generation == b.Generation && a.Factory == b.Factory && a.Digest == b.Digest
}

func (r *registry) exit(ctx context.Context, key ActorKey, generation string) (terminalEntry, error) {
	entry, err := r.get(ctx, exitsBucket, "e."+generation, true)
	if err != nil {
		return terminalEntry{}, errors.Join(ErrExitUnknown, err)
	}
	var v terminal
	if decodeRecord(entry.Value(), &v) != nil || !validTerminal(v) || v.Key != key || v.Generation != generation {
		return terminalEntry{}, ErrProtocol
	}
	return terminalEntry{v, entry.Revision()}, nil
}

func (r *registry) prepare(ctx context.Context, v terminal) (terminalEntry, error) {
	if !validTerminal(v) || v.Status != "PREPARED" {
		return terminalEntry{}, ErrProtocol
	}
	key := "e." + v.Generation
	data, _ := json.Marshal(v)
	rev, err := r.writeVerified(ctx, exitsBucket, key, data, 0, true)
	if err != nil {
		existing, readErr := r.exit(ctx, v.Key, v.Generation)
		if errors.Is(readErr, ErrProtocol) {
			return terminalEntry{}, ErrProtocol
		}
		if readErr == nil {
			if !sameTerminal(existing.value, v) {
				return terminalEntry{}, ErrProtocol
			}
			return existing, nil
		}
		return terminalEntry{}, errors.Join(ErrExitUnknown, err)
	}
	return terminalEntry{v, rev}, nil
}

func matchesPrepared(o owned, p terminalEntry) bool {
	return validOwner(o.value) &&
		validTerminal(p.value) &&
		p.revision > 0 &&
		o.value.Key == p.value.Key &&
		o.value.Generation == p.value.Generation &&
		o.value.Factory == p.value.Factory
}

func (r *registry) fail(ctx context.Context, o owned, p terminalEntry, operation string) (owned, error) {
	if !matchesPrepared(o, p) ||
		p.value.Status != "PREPARED" ||
		p.value.Intended != "FAILED_HELD" ||
		o.value.State != "STARTING" ||
		!opaque(operation) {
		return owned{}, ErrProtocol
	}
	next := o.value
	next.State = "FAILED"
	next.ExitKey = "e." + next.Generation
	next.Digest = p.value.Digest
	next.Prepared = p.revision
	next.Operation = operation
	return r.change(ctx, o, next)
}

func (r *registry) release(ctx context.Context, o owned, p terminalEntry, operation string) (releaseReceipt, error) {
	if !matchesPrepared(o, p) || !opaque(operation) || (o.value.State != "STOPPING" && o.value.State != "FAILED") {
		return releaseReceipt{}, ErrProtocol
	}
	prepared := p.revision
	if p.value.Status == "COMMITTED" {
		if p.value.Disposition != "FAILED_HELD" {
			return releaseReceipt{}, ErrProtocol
		}
		prepared = p.value.Prepared
	}
	if o.value.State == "FAILED" && (o.value.Digest != p.value.Digest || o.value.Prepared != prepared) {
		return releaseReceipt{}, ErrProtocol
	}
	next := o.value
	next.State = "INACTIVE"
	next.Operation = operation
	next.ExitKey = "e." + next.Generation
	next.Digest = p.value.Digest
	next.Prepared = prepared
	released, err := r.change(ctx, o, next)
	if err != nil {
		return releaseReceipt{}, err
	}
	return releaseReceipt{released, p}, nil
}

// commitHeld is valid only after the exact FAILED revision was established.
func (r *registry) commitHeld(ctx context.Context, p terminalEntry, failed owned, operation string) (terminalEntry, error) {
	if !matchesPrepared(failed, p) ||
		failed.value.State != "FAILED" ||
		failed.value.Digest != p.value.Digest ||
		failed.value.Prepared != p.revision ||
		p.value.Status != "PREPARED" ||
		p.value.Intended != "FAILED_HELD" ||
		!opaque(operation) {
		return terminalEntry{}, ErrProtocol
	}

	// Confirmation fences stale FAILED evidence against concurrent release.
	confirmed, err := r.confirm(ctx, failed, failed.value.Factory)
	if err != nil {
		return terminalEntry{}, err
	}
	_ = confirmed
	next := p.value
	next.Status = "COMMITTED"
	next.Disposition = "FAILED_HELD"
	next.Prepared = p.revision
	next.Operation = operation
	return r.commit(ctx, p, next)
}

func (r *registry) commitReleased(ctx context.Context, receipt releaseReceipt, operation string) (terminalEntry, error) {
	o, p := receipt.owner, receipt.prepared
	if !matchesPrepared(o, p) || o.value.State != "INACTIVE" || o.value.Digest != p.value.Digest || !opaque(operation) {
		return terminalEntry{}, ErrProtocol
	}
	next := p.value
	next.Status = "COMMITTED"
	next.Disposition = "RELEASED"
	next.Prepared = o.value.Prepared
	next.ReleaseOperation = o.value.Operation
	next.ReleaseRevision = o.revision
	next.Operation = operation
	return r.commit(ctx, p, next)
}

func (r *registry) commit(ctx context.Context, previous terminalEntry, next terminal) (terminalEntry, error) {
	if !validTerminal(next) {
		return terminalEntry{}, ErrProtocol
	}
	data, _ := json.Marshal(next)
	rev, err := r.writeVerified(ctx, exitsBucket, "e."+next.Generation, data, previous.revision, true)
	if err != nil {
		return terminalEntry{}, errors.Join(ErrExitUnknown, err)
	}
	return terminalEntry{next, rev}, nil
}

// observeExit is a Registry observation, not public distributed Addr.Wait.
// PREPARED and absent records never establish successful completion.
func (r *registry) observeExit(ctx context.Context, key ActorKey, generation string) (actorx.Exit, error) {
	p, err := r.exit(ctx, key, generation)
	if err != nil {
		return actorx.Exit{}, err
	}
	if p.value.Status != "COMMITTED" {
		return actorx.Exit{}, ErrExitUnknown
	}
	reason, err := errwire.Decode(p.value.Cause)
	if err != nil {
		return actorx.Exit{}, err
	}
	return actorx.Exit{Reason: reason}, nil
}

// recoverRelease accepts only the original retained release operation and
// matching persistent evidence. A replacement current record provides no proof.
func (r *registry) recoverRelease(ctx context.Context, p terminalEntry, operation string) (releaseReceipt, error) {
	if !validTerminal(p.value) || !opaque(operation) {
		return releaseReceipt{}, ErrProtocol
	}
	o, err := r.owner(ctx, p.value.Key)
	if err != nil {
		return releaseReceipt{}, errors.Join(ErrExitUnknown, err)
	}
	prepared := p.revision
	if p.value.Status == "COMMITTED" {
		prepared = p.value.Prepared
	}
	if !matchesPrepared(o, p) ||
		o.value.State != "INACTIVE" ||
		o.value.Operation != operation ||
		o.value.Digest != p.value.Digest ||
		o.value.Prepared != prepared {
		return releaseReceipt{}, ErrExitUnknown
	}
	return releaseReceipt{o, p}, nil
}
