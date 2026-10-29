package actorxdist

import "github.com/go-aie/actorx"

// ActorKey identifies a logical object across activations.
// Validation takes place at Runtime entry points without network I/O.
type ActorKey struct {
	Type string
	ID   string
}

// ActorSpec declares a worker's logical type and local constructor.
// T must be a concrete pointer to a struct implementing actorx.Actor.
// Runtime.Register validates that restriction without invoking New.
// Use Runtime.Register for both startup and later declarations; the returned
// Registration handle owns withdrawal through Unregister.
type ActorSpec[T actorx.Actor] struct {
	// Type is the logical type name callers combine with an ID to address an Actor.
	// Runtimes registering the same Type must declare the same message contract.
	Type string

	// Description optionally explains what this Actor type does.
	// It must be valid UTF-8 of at most 512 bytes and does not affect the contract.
	Description string

	// New constructs a fresh, non-nil Actor for the given logical key when this
	// Runtime activates it. Register does not call New.
	New func(ActorKey) T
}
