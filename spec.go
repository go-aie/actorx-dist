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
	Type string
	New  func(ActorKey) T
}
