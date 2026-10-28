package actorxremote

import "github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"

// Snapshot through the standard marshaler, then enforce wire constraints on the
// encoded bytes. Neither requests nor decoded results retain caller-owned maps.
func snapshot(value any) ([]byte, error) { return wire.Encode(value) }
