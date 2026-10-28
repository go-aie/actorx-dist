package actorxdist

import "github.com/go-aie/actorx-dist/internal/fault"

var (
	ErrTypeUnavailable   = fault.ErrTypeUnavailable
	ErrNotActivated      = fault.ErrNotActivated
	ErrContract          = fault.ErrContract
	ErrProtocol          = fault.ErrProtocol
	ErrOwnerUnavailable  = fault.ErrOwnerUnavailable
	ErrActivationUnknown = fault.ErrActivationUnknown
	ErrOutcomeUnknown    = fault.ErrOutcomeUnknown
	ErrExitUnknown       = fault.ErrExitUnknown
)
