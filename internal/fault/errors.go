// Package fault shares sentinel identities between protocol codecs and dist.
package fault

import "errors"

var (
	ErrTypeUnavailable   = errors.New("dist: type unavailable")
	ErrNotActivated      = errors.New("dist: not activated")
	ErrContract          = errors.New("dist: incompatible contract")
	ErrProtocol          = errors.New("dist: unsupported protocol")
	ErrOwnerUnavailable  = errors.New("dist: owner unavailable")
	ErrActivationUnknown = errors.New("dist: activation outcome unknown")
	ErrOutcomeUnknown    = errors.New("dist: request outcome unknown")
	ErrExitUnknown       = errors.New("dist: exit unknown")
)
