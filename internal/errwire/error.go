// Package errwire is the bounded v1 framework error envelope. It never
// serializes arbitrary Go error values or infers classifications from text.
package errwire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/go-aie/actorx"
	dist "github.com/go-aie/actorx-dist/internal/fault"
	"github.com/go-aie/actorx-dist/internal/strictjson"
)

const MaxBytes = 8192
const MaxMessageBytes = 512
const MaxSources = 4

// Order is part of the canonical envelope; Decode rejects reordered or repeated codes.
var classes = []struct {
	code string
	err  error
}{
	{"closed", actorx.ErrClosed},
	{"stopped", actorx.ErrStopped},
	{"full", actorx.ErrFull},
	{"unhandled", actorx.ErrUnhandled},
	{"reply_type", actorx.ErrReplyType},
	{"mode", actorx.ErrMode},
	{"setup", actorx.ErrSetup},
	{"phase", actorx.ErrPhase},
	{"chunk_size", actorx.ErrChunkSize},
	{"type_unavailable", dist.ErrTypeUnavailable},
	{"not_activated", dist.ErrNotActivated},
	{"contract", dist.ErrContract},
	{"protocol", dist.ErrProtocol},
	{"owner_unavailable", dist.ErrOwnerUnavailable},
	{"activation_unknown", dist.ErrActivationUnknown},
	{"outcome_unknown", dist.ErrOutcomeUnknown},
	{"exit_unknown", dist.ErrExitUnknown},
	{"canceled", context.Canceled},
	{"deadline", context.DeadlineExceeded},
}

// Source identifies an operation, without payload or physical routing details.
type Source struct {
	Operation  string `json:"operation"`
	Type       string `json:"type"`
	ID         string `json:"id"`
	Generation string `json:"generation,omitempty"`
}

// Envelope is the versioned wire representation of bounded error diagnostics.
type Envelope struct {
	Version   int      `json:"version"`
	Codes     []string `json:"codes"`
	Message   string   `json:"message"`
	Sources   []Source `json:"sources"`
	Truncated bool     `json:"truncated,omitempty"`
}

// Remote preserves framework classifications without reconstructing application errors.
type Remote struct{ envelope Envelope }

func (e *Remote) Error() string { return e.envelope.Message }

func (e *Remote) Is(target error) bool {
	for _, c := range classes {
		if target == c.err {
			for _, code := range e.envelope.Codes {
				if code == c.code {
					return true
				}
			}
		}
	}
	return false
}

func (e *Remote) Sources() []Source { return append([]Source(nil), e.envelope.Sources...) }

func bounded(value string, max int, truncated *bool) string {
	value = strings.ToValidUTF8(value, "�")
	if len(value) <= max {
		return value
	}
	*truncated = true
	value = value[:max]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

// Encode preserves all finite classifications; diagnostics retain the nearest
// source followed by the deepest downstream sources if the chain overflows.
func Encode(err error, source Source) ([]byte, error) {
	if err == nil {
		return []byte("null"), nil
	}
	e := Envelope{Version: 1, Codes: []string{}, Sources: []Source{source}}
	e.Message = bounded(err.Error(), MaxMessageBytes, &e.Truncated)
	for _, c := range classes {
		if errors.Is(err, c.err) {
			e.Codes = append(e.Codes, c.code)
		}
	}

	// Traverse wrapping and joining only for diagnostics. A bounded traversal
	// avoids retaining unbounded remote source trees.
	visited := 0
	var walk func(error, int)
	walk = func(err error, depth int) {
		if err == nil {
			return
		}
		visited++
		if depth > 16 || visited > 64 {
			e.Truncated = true
			return
		}
		if r, ok := err.(*Remote); ok {
			e.Sources = append(e.Sources, r.envelope.Sources...)
			e.Truncated = e.Truncated || r.envelope.Truncated
			return
		}
		switch v := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range v.Unwrap() {
				walk(child, depth+1)
			}
		case interface{ Unwrap() error }:
			walk(v.Unwrap(), depth+1)
		}
	}
	walk(err, 0)
	if len(e.Sources) > MaxSources {
		e.Sources = append(e.Sources[:1:1], e.Sources[len(e.Sources)-(MaxSources-1):]...)
		e.Truncated = true
	}
	for i := range e.Sources {
		s := &e.Sources[i]
		s.Operation = bounded(s.Operation, 32, &e.Truncated)
		s.Type = bounded(s.Type, 64, &e.Truncated)
		s.ID = bounded(s.ID, 128, &e.Truncated)
		s.Generation = bounded(s.Generation, 22, &e.Truncated)
	}
	data, marshalErr := json.Marshal(e)
	if marshalErr != nil {
		return nil, marshalErr
	}

	// JSON escaping can expand diagnostics sixfold. Reduce text further,
	// never the classification set, to respect the total envelope limit.
	for len(data) > MaxBytes && len(e.Message) > 0 {
		e.Truncated = true
		e.Message = bounded(e.Message, len(e.Message)/2, &e.Truncated)
		data, marshalErr = json.Marshal(e)
		if marshalErr != nil {
			return nil, marshalErr
		}
	}
	if len(data) > MaxBytes {
		return nil, actorx.ErrChunkSize
	}
	return data, nil
}

// Decode returns the remote error separately from malformed-envelope failures.
// A null envelope returns nil for both results.
func Decode(data []byte) (error, error) {
	if len(data) > MaxBytes {
		return nil, actorx.ErrChunkSize
	}
	if strictjson.Validate(data, MaxBytes, 8, 256) != nil {
		return nil, dist.ErrProtocol
	}
	if bytes.Equal(data, []byte("null")) {
		return nil, nil
	}
	var e Envelope
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return nil, dist.ErrProtocol
	}
	if d.Decode(new(any)) != io.EOF || e.Version != 1 || e.Codes == nil || e.Sources == nil ||
		len(e.Message) > MaxMessageBytes || !utf8.ValidString(e.Message) || len(e.Sources) > MaxSources || len(e.Codes) > len(classes) {
		return nil, dist.ErrProtocol
	}

	// Codes must be unique and in canonical order.
	last := -1
	for _, code := range e.Codes {
		index := -1
		for i, c := range classes {
			if c.code == code {
				index = i
				break
			}
		}
		if index <= last {
			return nil, dist.ErrProtocol
		}
		last = index
	}
	for _, s := range e.Sources {
		if len(s.Operation) > 32 || len(s.Type) > 64 || len(s.ID) > 128 || len(s.Generation) > 22 {
			return nil, dist.ErrProtocol
		}
	}
	return &Remote{envelope: e}, nil
}
