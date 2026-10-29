package actorxdist

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"unicode/utf8"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/go-aie/actorx-dist/internal/strictjson"
)

const maxDiscoveryQueryBytes = 1 << 10

type discoveryQueryFrame struct {
	Version         int    `json:"version"`
	QueryID         string `json:"query_id"`
	OriginRuntimeID string `json:"origin_runtime_id"`
	DeadlineUnixMS  int64  `json:"deadline_unix_ms"`
}

type discoveryReplyFrame struct {
	Version   int              `json:"version"`
	QueryID   string           `json:"query_id"`
	RuntimeID string           `json:"runtime_id"`
	Entries   []discoveryEntry `json:"entries,omitempty"`
	Truncated bool             `json:"truncated,omitempty"`
	Error     string           `json:"error,omitempty"`
}

// validateDiscoveryObject complements strictjson with exact field names and non-null
// values. encoding/json alone accepts null scalars and case-insensitive names.
func validateDiscoveryObject(fields map[string]json.RawMessage, required, optional []string) error {
	if fields == nil {
		return ErrProtocol
	}

	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return ErrProtocol
		}
	}
	for name, value := range fields {
		if (!slices.Contains(required, name) && !slices.Contains(optional, name)) ||
			bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return ErrProtocol
		}
	}

	return nil
}

func decodeDiscoveryQuery(data []byte) (discoveryQueryFrame, error) {
	var frame discoveryQueryFrame
	if strictjson.Validate(data, maxDiscoveryQueryBytes, 2, 16) != nil {
		return frame, ErrProtocol
	}

	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil ||
		validateDiscoveryObject(fields, []string{"version", "query_id", "origin_runtime_id", "deadline_unix_ms"}, nil) != nil {
		return frame, ErrProtocol
	}
	if json.Unmarshal(data, &frame) != nil || !validDiscoveryQuery(frame) {
		return discoveryQueryFrame{}, ErrProtocol
	}

	return frame, nil
}

func validDiscoveryQuery(frame discoveryQueryFrame) bool {
	return frame.Version == 1 && opaque(frame.QueryID) &&
		opaque(frame.OriginRuntimeID) && frame.DeadlineUnixMS > 0
}

func validDiscoveryEntries(entries []discoveryEntry) bool {
	if len(entries) > maxDiscoveryEntries {
		return false
	}

	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if identity.Validate(entry.Type, "discovery") != nil || !validDigest(entry.Fingerprint) ||
			!utf8.ValidString(entry.Description) || len(entry.Description) > 512 || seen[entry.Type] {
			return false
		}
		seen[entry.Type] = true
	}

	return true
}

// A nonempty returned QueryID is safe to route even when the rest of the reply
// is invalid. Ambiguous syntax (including duplicate keys) is never correlated.
func decodeDiscoveryReply(data []byte) (discoveryReplyFrame, string, error) {
	var frame discoveryReplyFrame
	if strictjson.Validate(data, maxDiscoverySnapshotBytes, 4, 2048) != nil {
		return frame, "", ErrProtocol
	}

	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return frame, "", ErrProtocol
	}

	// Establish safe correlation before validating the remaining fields, so a
	// malformed but attributable response can fail its query explicitly.
	var queryID string
	_ = json.Unmarshal(fields["query_id"], &queryID)
	if !opaque(queryID) {
		return frame, "", ErrProtocol
	}

	err := validateDiscoveryObject(fields, []string{"version", "query_id", "runtime_id"}, []string{"entries", "truncated", "error"})
	if err != nil || json.Unmarshal(data, &frame) != nil || frame.Version != 1 || !opaque(frame.RuntimeID) {
		return discoveryReplyFrame{}, queryID, ErrProtocol
	}

	if encoded, ok := fields["entries"]; ok {
		var entries []json.RawMessage
		if json.Unmarshal(encoded, &entries) != nil {
			return discoveryReplyFrame{}, queryID, ErrProtocol
		}
		for _, entry := range entries {
			var fields map[string]json.RawMessage
			if json.Unmarshal(entry, &fields) != nil ||
				validateDiscoveryObject(fields, []string{"type", "fingerprint"}, []string{"description"}) != nil {
				return discoveryReplyFrame{}, queryID, ErrProtocol
			}
		}
	}

	// Error replies and snapshots are mutually exclusive, including explicitly
	// empty entries or a false truncated field.
	if _, hasError := fields["error"]; hasError {
		_, entries := fields["entries"]
		_, truncated := fields["truncated"]
		if entries || truncated || (frame.Error != "full" && frame.Error != "unavailable") {
			return discoveryReplyFrame{}, queryID, ErrProtocol
		}
	} else if !validDiscoveryEntries(frame.Entries) {
		return discoveryReplyFrame{}, queryID, ErrProtocol
	}

	return frame, queryID, nil
}

func encodeDiscoveryReply(queryID string, snapshot discoverySnapshot, cause error) ([]byte, error) {
	if !opaque(queryID) || !opaque(snapshot.RuntimeID) {
		return nil, ErrProtocol
	}
	frame := discoveryReplyFrame{Version: 1, QueryID: queryID, RuntimeID: snapshot.RuntimeID}
	if cause != nil {
		frame.Error = "unavailable"
		if errors.Is(cause, actorx.ErrFull) {
			frame.Error = "full"
		}
		return json.Marshal(frame)
	}
	if !validDiscoveryEntries(snapshot.Entries) {
		return nil, ErrProtocol
	}

	frame.Entries, frame.Truncated = snapshot.Entries, snapshot.Truncated

	// Select a bounded prefix using individual encodings, then encode only the
	// retained frame. The complete wire envelope, including QueryID, is charged.
	entries := frame.Entries
	frame.Entries = nil
	base, _ := json.Marshal(frame)
	size := len(base) + len(`,"entries":[]`)
	sizes := make([]int, 0, len(entries))
	for i, entry := range entries {
		cost := entry.jsonArrayBytes(i)
		if size+cost > maxDiscoverySnapshotBytes {
			break
		}
		size += cost
		sizes = append(sizes, cost)
	}

	if len(sizes) < len(entries) {
		if !frame.Truncated {
			size += len(`,"truncated":true`)
		}
		frame.Truncated = true
		for size > maxDiscoverySnapshotBytes && len(sizes) > 0 {
			size -= sizes[len(sizes)-1]
			sizes = sizes[:len(sizes)-1]
		}
	}

	frame.Entries = entries[:len(sizes)]
	return json.Marshal(frame)
}
