package actorxdist

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

func TestDiscoveryQueryWire(t *testing.T) {
	frame := discoveryQueryFrame{1, token(t), token(t), time.Now().Add(time.Second).UnixMilli()}
	data, _ := json.Marshal(frame)
	if got, err := decodeDiscoveryQuery(data); err != nil || got != frame {
		t.Fatal(got, err)
	}

	for _, data := range []string{
		`null`, `{}`, `[]`, string(data) + `{}`,
		strings.Replace(string(data), `"version":1`, `"version":null`, 1),
		strings.Replace(string(data), `"version":1`, `"version":"1"`, 1),
		strings.Replace(string(data), `"version":1`, `"version":2`, 1),
		strings.Replace(string(data), `"version":1`, `"version":1,"version":1`, 1),
		strings.Replace(string(data), `"version":1`, `"Version":1`, 1),
		strings.Replace(string(data), `"version":1`, `"version":1,"extra":1`, 1),
		strings.Replace(string(data), frame.QueryID, "bad.*", 1),
		strings.Replace(string(data), frame.OriginRuntimeID, "bad", 1),
		strings.Replace(string(data), fmt.Sprint(frame.DeadlineUnixMS), "0", 1),
		string(data) + strings.Repeat(" ", maxDiscoveryQueryBytes),
		string(data) + "\xff",
	} {
		if _, err := decodeDiscoveryQuery([]byte(data)); !errors.Is(err, ErrProtocol) {
			t.Fatalf("accepted query %q: %v", data, err)
		}
	}
}

func TestDiscoveryReplyWire(t *testing.T) {
	id, runtimeID := token(t), token(t)
	entry := discoveryEntry{"demo", strings.Repeat("a", 64), "说明"}
	snapshot := discoverySnapshot{RuntimeID: runtimeID, Entries: []discoveryEntry{entry}}
	data, err := encodeDiscoveryReply(id, snapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, correlated, err := decodeDiscoveryReply(data)
	if err != nil || correlated != id || got.RuntimeID != runtimeID || len(got.Entries) != 1 || got.Entries[0] != entry {
		t.Fatal(got, correlated, err)
	}

	// A reliable QueryID survives field validation errors for Actor correlation.
	for _, bad := range []string{
		strings.Replace(string(data), `"version":1,`, ``, 1),
		strings.Replace(string(data), `"version":1`, `"version":2`, 1),
		strings.Replace(string(data), `"version":1`, `"version":null`, 1),
		strings.Replace(string(data), `"version":1`, `"Version":1`, 1),
		strings.Replace(string(data), `"version":1`, `"version":1,"unknown":true`, 1),
		strings.Replace(string(data), `"type":"demo"`, `"type":"Bad Type"`, 1),
		strings.Replace(string(data), `"type":"demo"`, `"type":null`, 1),
		strings.Replace(string(data), `"type":"demo"`, `"type":"demo","unknown":0`, 1),
		strings.Replace(string(data), entry.Fingerprint, strings.Repeat("A", 64), 1),
		strings.Replace(string(data), entry.Description, strings.Repeat("x", 513), 1),
		strings.Replace(string(data), `"description":"说明"`, `"description":null`, 1),
		strings.Replace(string(data), `"description":"说明"`, `"Description":"说明"`, 1),
		strings.Replace(string(data), `"entries":[`, `"error":"full","entries":[`, 1),
		fmt.Sprintf(`{"version":1,"query_id":%q,"runtime_id":%q,"error":"other"}`, id, runtimeID),
		fmt.Sprintf(`{"version":1,"query_id":%q,"runtime_id":%q,"error":"full","truncated":false}`, id, runtimeID),
		fmt.Sprintf(`{"version":1,"query_id":%q,"runtime_id":%q,"entries":null}`, id, runtimeID),
	} {
		if _, queryID, err := decodeDiscoveryReply([]byte(bad)); !errors.Is(err, ErrProtocol) || queryID != id {
			t.Fatalf("bad reply did not preserve safe correlation: %s, %q, %v", bad, queryID, err)
		}
	}

	// Ambiguous syntax or an invalid QueryID cannot be routed to a live query.
	for _, bad := range []string{
		`null`, `{}`, `[]`, string(data) + `{}`,
		strings.Replace(string(data), `"version":1`, `"version":1,"version":1`, 1),
		strings.Replace(string(data), id, "bad", 1),
		string(data) + strings.Repeat(" ", maxDiscoverySnapshotBytes),
	} {
		if _, queryID, err := decodeDiscoveryReply([]byte(bad)); !errors.Is(err, ErrProtocol) || queryID != "" {
			t.Fatalf("ambiguous reply correlated: %q, %q, %v", bad, queryID, err)
		}
	}

	for _, cause := range []error{nil, actorx.ErrFull, ErrOwnerUnavailable} {
		encoded, err := encodeDiscoveryReply(id, discoverySnapshot{RuntimeID: runtimeID}, cause)
		if err != nil {
			t.Fatal(err)
		}
		frame, _, err := decodeDiscoveryReply(encoded)
		if err != nil || len(frame.Entries) != 0 || frame.Truncated ||
			(cause == nil && frame.Error != "") || (cause == actorx.ErrFull && frame.Error != "full") ||
			(cause == ErrOwnerUnavailable && frame.Error != "unavailable") {
			t.Fatal(frame, err)
		}
	}

	// A missing optional description decodes as the empty string.
	entry.Description = ""
	encoded, _ := json.Marshal(discoveryReplyFrame{Version: 1, QueryID: id, RuntimeID: runtimeID, Entries: []discoveryEntry{entry}})
	if frame, _, err := decodeDiscoveryReply(encoded); err != nil || frame.Entries[0].Description != "" {
		t.Fatal(frame, err)
	}
}

func TestDiscoveryReplyLimits(t *testing.T) {
	entries := discoveryEntries(maxDiscoveryEntries, strings.Repeat("\x00", 512))
	snapshot := discoverySnapshot{RuntimeID: token(t), Entries: entries}
	data, err := encodeDiscoveryReply(token(t), snapshot, nil)
	frame, _, decodeErr := decodeDiscoveryReply(data)
	if err != nil || decodeErr != nil || len(data) > maxDiscoverySnapshotBytes || !frame.Truncated ||
		len(frame.Entries) == 0 || len(frame.Entries) >= len(entries) || frame.Entries[0] != entries[0] {
		t.Fatal(len(data), len(frame.Entries), err, decodeErr)
	}

	for _, entries := range [][]discoveryEntry{
		append(snapshot.Entries, discoveryEntry{}),
		{snapshot.Entries[0], snapshot.Entries[0]},
		{{Type: "demo", Fingerprint: strings.Repeat("a", 64), Description: "\xff"}},
	} {
		snapshot.Entries = entries
		if _, err := encodeDiscoveryReply(token(t), snapshot, nil); !errors.Is(err, ErrProtocol) {
			t.Fatal("invalid snapshot encoded", err)
		}

		// Peer-originated violations must be rejected too, not just local ones.
		data, _ := json.Marshal(discoveryReplyFrame{Version: 1, QueryID: token(t), RuntimeID: snapshot.RuntimeID, Entries: entries})
		if entries[0].Description != "\xff" { // json.Marshal replaces invalid UTF-8.
			if _, _, err := decodeDiscoveryReply(data); !errors.Is(err, ErrProtocol) {
				t.Fatal("invalid peer snapshot accepted", err)
			}
		}
	}
}

func TestDiscoveryReplyExactByteBoundary(t *testing.T) {
	id, runtimeID := token(t), token(t)
	entries := discoveryEntries(maxDiscoveryEntries, strings.Repeat("x", 100))
	frame := discoveryReplyFrame{Version: 1, QueryID: id, RuntimeID: runtimeID, Entries: entries}
	base, _ := json.Marshal(frame)
	gap := maxDiscoverySnapshotBytes - len(base)
	if gap < 0 {
		t.Fatal("fixture already exceeds the byte limit")
	}
	for i := range entries {
		extra := min(gap, 512-len(entries[i].Description))
		entries[i].Description += strings.Repeat("x", extra)
		gap -= extra
	}
	if gap != 0 {
		t.Fatal("fixture could not fill the byte limit", gap)
	}
	snapshot := discoverySnapshot{RuntimeID: runtimeID, Entries: entries}
	data, err := encodeDiscoveryReply(id, snapshot, nil)
	got, _, decodeErr := decodeDiscoveryReply(data)
	if err != nil || decodeErr != nil || len(data) != maxDiscoverySnapshotBytes || got.Truncated ||
		len(got.Entries) != len(entries) {
		t.Fatal("exact boundary was truncated", len(data), got.Truncated, err, decodeErr)
	}

	// One extra byte must remove one whole entry and set Truncated.
	for i := range entries {
		if len(entries[i].Description) < 512 {
			entries[i].Description += "x"
			break
		}
	}
	data, err = encodeDiscoveryReply(id, snapshot, nil)
	got, _, decodeErr = decodeDiscoveryReply(data)
	if err != nil || decodeErr != nil || len(data) > maxDiscoverySnapshotBytes || !got.Truncated ||
		len(got.Entries) != len(entries)-1 {
		t.Fatal("one byte overflow did not truncate a complete entry", len(data), got.Truncated, err, decodeErr)
	}
}
