package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func testOwnership(t *testing.T) ownership {
	t.Helper()

	return ownership{
		Version:     1,
		Operation:   token(t),
		Key:         ActorKey{"counter", "one"},
		Generation:  token(t),
		Factory:     token(t),
		Fingerprint: strings.Repeat("a", 64),
		State:       "STARTING",
	}
}

func TestOwnershipRecordEvidence(t *testing.T) {
	base := testOwnership(t)
	for _, state := range []string{"STARTING", "READY", "STOPPING", "FAILED", "INACTIVE"} {
		t.Run(state, func(t *testing.T) {
			v := base
			v.State = state
			terminal := state == "FAILED" || state == "INACTIVE"
			if terminal {
				v.ExitKey, v.Digest, v.Prepared = "e."+v.Generation, strings.Repeat("b", 64), 7
			}
			if !validOwner(v) {
				t.Fatal("rejected complete evidence")
			}
			if terminal {
				v.Prepared = 0
			} else {
				v.Prepared = 7
			}
			if validOwner(v) {
				t.Fatal("accepted inconsistent terminal evidence")
			}
		})
	}
	for _, mutate := range []func(*ownership){
		func(v *ownership) { v.Version++ },
		func(v *ownership) { v.Key.ID = "" },
		func(v *ownership) { v.Generation = "invalid" },
		func(v *ownership) { v.Factory = "invalid" },
		func(v *ownership) { v.Operation = "invalid" },
		func(v *ownership) { v.Fingerprint = strings.Repeat("A", 64) },
		func(v *ownership) { v.State = "UNKNOWN" },
	} {
		v := base
		mutate(&v)
		if validOwner(v) {
			t.Fatalf("accepted invalid ownership: %+v", v)
		}
	}
}

func TestRegistryRejectsStaleTransitionsBeforeIO(t *testing.T) {
	base := testOwnership(t)

	// A nil registry proves these invalid requests stop before any SDK access.
	var r *registry
	for _, tc := range []struct {
		name   string
		mutate func(*owned, *ownership)
		want   error
	}{
		{"missing revision", func(o *owned, _ *ownership) { o.revision = 0 }, ErrActivationUnknown},
		{"different generation", func(_ *owned, v *ownership) { v.Generation = token(t) }, ErrActivationUnknown},
		{"different factory", func(_ *owned, v *ownership) { v.Factory = token(t) }, ErrActivationUnknown},
		{"different key", func(_ *owned, v *ownership) { v.Key.ID = "other" }, ErrActivationUnknown},
		{"different contract", func(_ *owned, v *ownership) { v.Fingerprint = strings.Repeat("b", 64) }, ErrActivationUnknown},
		{"backwards state", func(o *owned, _ *ownership) { o.value.State = "READY" }, ErrProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old, next := owned{base, 1}, base
			tc.mutate(&old, &next)
			if _, err := r.change(context.Background(), old, next); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRegistryRecordStrictDecoding(t *testing.T) {
	base := testOwnership(t)
	data, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	var got ownership
	if err := decodeRecord(data, &got); err != nil || got != base {
		t.Fatal(got, err)
	}
	for _, data := range []string{
		`{"version":1,"version":1}`, `{"unknown":true}`, `{}`, // Empty records decode but must fail semantic validation.
		`{} {}`, strings.Repeat(" ", 16<<10+1),
	} {
		var got ownership
		err := decodeRecord([]byte(data), &got)
		if err == nil && validOwner(got) {
			t.Fatal("accepted malformed ownership", data)
		}
		if data != "{}" && !errors.Is(err, ErrProtocol) {
			t.Fatalf("strict decoder accepted %q: %v", data, err)
		}
	}
}
