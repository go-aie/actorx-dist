package actorxdist

import (
	"encoding/json"
	"testing"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/errwire"
)

func TestTerminalRequiresCompleteCleanupEvidence(t *testing.T) {
	o := testOwnership(t)
	for _, held := range []bool{false, true} {
		intended := "RELEASED"
		var reason error
		if held {
			intended, reason = "FAILED_HELD", actorx.ErrSetup
		}
		cause, err := errwire.Encode(reason, errwire.Source{})
		if err != nil {
			t.Fatal(err)
		}
		prepared := terminal{
			Version:         1,
			Operation:       token(t),
			Key:             o.Key,
			Generation:      o.Generation,
			Factory:         o.Factory,
			Cause:           cause,
			Evidence:        "local_wait",
			CreatedInstance: true,
			Intended:        intended,
			Status:          "PREPARED",
		}
		prepared.Digest = cleanupDigest(prepared)
		if !validTerminal(prepared) {
			t.Fatal("rejected PREPARED")
		}

		committed := prepared
		committed.Status, committed.Prepared, committed.Disposition = "COMMITTED", 7, intended
		if !held {
			committed.ReleaseOperation, committed.ReleaseRevision = token(t), 9
		}
		if !validTerminal(committed) {
			t.Fatal("rejected COMMITTED")
		}

		for _, tc := range []struct {
			name   string
			mutate func(*terminal)
		}{
			{"unproven preparation", func(v *terminal) { v.Prepared = 0 }},
			{"changed cause", func(v *terminal) { v.Cause = json.RawMessage(`{}`) }},
			{"different identity", func(v *terminal) { v.Generation = token(t) }},
			{"missing wait evidence", func(v *terminal) {
				v.Evidence = "no_instance"
				v.Digest = cleanupDigest(*v)
			}},
			{"missing disposition", func(v *terminal) { v.Disposition = "" }},
			{"incomplete release", func(v *terminal) {
				v.Disposition = "RELEASED"
				v.ReleaseOperation = ""
				v.ReleaseRevision = 9
			}},
		} {
			t.Run(intended+"/"+tc.name, func(t *testing.T) {
				v := committed
				tc.mutate(&v)
				if validTerminal(v) {
					t.Fatal("accepted incomplete or inconsistent evidence")
				}
			})
		}

		prepared.ReleaseRevision = 9
		if validTerminal(prepared) {
			t.Fatal("PREPARED accepted a release receipt")
		}
	}
}
