package recovery

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/go-aie/actorx"
)

func TestNamespaceRejectedBeforeIO(t *testing.T) {
	// A nil connection proves namespace errors cannot reach the SDK.
	if _, err := Inspect(context.Background(), nil, "bad.namespace", Key{"counter", "one"}); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}
	var audit bytes.Buffer
	if err := Apply(context.Background(), nil, "other", Snapshot{Namespace: "original"}, true, "fenced", &audit); err == nil || audit.Len() != 0 {
		t.Fatal("cross-namespace recovery was not rejected before I/O", err, audit.String())
	}
}
