package errwire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	dist "github.com/go-aie/actorx-dist/internal/fault"
)

func TestDownstreamClassifications(t *testing.T) {
	original := errors.Join(dist.ErrOutcomeUnknown, context.DeadlineExceeded)
	data, err := Encode(original, Source{Operation: "call", Type: "c", ID: "42"})
	if err != nil {
		t.Fatal(err)
	}
	remote, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	data, err = Encode(fmt.Errorf("B calling C: %w", remote), Source{Operation: "call", Type: "b", ID: "42"})
	if err != nil {
		t.Fatal(err)
	}
	remote, err = Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(remote, dist.ErrOutcomeUnknown) || !errors.Is(remote, context.DeadlineExceeded) {
		t.Fatal(remote)
	}
	if errors.Is(remote, dist.ErrActivationUnknown) {
		t.Fatal("invented classification")
	}
	sources := remote.(*Remote).Sources()
	if len(sources) != 2 || sources[1].Type != "c" {
		t.Fatal(sources)
	}
	sources[1].Type = "mutated"
	if remote.(*Remote).Sources()[1].Type != "c" {
		t.Fatal("mutable diagnostics")
	}
}

func TestBoundsAndUnknownErrors(t *testing.T) {
	var err error = errors.New(strings.Repeat("订单", 4096))
	for range 20 {
		data, encodeErr := Encode(err, Source{Operation: "call", ID: strings.Repeat("x", 1024)})
		if encodeErr != nil || len(data) > MaxBytes {
			t.Fatalf("%d %v", len(data), encodeErr)
		}
		var e Envelope
		if json.Unmarshal(data, &e) != nil || !e.Truncated || len(e.Sources) > MaxSources {
			t.Fatal("unbounded diagnostics")
		}
		err, encodeErr = Decode(data)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("text classified")
		}
	}
	for _, data := range []string{
		`{"version":1,"version":2,"codes":[],"message":"","sources":[]}`,
		`{"version":2,"codes":[],"message":"","sources":[]}`,
		`{"version":1,"codes":["unknown"],"message":"","sources":[]}`,
		`{"version":1,"codes":["deadline","deadline"],"message":"","sources":[]}`,
		`{"version":1,"codes":[],"message":"","sources":[],"extra":1}`,
		`{"version":1,"codes":[],"message":"","sources":[]} {}`,
	} {
		if _, decodeErr := Decode([]byte(data)); decodeErr == nil {
			t.Fatalf("accepted %s", data)
		}
	}
}

func TestSizingBaseline(t *testing.T) {
	joined := make([]error, len(classes))
	for i, c := range classes {
		joined[i] = c.err
	}
	var err error = errors.Join(joined...)
	for range MaxSources {
		data, encodeErr := Encode(fmt.Errorf("%s: %w", strings.Repeat("\x00", 512), err), Source{
			Operation:  strings.Repeat("\x00", 32),
			Type:       strings.Repeat("x", 64),
			ID:         strings.Repeat("\x00", 128),
			Generation: strings.Repeat("x", 22),
		})
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		err, encodeErr = Decode(data)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		if len(err.(*Remote).envelope.Codes) != len(classes) {
			t.Fatal("classification lost")
		}
		t.Logf("error_bytes=%d limit=%d sources=%d", len(data), MaxBytes, len(err.(*Remote).Sources()))
	}
}

func TestEscapeExpansionPreservesClasses(t *testing.T) {
	joined := make([]error, len(classes))
	for i, c := range classes {
		joined[i] = c.err
	}
	var err error = errors.Join(joined...)
	for range 8 {
		data, encodeErr := Encode(fmt.Errorf("%s: %w", strings.Repeat("\x00", 512), err), Source{
			Operation:  strings.Repeat("\x00", 32),
			Type:       strings.Repeat("\x00", 64),
			ID:         strings.Repeat("\x00", 128),
			Generation: strings.Repeat("\x00", 22),
		})
		if encodeErr != nil || len(data) > MaxBytes {
			t.Fatalf("escaped error: %d %v", len(data), encodeErr)
		}
		err, encodeErr = Decode(data)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		for _, c := range classes {
			if !errors.Is(err, c.err) {
				t.Fatalf("lost %s", c.code)
			}
		}
	}
}
