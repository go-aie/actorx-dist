package actorxdist

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/go-aie/actorx"
)

type codecDTO struct {
	Text     string             `json:"text"`
	Integer  int64              `json:"integer"`
	Unsigned uint64             `json:"unsigned"`
	Bytes    []byte             `json:"bytes"`
	Array    [2]int             `json:"array"`
	Nested   []map[string]*Addr `json:"nested"`
	Next     *codecDTO          `json:"next,omitempty"`
}

type recursiveWirePointer *recursiveWirePointer

func TestCodecRejectsRecursivePointerSchema(t *testing.T) {
	if _, err := buildContract([]actorx.CallDescriptor{actorx.Message[recursiveWirePointer]()}); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}
}

func TestCodecStrictValuesAndNestedAddresses(t *testing.T) {
	sender, receiver := testRuntime(t), testRuntime(t)
	a := mustAddr(t, sender, "counter", "wire")
	_ = a.bind(token(t))
	input := codecDTO{
		Text:     "你好<&\n",
		Integer:  math.MinInt64,
		Unsigned: math.MaxUint64,
		Bytes:    []byte{0, 255},
		Array:    [2]int{1, 2},
		Nested:   []map[string]*Addr{{"self": &a}},
	}
	if _, err := buildContract([]actorx.CallDescriptor{actorx.Message[codecDTO]()}); err != nil {
		t.Fatal(err)
	}
	data, err := encodeValue(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeValue(receiver, data, reflect.TypeFor[codecDTO]())
	if err != nil {
		t.Fatal(string(data), err)
	}
	got := decoded.(codecDTO)
	if got.Text != input.Text ||
		got.Integer != input.Integer ||
		got.Unsigned != input.Unsigned ||
		got.Array != input.Array ||
		!reflect.DeepEqual(got.Bytes, input.Bytes) {
		t.Fatal(got)
	}
	imported := got.Nested[0]["self"]
	if imported.rt != receiver || imported.generation() != a.generation() || imported.Key() != a.Key() {
		t.Fatal("nested address lost receiving owner")
	}

	// Each mutation starts from the valid payload to isolate one rejection rule.
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	for _, mutation := range []func(){
		func() { delete(fields, "text") },
		func() { fields["Text"] = json.RawMessage(`"bad"`) },
		func() { fields["integer"] = json.RawMessage(`1e3`) },
		func() { fields["unsigned"] = json.RawMessage(`18446744073709551616`) },
		func() { fields["array"] = json.RawMessage(`[1]`) },
		func() { fields["bytes"] = json.RawMessage(`"AB=="`) },
		func() { fields["text"] = json.RawMessage(`null`) },
	} {
		_ = json.Unmarshal(data, &fields)
		mutation()
		bad, _ := json.Marshal(fields)
		if _, err := decodeValue(receiver, bad, reflect.TypeFor[codecDTO]()); err == nil {
			t.Fatal(string(bad))
		}
	}

	for _, bad := range []string{`{"n":1,"n":2}`, `{"n":1} {}`, `{"n":1,"N":2}`, `{"n":1.0}`, `{"n":null}`} {
		if _, err := decodeValue(receiver, []byte(bad), reflect.TypeFor[struct {
			N int `json:"n"`
		}]()); err == nil {
			t.Fatal(bad)
		}
	}

	// Encoding must also reject cycles, nonportable values and oversized payloads.
	cyclic := &schemaNode{}
	cyclic.Next = cyclic
	for _, value := range []any{
		cyclic,
		strings.Repeat("x", maxPayload),
		math.Inf(1),
		math.NaN(),
		string([]byte{255}),
		int(math.MaxInt32) + 1,
		uint(math.MaxUint32) + 1,
		make([]int, 8193),
	} {
		if _, err := encodeValue(value); err == nil {
			t.Fatalf("accepted %T", value)
		}
	}
	if _, err := decodeValue(receiver, []byte(`2147483648`), reflect.TypeFor[int]()); err == nil {
		t.Fatal("nonportable int")
	}
	if _, err := encodeValue(strings.Repeat("x", maxPayload)); !errors.Is(err, actorx.ErrChunkSize) {
		t.Fatal(err)
	}
}

func TestCodecAllocationBounds(t *testing.T) {
	huge := reflect.ArrayOf(1<<25, reflect.TypeFor[uint64]())
	if _, err := decodeValue(nil, []byte(`[]`), huge); !errors.Is(err, actorx.ErrChunkSize) {
		t.Fatal("large fixed allocation was not rejected", err)
	}

	// A short encoded slice may still demand excessive decoded object storage.
	typ := reflect.SliceOf(reflect.ArrayOf(8192, reflect.TypeFor[int]()))
	if _, err := decodeValue(nil, []byte(`[[],[],[]]`), typ); !errors.Is(err, actorx.ErrChunkSize) {
		t.Fatal("large dynamic allocation was not rejected", err)
	}
}
