package actorxdist

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

// Freeze the v1 wire bytes and fingerprint independently of schema generation.
const contractV1JSON = `[1,[["named(\"example.com/contracts\",\"Add\")","int",["send","call"]]],[["int",["scalar","int"]],["named(\"example.com/contracts\",\"Add\")",["struct",[["n","int",false]]]]]]`
const contractV1SHA256 = "96cd13118295f725033a715a3ee47dd8f376c7537c353414eb4191ceb1e7c5ab"

func TestContractFingerprintVector(t *testing.T) {
	digest := sha256.Sum256([]byte(contractV1JSON))
	if hex.EncodeToString(digest[:]) != contractV1SHA256 {
		t.Fatal("contract fingerprint fixture drift")
	}
	messageID := `named("example.com/contracts","Add")`
	vector := []any{
		1,
		[]any{[]any{messageID, "int", []string{"send", "call"}}},
		[]any{[]any{"int", []string{"scalar", "int"}}, []any{messageID, []any{"struct", []any{[]any{"n", "int", false}}}}},
	}
	got, err := json.Marshal(vector)
	if err != nil || string(got) != contractV1JSON {
		t.Fatalf("canonical encoding: %s %v", got, err)
	}
}

type schemaNode struct {
	Value int         `json:"value"`
	Next  *schemaNode `json:"next,omitempty"`
}

type badEmbedded struct{ schemaNode }

type badTag struct {
	N int `json:"n,string"`
}

type customJSON struct{}

func (customJSON) MarshalJSON() ([]byte, error) { panic("custom codec executed") }

type genericDTO[T any] struct{ Value T }

func TestSchemaCanonicalAndRejections(t *testing.T) {
	descriptions := []actorx.CallDescriptor{actorx.Request[add, int](), actorx.Streaming[series, string](), actorx.Message[*schemaNode]()}
	c, err := buildContract(descriptions)
	if err != nil {
		t.Fatal(err)
	}
	descriptions[0], descriptions[2] = descriptions[2], descriptions[0]
	same, err := buildContract(descriptions)
	if err != nil || !reflect.DeepEqual(c, same) {
		t.Fatal("order changed contract", err)
	}

	changed, _ := buildContract(append(descriptions, actorx.Message[read]()))
	if c.fingerprint == changed.fingerprint {
		t.Fatal("new handler did not change contract")
	}

	addr, err := buildContract([]actorx.CallDescriptor{actorx.Message[struct{ Nested []map[string]*Addr }]()})
	if err != nil || !strings.Contains(string(addr.data), `["addr-v1"]`) {
		t.Fatal(err)
	}

	cases := []actorx.CallDescriptor{
		actorx.Message[struct{ V any }](),
		actorx.Message[struct{ F func() }](),
		actorx.Message[struct{ C chan int }](),
		actorx.Message[map[int]int](),
		actorx.Message[badEmbedded](),
		actorx.Message[badTag](),
		actorx.Message[customJSON](),
		actorx.Message[time.Time](),
		actorx.Message[genericDTO[int]](),
		actorx.Message[struct{ A actorx.Addr }](),
		actorx.Message[*Runtime](),
		actorx.Message[uintptr](),
		actorx.Message[struct{ C actorx.Context }](),
		actorx.Message[struct{ C Context }](),
		actorx.Message[struct{ R actorx.Reply[int] }](),
		actorx.Message[struct{ R actorx.StreamReply[int] }](),
	}
	for _, d := range cases {
		if _, err := buildContract([]actorx.CallDescriptor{d}); !errors.Is(err, actorx.ErrSetup) {
			t.Errorf("accepted %v: %v", d.MessageType(), err)
		}
	}

	// Generated schema uses exactly the frozen v1 structure. Only the Go
	// package/type/field identities differ from its example.com DTO.
	generated, err := buildContract([]actorx.CallDescriptor{actorx.Request[add, int]()})
	if err != nil {
		t.Fatal(err)
	}
	expected := strings.ReplaceAll(contractV1JSON, "example.com/contracts", reflect.TypeFor[add]().PkgPath())
	expected = strings.ReplaceAll(expected, `\"Add\"`, `\"add\"`)
	expected = strings.ReplaceAll(expected, `"n"`, `"N"`)
	if string(generated.data) != expected {
		t.Fatalf("canonical vector mismatch: %s\nwant: %s", generated.data, expected)
	}
	var value any
	if json.Unmarshal([]byte(contractV1JSON), &value) != nil {
		t.Fatal("fixture")
	}
	raw, _ := json.Marshal(value)
	if string(raw) != contractV1JSON {
		t.Fatal("canonical bytes drift")
	}

	duplicate := reflect.StructOf([]reflect.StructField{
		{Name: "A", Type: reflect.TypeFor[int](), Tag: `json:"n"`},
		{Name: "B", Type: reflect.TypeFor[int](), Tag: `json:"n"`},
	})
	db := schemaBuilder{ids: map[reflect.Type]string{}, nodes: map[string]any{}, visiting: map[reflect.Type]bool{}}
	if _, err := db.typ(duplicate, 0); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal("accepted duplicate JSON names", err)
	}

	// Bounded traversal rejects excessively large anonymous field identities.
	fields := make([]reflect.StructField, 300)
	for i := range fields {
		fields[i] = reflect.StructField{Name: strings.Repeat("X", i+1), Type: reflect.TypeFor[int]()}
	}
	b := schemaBuilder{ids: map[reflect.Type]string{}, nodes: map[string]any{}, visiting: map[reflect.Type]bool{}}
	if _, err := b.typ(reflect.StructOf(fields), 0); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal("unbounded identity", err)
	}
}
