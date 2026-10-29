package wire

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestModuleVectors(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors map[string]json.RawMessage
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	for name, raw := range vectors {
		t.Run(name, func(t *testing.T) {
			value, err := Decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := Encode(value)
			if err != nil {
				t.Fatal(err)
			}
			var before, after any
			_ = json.Unmarshal(raw, &before)
			_ = json.Unmarshal(encoded, &after)
			// Compare through re-encoding generic values; numbers in these fixed
			// vectors are small, while the boundary vectors below retain raw IDs.
			b, _ := json.Marshal(before)
			a, _ := json.Marshal(after)
			if !bytes.Equal(a, b) {
				t.Fatal("round trip changed vector")
			}
			if _, exists := value.(map[string]any)["jsonrpc"]; exists {
				m, err := Parse(raw)
				if err != nil {
					t.Fatal(err)
				}
				if name == "provider_call" || name == "provider_stream" {
					if _, err := ProviderRequestSequence(m.ID); err != nil {
						t.Fatal(err)
					}
				}
				if m.Error != nil && ValidateError(m.Error) != nil {
					t.Fatal("invalid vector error")
				}
			}
		})
	}
	m, err := Response(vectors["call_unsupported"], "c:1")
	if err != nil || m.Error.Code != 1013 {
		t.Fatalf("unsupported vector: %v", err)
	}
	var data map[string]json.RawMessage
	_ = json.Unmarshal(m.Error.Data, &data)
	a, err := ParseAddress(data["address"], true)
	if err != nil || a.Generation != "AAAAAAAAAAAAAAAAAAAAAA" {
		t.Fatalf("address vector: %v", err)
	}

	raw, err = os.ReadFile("../../testdata/json-boundaries.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases struct{ Valid, Invalid []string }
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, raw := range cases.Valid {
		m, err := Parse([]byte(raw))
		if err != nil {
			t.Fatalf("valid %s: %v", raw, err)
		}
		encoded, err := Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		if m.ID != nil && !bytes.Contains(encoded, append([]byte(`"id":`), m.ID...)) {
			t.Fatal("ID precision changed")
		}
	}
	for _, raw := range cases.Invalid {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestJSONBoundsAndPrecision(t *testing.T) {
	value, err := Decode([]byte(`{"n":900719925474099312345,"f":1.234567890123456789}`))
	if err != nil {
		t.Fatal(err)
	}
	m := value.(map[string]any)
	if m["n"] != json.Number("900719925474099312345") || m["f"] != json.Number("1.234567890123456789") {
		t.Fatal(value)
	}
	for _, raw := range []string{
		`"\ud800"`,
		`"\udc00"`,
		`"\ud800x"`,
		`"\ud800\u0041"`,
		string([]byte{0xff}),
		`{"a":1,"\u0061":2}`,
		strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65),
		strings.Repeat(" ", MaxBytes+1),
	} {
		if Validate([]byte(raw)) == nil {
			t.Fatalf("accepted invalid JSON %.100s", raw)
		}
	}
	for _, raw := range []string{
		`"\ud83d\ude00"`,
		`"\\ud800"`,
		strings.Repeat("[", 64) + "0" + strings.Repeat("]", 64),
		`"` + strings.Repeat("x", MaxBytes-2) + `"`,
	} {
		if err := Validate([]byte(raw)); err != nil {
			t.Fatalf("rejected valid JSON: %v", err)
		}
	}
}

func TestAddressAndFields(t *testing.T) {
	base := `{"version":1,"type":"demo.ping","id":"session-42","generation":"AAAAAAAAAAAAAAAAAAAAAA"}`
	for _, replace := range [][2]string{
		{`"version":1`, `"version":null`},
		{`"version":1`, `"version":true`},
		{`"demo.ping"`, `"Invalid"`},
		{`"session-42"`, `""`},
		{`"session-42"`, `"\u0000"`},
		{`"AAAAAAAAAAAAAAAAAAAAAA"`, `null`},
		{`"AAAAAAAAAAAAAAAAAAAAAA"`, `""`},
		{`"AAAAAAAAAAAAAAAAAAAAAA"`, `"aaaaaaaaaaaaaaaaaaaaaa"`},
		{`"AAAAAAAAAAAAAAAAAAAAAA"`, `"AAAAAAAAAAAAAAAAAAAAAA=="`},
		{`"version":1`, `"version":1,"extra":true`},
	} {
		if _, err := ParseAddress([]byte(strings.Replace(base, replace[0], replace[1], 1)), true); err == nil {
			t.Fatal(replace)
		}
	}
	for _, raw := range []string{`{}`, `{"value":null}`, `{"address":null,"value":null,"extra":true}`} {
		if _, err := Fields([]byte(raw), []string{"address", "value"}, "address", "value"); err == nil {
			t.Fatal(raw)
		}
	}
	if _, err := Fields([]byte(`{"address":null,"value":null}`), []string{"address", "value"}, "address", "value"); err != nil {
		t.Fatal(err)
	}
}

func TestErrorsCorrelationAndExtensions(t *testing.T) {
	for code := 1000; code <= 1013; code++ {
		e := &RPCError{Code: code, Data: json.RawMessage(`{"kind":"` + Kind(code) + `"}`)}
		if ValidateError(e) != nil {
			t.Fatal(code)
		}
		e.Data = json.RawMessage(`{"kind":null}`)
		if ValidateError(e) == nil {
			t.Fatal(code)
		}
	}
	if Kind(123456) != "unknown" || ValidateError(&RPCError{Code: 123456, Data: json.RawMessage(`{"kind":false}`)}) != nil {
		t.Fatal("unknown code rejected")
	}
	for _, id := range []string{`null`, `1`, `true`, `"wrong"`} {
		if _, err := Response([]byte(`{"jsonrpc":"2.0","id":`+id+`,"result":null}`), "expected"); err == nil {
			t.Fatal(id)
		}
	}
	// Extensions must not override exact error keys through Go's case folding.
	m, err := Response([]byte(`{"jsonrpc":"2.0","id":"x","error":{"code":1001,"Code":"extension","message":"x","Message":null,"data":null,"Data":123}}`), "x")
	if err != nil || m.Error.Code != 1001 || string(m.Error.Data) != "null" {
		t.Fatalf("extension handling: %+v %v", m, err)
	}
}
