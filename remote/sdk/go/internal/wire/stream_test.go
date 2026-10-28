package wire

import (
	"encoding/json"
	"testing"
)

func TestStreamSequencesTokensAndWindows(t *testing.T) {
	for _, raw := range []string{`0`, `1`, `9007199254740991`} {
		if _, err := Sequence(json.RawMessage(raw)); err != nil {
			t.Fatal(raw, err)
		}
	}
	for _, raw := range []string{`null`, `-1`, `1.0`, `1e0`, `true`, `"1"`, `9007199254740992`, `18446744073709551616`, ``} {
		if _, err := Sequence(json.RawMessage(raw)); err == nil {
			t.Fatal(raw)
		}
	}
	for _, raw := range []string{`{"items":4,"bytes":32768}`, `{"bytes":32768,"items":4}`} {
		if CheckWindow(json.RawMessage(raw)) != nil {
			t.Fatal(raw)
		}
	}
	for _, raw := range []string{
		`null`,
		`{"items":4,"bytes":32768,"extra":0}`,
		`{"items":4.0,"bytes":32768}`,
		`{"items":4,"bytes":null}`,
		`{"items":true,"bytes":32768}`,
		`{"items":4}`,
	} {
		if CheckWindow(json.RawMessage(raw)) == nil {
			t.Fatal(raw)
		}
	}
	token := `"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"`
	if _, err := Token(json.RawMessage(token)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`null`,
		`1`,
		`""`,
		`"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="`,
		`"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB"`,
	} {
		if _, err := Token(json.RawMessage(raw)); err == nil {
			t.Fatal(raw)
		}
	}
}

func TestStreamControlPreservesIDTypesAndExactFields(t *testing.T) {
	for _, id := range []string{`null`, `1`, `900719925474099312345`, `"1"`, `"a\\b"`} {
		raw := []byte(`{"jsonrpc":"2.0","method":"actor/credit","params":{"request_id":` + id + `,"stream_token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","ack_seq":9007199254740991}}`)
		m, err := Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		got, _, seq, err := Control(m)
		if err != nil || !SameID(got, json.RawMessage(id)) || seq != MaxSequence {
			t.Fatal(string(got), seq, err)
		}
	}
	if SameID(json.RawMessage(`1`), json.RawMessage(`"1"`)) || !SameID(json.RawMessage(`"\u0061"`), json.RawMessage(`"a"`)) {
		t.Fatal("ID correlation")
	}
	for _, raw := range []string{
		`{"jsonrpc":"2.0","id":null,"method":"actor/cancel","params":{"request_id":null,"stream_token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}`,
		`{"jsonrpc":"2.0","method":"actor/cancel","params":{"request_id":null,"stream_token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","ack_seq":0}}`,
		`{"jsonrpc":"2.0","method":"actor/credit","params":{"request_id":null,"stream_token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}`,
		`{"jsonrpc":"2.0","method":"actor/credit","params":{"stream_token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","ack_seq":0}}`,
	} {
		m, err := Parse([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := Control(m); err == nil {
			t.Fatal(raw)
		}
	}
}
