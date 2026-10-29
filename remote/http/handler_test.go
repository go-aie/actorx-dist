package remotehttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-aie/actorx-dist/remote"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

func TestRegisterRejectsCapabilitiesBeforeInstallation(t *testing.T) {
	// This gateway has no installation backend: an accepted registration would
	// fail here, rather than silently installing a retired capability variant.
	g := &remote.Gateway{}
	h, err := NewHandler(g, Options{APIKey: testKey})
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range []string{`{"stream":true}`, `{"stream":false}`, `null`, `false`, `{}`} {
		t.Run(capability, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/runtime/rpc", strings.NewReader(
				`{"jsonrpc":"2.0","id":"register","method":"runtime/register","params":{"type":"demo.ping","capabilities":`+capability+`}}`,
			))
			r.Header.Set("Authorization", "Bearer "+testKey)
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set(ProtocolHeader, protocol.Version)
			r.Header.Set("Accept", "text/event-stream, application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			m, e := protocol.ParseMessage(w.Body.Bytes())
			if w.Code != http.StatusOK || e != nil || m.Error == nil || m.Error.Code != protocol.InvalidParams {
				t.Fatalf("status %d, body %s, error %v", w.Code, w.Body.String(), e)
			}
		})
	}
}

func TestHTTPRequests(t *testing.T) {
	g := &remote.Gateway{}
	h, err := NewHandler(g, Options{APIKey: testKey})
	if err != nil {
		t.Fatal(err)
	}

	// Keep HTTP rejection status separate from errors in a valid JSON-RPC reply.
	for _, tc := range []struct {
		name, body string
		status     int
		code       int
		headers    http.Header
	}{
		{"invalid directory scope", `{"jsonrpc":"2.0","id":9007199254740993,"method":"runtime/actors","params":{"scope":"all"}}`, 200, protocol.InvalidParams, nil},
		{"parse", `{`, 200, protocol.ParseError, nil},
		{"invalid", `false`, 200, protocol.InvalidRequest, nil},
		{"unknown", `{"jsonrpc":"2.0","id":null,"method":"actor/unsupported"}`, 200, protocol.MethodNotFound, nil},
		{
			"params",
			`{"jsonrpc":"2.0","id":1,"method":"runtime/actors","params":[]}`,
			200,
			protocol.InvalidParams,
			nil,
		},
		{
			"register mode",
			`{"jsonrpc":"2.0","id":1,"method":"runtime/register","params":{"type":"demo.ping"}}`,
			200,
			protocol.TransportMode,
			nil,
		},
		{
			"register notification",
			`{"jsonrpc":"2.0","method":"runtime/register","params":{"type":"demo.ping"}}`,
			204,
			0,
			nil,
		},
		{"notification", `{"jsonrpc":"2.0","method":"runtime/actors"}`, 204, 0, nil},
		{"all notifications", `[{"jsonrpc":"2.0","method":"unknown"}]`, 204, 0, nil},
		{
			"SSE notification",
			`{"jsonrpc":"2.0","method":"runtime/register","params":{"type":"demo.ping"}}`,
			204,
			0,
			http.Header{"Accept": {"text/event-stream"}},
		},
		{
			"SSE notification batch",
			`[{"jsonrpc":"2.0","method":"runtime/register","params":{"type":"demo.ping"}}]`,
			204,
			0,
			http.Header{"Accept": {"text/event-stream"}},
		},
		{"empty batch", `[]`, 200, protocol.InvalidRequest, nil},
		{
			"no Actor",
			`{"jsonrpc":"2.0","id":1,"method":"actor/call","params":{"address":{"version":1,"type":"demo.ping","id":"one"},"input":null}}`,
			200,
			protocol.ActorUnavailable,
			nil,
		},
		{
			"expired",
			`{"jsonrpc":"2.0","id":1,"method":"actor/call","params":{"address":{"version":1,"type":"demo.ping","id":"one"},"input":null,"deadline_unix_ms":0}}`,
			200,
			protocol.DeadlineExceeded,
			nil,
		},
		{"no session", `{"jsonrpc":"2.0","id":1,"result":null}`, 403, 0, nil},
		{"response batch", `[{"jsonrpc":"2.0","id":1,"result":null}]`, 400, 0, nil},
		{"wrong session", `{"jsonrpc":"2.0","id":1,"result":null}`, 403, 0, http.Header{SessionHeader: {"wrong"}}},
		{
			"session request",
			`{"jsonrpc":"2.0","id":1,"method":"runtime/actors"}`,
			400,
			0,
			http.Header{SessionHeader: {"wrong"}},
		},
		{"session malformed", `{`, 400, 0, http.Header{SessionHeader: {"wrong"}}},
		{"session batch", `[{"jsonrpc":"2.0","id":1,"result":null}]`, 400, 0, http.Header{SessionHeader: {"wrong"}}},
		{"version", `{}`, 400, 0, http.Header{ProtocolHeader: {"unsupported"}}},
		{"duplicate version", `{}`, 400, 0, http.Header{ProtocolHeader: {protocol.Version, protocol.Version}}},
		{"duplicate session", `{}`, 400, 0, http.Header{SessionHeader: {"one", "two"}}},
		{"content type", `{}`, 415, 0, http.Header{"Content-Type": {"text/plain"}}},
		{"accept", `{}`, 406, 0, http.Header{"Accept": {"text/html"}}},
		{"accept zero", `{}`, 406, 0, http.Header{"Accept": {"*/*;q=1, application/json;q=0, text/event-stream;q=0"}}},
		{"limit", strings.Repeat(" ", protocol.MaxMessageBytes+1), 413, 0, nil},
		{"batch limit", `[` + strings.Repeat(`{"jsonrpc":"2.0","method":"unknown"},`, 8) + `{}]`, 413, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/runtime/rpc", strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer "+testKey)
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set(ProtocolHeader, protocol.Version)
			for k, v := range tc.headers {
				r.Header[k] = v
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
			if tc.code != 0 {
				m, e := protocol.ParseMessage(w.Body.Bytes())
				if e != nil || m.Error == nil || m.Error.Code != tc.code {
					t.Fatal(w.Body.String(), e)
				}
			}
			if tc.name == "invalid directory scope" && !strings.Contains(w.Body.String(), `"id":9007199254740993`) {
				t.Fatal("id lost precision", w.Body.String())
			}
		})
	}
	// A mixed batch answers requests and invalid elements, suppresses notification
	// replies and never installs the attempted provider registration.
	r := httptest.NewRequest(
		"POST", "/runtime/rpc",
		strings.NewReader(`[{"jsonrpc":"2.0","method":"runtime/actors"},{"jsonrpc":"2.0","id":"r","method":"runtime/register","params":{"type":"demo.ping"}},{"jsonrpc":"2.0","id":"s","method":"runtime/actors"},1]`),
	)
	r.Header.Set("Authorization", "Bearer "+testKey)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(ProtocolHeader, protocol.Version)
	r.Header.Set("Accept", "text/event-stream, application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var batch []protocol.Message
	if json.Unmarshal(w.Body.Bytes(), &batch) != nil ||
		len(batch) != 3 ||
		batch[0].Error.Code != protocol.TransportMode ||
		batch[2].Error.Code != protocol.InvalidRequest {
		t.Fatal(w.Body.String())
	}

	for _, tc := range []struct {
		method, path string
		status       int
	}{{"GET", "/runtime/rpc", 405}, {"POST", "/other", 404}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.status {
			t.Fatal(w.Code)
		}
	}
}

// Rejection happens before body reads, so no request kind can register, activate,
// deliver provider evidence or alter consumer credit/cancellation state.
func TestProtocolVersionRequiredBeforeDispatch(t *testing.T) {
	h, err := NewHandler(&remote.Gateway{}, Options{APIKey: testKey})
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range [][]string{nil, {""}, {"actorx.remote/unsupported"}, {protocol.Version + ", " + protocol.Version}, {protocol.Version, protocol.Version}} {
		for _, session := range []string{"", "provider-session"} {
			r := httptest.NewRequest(http.MethodPost, "/runtime/rpc", nil)
			r.Body = unreadBody{t}
			r.Header.Set("Authorization", "Bearer "+testKey)
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Accept", "text/event-stream, application/json")
			if session != "" {
				r.Header.Set(SessionHeader, session)
			}
			r.Header[ProtocolHeader] = version
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("version %q, session %q: %d %s", version, session, w.Code, w.Body.String())
			}
		}
	}
}
