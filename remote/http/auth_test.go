package remotehttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-aie/actorx-dist/remote"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

const testKey = "test-credential-for-http-adapter-tests"

// unreadBody proves rejected headers are checked before any RPC body read.
type unreadBody struct{ t *testing.T }

func (b unreadBody) Read([]byte) (int, error) {
	b.t.Fatal("rejected request body read")
	return 0, io.EOF
}

func (unreadBody) Close() error { return nil }

func TestAuthentication(t *testing.T) {
	g := &remote.Gateway{}
	if _, err := NewHandler(nil, Options{APIKey: testKey}); err == nil {
		t.Fatal("nil gateway accepted")
	}
	for _, key := range []string{"", " ", "a\nb", "a,b", "a=b"} {
		if _, err := NewHandler(g, Options{APIKey: key}); err == nil {
			t.Fatal("invalid key accepted")
		}
	}
	h, err := NewHandler(g, Options{APIKey: testKey})
	if err != nil {
		t.Fatal(err)
	}

	for _, auth := range [][]string{
		nil,
		{"Bearer wrong"},
		{"Basic " + testKey},
		{"Bearer " + testKey, "Bearer " + testKey},
		{"Bearer " + testKey + ", Bearer " + testKey},
		{"Bearer\t" + testKey},
		{"Bearer " + testKey + " "},
	} {
		for _, body := range []string{
			`{"jsonrpc":"2.0","id":1,"method":"runtime/register","params":{"type":"demo.ping"}}`,
			`{"jsonrpc":"2.0","id":1,"method":"actor/call"}`,
			`{"jsonrpc":"2.0","id":"pending","result":null}`,
		} {
			r := httptest.NewRequest(http.MethodPost, "/runtime/rpc", strings.NewReader(body))
			r.Body = unreadBody{t}
			r.Header["Authorization"] = auth
			r.Header.Set(SessionHeader, "session")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			challenge := `Bearer realm="actorx-remote"`
			if len(auth) != 0 {
				challenge += `, error="invalid_token"`
			}
			if w.Code != 401 || w.Header().Get("WWW-Authenticate") != challenge || strings.Contains(w.Body.String(), testKey) {
				t.Fatal(w.Code, w.Header(), w.Body.String())
			}
		}
	}

	for _, auth := range []string{"Bearer " + testKey, "bearer   " + testKey} {
		r := httptest.NewRequest(http.MethodPost, "/runtime/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"runtime/actors"}`))
		r.Header.Set("Authorization", auth)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set(ProtocolHeader, protocol.Version)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
