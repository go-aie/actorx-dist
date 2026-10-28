package actorxremote

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

var bearerKey = regexp.MustCompile(`^[A-Za-z0-9._~+/-]+=*$`)

func validateOptions(origin, key string) (string, error) {
	u, err := url.Parse(origin)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") ||
		u.Hostname() == "" || u.User != nil || u.Opaque != "" ||
		u.RawQuery != "" || u.ForceQuery || strings.Contains(origin, "#") ||
		(u.Path != "" && u.Path != "/") || u.RawPath != "" || !utf8.ValidString(origin) {
		return "", fmt.Errorf("actorxremote: expected gateway origin without credentials")
	}
	if !bearerKey.MatchString(key) {
		return "", fmt.Errorf("actorxremote: valid API key required")
	}
	if port := u.Port(); port != "" {
		if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			return "", fmt.Errorf("actorxremote: invalid gateway port")
		}
	}
	u.Path = "/runtime/rpc"
	return u.String(), nil
}

// post is shared by consumer operations, registration SSE and provider replies.
// GetBody stays nil even for byte-backed bodies, preventing the standard
// transport from retrying a POST on a stale keep-alive connection.
func (r *Runtime) post(ctx context.Context, raw []byte, sse bool, session string) (*http.Response, error) {
	var connected atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) { connected.Store(true) },
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(raw))
	if err != nil {
		return nil, transportError(err)
	}

	req.GetBody = nil
	req.Header.Set("Authorization", "Bearer "+r.key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Actorx-Remote-Protocol", wire.Version)
	req.Header.Set("Accept", "application/json")
	if sse {
		req.Header.Set("Accept", "text/event-stream, application/json")
	}
	if session != "" {
		req.Header.Set("X-Actorx-Remote-Session", session)
	}

	response, err := r.http.Do(req)
	// With our owned standard transport, no GotConn means DNS/dial/TLS (or
	// waiting for a connection) failed before a request could be dispatched.
	if err != nil && r.http.Transport == r.transport && !connected.Load() {
		return nil, errConnection
	}
	return response, err
}

func statusError(status int) error {
	code := "transport"
	if status == 401 {
		code = "authentication"
	}
	if status == 403 {
		code = "session"
	}
	return &TransportError{Code: code, Status: status}
}

func mediaType(h http.Header) string {
	t, _, err := mime.ParseMediaType(h.Get("Content-Type"))
	if err != nil {
		return ""
	}
	return strings.ToLower(t)
}

func readBody(body io.Reader) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(body, wire.MaxBytes+1))
	if err != nil {
		return nil, transportError(err)
	}
	if len(raw) > wire.MaxBytes {
		return nil, protocolError()
	}
	return raw, nil
}

func connectionFailed(err error) bool {
	var op *net.OpError
	var dns *net.DNSError
	return errors.Is(err, errConnection) ||
		(errors.As(err, &op) && op.Op == "dial") || errors.As(err, &dns)
}

var errConnection = errors.New("gateway connection not established")

// sseReader accepts the protocol's exact single-line data frames, with a bound
// even when a peer never sends a newline. EOF is always a missing terminal, not
// a successful RPC. The owner closes the underlying body to cancel a blocked read.
type sseReader struct{ reader *bufio.Reader }

func newSSEReader(body io.Reader) *sseReader {
	// A full buffer without LF must already exceed prefix + maximum JSON.
	// Do not wait for one more byte from a stalled over-limit peer.
	return &sseReader{reader: bufio.NewReaderSize(body, wire.MaxBytes+7)}
}

func (s *sseReader) next() ([]byte, error) {
	line, err := s.reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) || len(line) > wire.MaxBytes+7 {
		return nil, protocolError()
	}
	if err != nil {
		return nil, transportError(err)
	}
	if !bytes.HasPrefix(line, []byte("data: ")) || bytes.ContainsAny(line[:len(line)-1], "\r\n") {
		return nil, protocolError()
	}
	end, err := s.reader.ReadByte()
	if err != nil {
		return nil, transportError(err)
	}
	if end != '\n' {
		return nil, protocolError()
	}
	return bytes.Clone(line[6 : len(line)-1]), nil
}
