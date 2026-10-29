package remotehttp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-aie/actorx-dist/remote"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

const (
	ProtocolHeader = "X-Actorx-Remote-Protocol"
	SessionHeader  = "X-Actorx-Remote-Session"

	budget = 5 * time.Second
)

type handler struct {
	gateway *remote.Gateway
	key     [sha256.Size]byte
}

// NewHandler serves POST /runtime/rpc. Construction performs no I/O and takes
// ownership of neither gateway nor its borrowed actorx-dist Runtime.
func NewHandler(gateway *remote.Gateway, options Options) (http.Handler, error) {
	if gateway == nil || !bearerToken(options.APIKey) {
		return nil, fmt.Errorf("remote/http: gateway and valid API key required")
	}
	return &handler{gateway: gateway, key: sha256.Sum256([]byte(options.APIKey))}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	if r.URL.Path != "/runtime/rpc" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	// Authentication precedes body reads, session lookup and any Actor work.
	if !h.authenticate(w, r) {
		return
	}

	version := r.Header.Values(ProtocolHeader)
	sessions := r.Header.Values(SessionHeader)
	if len(version) != 1 || version[0] != protocol.Version ||
		len(sessions) > 1 || len(sessions) == 1 && (sessions[0] == "" || strings.ContainsAny(sessions[0], ", \t")) {
		http.Error(w, "Invalid protocol or session header", http.StatusBadRequest)
		return
	}

	cts := r.Header.Values("Content-Type")
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if len(cts) != 1 || err != nil || mediaType != "application/json" {
		http.Error(w, "JSON required", http.StatusUnsupportedMediaType)
		return
	}

	jsonOK := accepts(r.Header, "application/json")
	sseOK := len(r.Header.Values("Accept")) > 0 && accepts(r.Header, "text/event-stream")
	if !jsonOK && !sseOK {
		http.Error(w, "Unsupported response format", http.StatusNotAcceptable)
		return
	}

	// Bound body reading as well as dispatch. Unsupported controllers (e.g.
	// middleware without Unwrap) cannot serve SSE, but may serve ordinary RPC.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(started.Add(budget))
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, protocol.MaxMessageBytes))
	_ = rc.SetReadDeadline(time.Time{})
	if err != nil {
		var tooLarge *http.MaxBytesError
		status := http.StatusBadRequest
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, "Cannot read RPC body", status)
		return
	}
	original := raw
	raw = bytes.TrimSpace(raw)
	m, rpcErr := protocol.ParseMessage(raw)
	ctx, cancel := context.WithDeadline(r.Context(), started.Add(budget))
	defer cancel()

	// Provider response POSTs are correlated independently from RPC requests.
	if len(sessions) != 0 {
		if rpcErr != nil || m.IsRequest() && (!m.IsNotification() || m.Method != protocol.ChunkMethod) {
			http.Error(w, "Expected one provider response", http.StatusBadRequest)
			return
		}
		h.accept(w, ctx, sessions[0], original)
		return
	}
	if rpcErr == nil && m.IsNotification() && (m.Method == protocol.Credit || m.Method == protocol.Cancel) {
		h.control(w, ctx, m)
		return
	}
	if rpcErr == nil && m.Method == protocol.ChunkMethod {
		http.Error(w, "Provider session required", http.StatusForbidden)
		return
	}
	if rpcErr == nil && !m.IsRequest() {
		http.Error(w, "Provider session required", http.StatusForbidden)
		return
	}

	// A provider response cannot be hidden in a batch. Reject the whole
	// transport document before any request in that batch can have side effects.
	onlyNotifications := rpcErr == nil && m.IsNotification()
	if len(raw) > 0 && raw[0] == '[' && protocol.ValidateJSON(raw) == nil {
		var items []json.RawMessage
		_ = json.Unmarshal(raw, &items)
		onlyNotifications = len(items) > 0 && len(items) <= protocol.MaxBatchItems
		for _, item := range items {
			itemMessage, e := protocol.ParseMessage(item)
			if e == nil && !itemMessage.IsRequest() {
				http.Error(w, "Provider responses require a single POST", http.StatusBadRequest)
				return
			}
			onlyNotifications = onlyNotifications && e == nil && itemMessage.IsNotification()
		}
	}

	if rpcErr == nil && m.Method == protocol.Register && !m.IsNotification() && sseOK {
		h.register(w, r, ctx, m)
		return
	}
	if rpcErr == nil && m.Method == protocol.Stream && !m.IsNotification() && sseOK {
		streamCtx, stop := context.WithDeadline(r.Context(), started.Add(60*time.Second))
		defer stop()
		h.stream(w, streamCtx, m)
		return
	}
	if !jsonOK && !onlyNotifications {
		http.Error(w, "JSON response required", http.StatusNotAcceptable)
		return
	}

	encoded, err := protocol.ProcessRequests(ctx, raw, h.dispatch)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, protocol.ErrLimit) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, "Invalid RPC document or limit exceeded", status)
		return
	}
	if len(encoded) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(encoded)
}

func (h *handler) dispatch(ctx context.Context, m protocol.Message, _ bool) (any, *protocol.Error) {
	params, err := m.Parameters(protocol.ToGateway)
	if err != nil {
		return nil, err
	}
	switch m.Method {
	case protocol.Actors:
		result, e := h.gateway.Actors(ctx)
		return result, rpcError(e)
	case protocol.Credit, protocol.Cancel, protocol.ChunkMethod:
		return nil, protocol.NewError(protocol.TransportMode, "Control requires a single notification POST", nil)
	case protocol.Call:
		result, callErr := h.gateway.Call(ctx, params.(protocol.CallParams))
		return result, rpcError(callErr)
	default:
		return nil, protocol.NewError(protocol.TransportMode, "Registration requires a single SSE request", nil)
	}
}

func rpcError(err error) *protocol.Error {
	if err == nil {
		return nil
	}
	var e *protocol.Error
	if errors.As(err, &e) {
		return e
	}
	return protocol.NewError(protocol.InternalError, "Internal error", nil)
}

func writeRPC(w http.ResponseWriter, m protocol.Message) {
	raw, err := m.Encode()
	if err != nil {
		http.Error(w, "RPC limit exceeded", http.StatusRequestEntityTooLarge)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}

func (h *handler) accept(w http.ResponseWriter, ctx context.Context, token string, raw []byte) {
	err := h.gateway.AcceptEncoded(ctx, token, raw)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	status := http.StatusBadRequest
	if errors.Is(err, remote.ErrSession) {
		status = http.StatusForbidden
	} else if errors.Is(err, remote.ErrCorrelation) {
		status = http.StatusConflict
	}
	http.Error(w, "Provider response rejected", status)
}

func (h *handler) register(w http.ResponseWriter, r *http.Request, ctx context.Context, m protocol.Message) {
	params, rpcErr := m.Parameters(protocol.ToGateway)
	if rpcErr != nil {
		writeRPC(w, protocol.ErrorResponse(m.ID, rpcErr))
		return
	}
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Now().Add(budget)); err != nil {
		http.Error(w, "SSE write deadlines unavailable", http.StatusInternalServerError)
		return
	}
	defer rc.SetWriteDeadline(time.Time{})

	registration, err := h.gateway.Register(ctx, params.(protocol.RegisterParams))
	if registration != nil {
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			// On timeout the Gateway retains and joins the registration.
			_ = registration.Close(cleanup)
		}()
	}
	if err != nil {
		writeRPC(w, protocol.ErrorResponse(m.ID, rpcError(err)))
		return
	}

	// The install budget ends at Register; each SSE frame gets a fresh write
	// deadline, while the HTTP request context owns the stream's lifetime.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	write := func(message protocol.Message) error {
		raw, err := message.Encode()
		if err != nil {
			return err
		}
		if err = rc.SetWriteDeadline(time.Now().Add(budget)); err != nil {
			return err
		}
		if _, err = fmt.Fprintf(w, "data: %s\n\n", raw); err != nil {
			return err
		}
		if err = rc.Flush(); err != nil {
			return err
		}
		return rc.SetWriteDeadline(time.Time{})
	}

	// Publish registration first, even when create/invoke is already queued.
	if write(protocol.Success(m.ID, registration.Info())) != nil {
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case message, ok := <-registration.Events():
			if !ok || write(message) != nil {
				return
			}
		}
	}
}

// Honor the most specific media range, including explicit q=0 exclusions.
func accepts(header http.Header, target string) bool {
	values := header.Values("Accept")
	if len(values) == 0 {
		return true
	}
	best, quality := -1, 0.0
	major, _, _ := strings.Cut(target, "/")
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			mt, params, err := mime.ParseMediaType(strings.TrimSpace(part))
			if err != nil {
				continue
			}
			specificity := -1
			switch mt {
			case "*/*":
				specificity = 0
			case major + "/*":
				specificity = 1
			case target:
				specificity = 2
			}
			if specificity < 0 {
				continue
			}
			q := 1.0
			if raw, ok := params["q"]; ok {
				q, err = strconv.ParseFloat(raw, 64)
			}
			if err != nil || !(q >= 0 && q <= 1) {
				continue
			}
			if specificity > best || specificity == best && q > quality {
				best, quality = specificity, q
			}
		}
	}
	return quality > 0
}
