package remotehttp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

// Explicit real HTTP + JetStream gate. It owns only its random Type/IDs and
// Runtime, never the server or shared Registry buckets.
func TestHTTPNATS(t *testing.T) {
	url := os.Getenv("ACTORX_REMOTE_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_REMOTE_NATS_URL to an isolated development JetStream server")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rt, err := actorxdist.NewRuntime(ctx, actorxdist.WithNATS(url))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rt.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	g, err := remote.NewGateway(rt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := g.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	h, err := NewHandler(g, Options{APIKey: testKey})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	client := server.Client() // no short Client.Timeout on the SSE lifetime
	typ := fmt.Sprintf("remote-http-%x", time.Now().UnixNano())
	post := func(ctx context.Context, raw []byte, token, accept, key string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/runtime/rpc", bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(ProtocolHeader, protocol.Version)
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		if token != "" {
			req.Header.Set(SessionHeader, token)
		}
		return client.Do(req)
	}
	encode := func(m protocol.Message) []byte {
		t.Helper()
		raw, err := m.Encode()
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	request := func(method, id string, p any) []byte {
		t.Helper()
		m, err := protocol.NewRequest(method, id, p)
		if err != nil {
			t.Fatal(err)
		}
		return encode(m)
	}
	checkPost := func(raw []byte, token, key string, status int) {
		t.Helper()
		res, err := post(ctx, raw, token, "application/json", key)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		if res.StatusCode != status || status == 204 && len(body) != 0 {
			t.Fatalf("status %d, expected %d: %s", res.StatusCode, status, body)
		}
	}

	// Install a real SSE provider only after proving unauthenticated registration
	// has no directory side effects.
	register := request(protocol.Register, "register", protocol.RegisterParams{Type: typ, Description: "Go test provider"})
	checkPost(register, "", "wrong", 401)
	if len(g.Actors()) != 0 {
		t.Fatal("unauthorized registration published an Actor")
	}
	providerCtx, disconnect := context.WithCancel(ctx)
	defer disconnect()
	stream, err := post(providerCtx, register, "", "text/event-stream, application/json", testKey)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	if stream.StatusCode != 200 || stream.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal("registration did not establish SSE", stream.Status)
	}
	frames := make(chan protocol.Message, 16)
	readDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stream.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			if !strings.HasPrefix(line, "data: ") {
				readDone <- fmt.Errorf("invalid SSE frame")
				return
			}
			m, rpcErr := protocol.ParseMessage([]byte(strings.TrimPrefix(line, "data: ")))
			if rpcErr != nil {
				readDone <- rpcErr
				return
			}
			select {
			case frames <- m:
			case <-providerCtx.Done():
				readDone <- providerCtx.Err()
				return
			}
		}
		readDone <- scanner.Err()
	}()
	next := func(method string) protocol.Message {
		t.Helper()
		select {
		case m := <-frames:
			if m.Method != method {
				t.Fatalf("expected %q, got %q", method, m.Method)
			}
			return m
		case err := <-readDone:
			t.Fatal("SSE ended", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		return protocol.Message{}
	}
	// Start a Call before consuming the registration result. Even with create
	// queued, the first frame must be the successful register response.
	type callOutcome struct {
		message protocol.Message
		err     error
	}
	call := func(address protocol.Address, input string) <-chan callOutcome {
		done := make(chan callOutcome, 1)
		raw := request(protocol.Call, "consumer", protocol.CallParams{Address: address, Input: json.RawMessage(input)})
		go func() {
			res, err := post(ctx, raw, "", "application/json", testKey)
			if err != nil {
				done <- callOutcome{err: err}
				return
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err == nil && res.StatusCode != 200 {
				err = fmt.Errorf("call HTTP status %d", res.StatusCode)
			}
			m, rpcErr := protocol.ParseMessage(body)
			if err == nil && rpcErr != nil {
				err = rpcErr
			}
			done <- callOutcome{m, err}
		}()
		return done
	}
	awaitCall := func(done <-chan callOutcome) protocol.Message {
		t.Helper()
		select {
		case result := <-done:
			if result.err != nil {
				t.Fatal(result.err)
			}
			return result.message
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		return protocol.Message{}
	}
	cold := protocol.Address{Version: 1, Type: typ, ID: "one"}
	first := call(cold, `{"text":"ping"}`)
	registration := next("")
	var info protocol.RegistrationInfo
	if registration.Error != nil ||
		string(registration.ID) != `"register"` ||
		json.Unmarshal(registration.Result, &info) != nil ||
		info.Protocol != protocol.Version ||
		info.Type != typ ||
		info.SessionToken == "" {
		t.Fatal("invalid first registration frame")
	}
	if actors := g.Actors(); len(actors) != 1 || actors[0].Type != typ {
		t.Fatal("missing directory entry", actors)
	}
	directory, err := post(ctx, request(protocol.Actors, "directory", struct{}{}), "", "application/json", testKey)
	if err != nil {
		t.Fatal(err)
	}
	directoryBody, _ := io.ReadAll(directory.Body)
	directory.Body.Close()
	directoryMessage, directoryErr := protocol.ParseMessage(directoryBody)
	var actors protocol.ActorsResult
	if directory.StatusCode != 200 ||
		directoryErr != nil ||
		json.Unmarshal(directoryMessage.Result, &actors) != nil ||
		len(actors.Actors) != 1 ||
		actors.Actors[0].Type != typ {
		t.Fatal("HTTP directory", string(directoryBody))
	}
	// A second registration fails in ordinary JSON, leaving this SSE alive.
	res, err := post(ctx, register, "", "text/event-stream, application/json", testKey)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	conflict, parseErr := protocol.ParseMessage(body)
	if res.StatusCode != 200 ||
		res.Header.Get("Content-Type") != "application/json" ||
		parseErr != nil ||
		conflict.Error == nil ||
		conflict.Error.Code != protocol.RegistrationConflict {
		t.Fatal("registration conflict", res.Status, string(body))
	}
	create := next(protocol.Create)
	params, rpcErr := create.Parameters(protocol.ToProvider)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	fixed := params.(protocol.CreateParams).Address
	if fixed.Generation == "" || fixed.ID != cold.ID || fixed.Type != typ {
		t.Fatal("invalid Self", fixed)
	}
	created := protocol.Success(create.ID, protocol.CreateResult{Address: fixed})
	checkPost(encode(created), "", testKey, 403)
	checkPost(encode(created), "invalid", testKey, 403)
	checkPost(encode(created), info.SessionToken, "wrong", 401)
	checkPost(encode(protocol.Success(json.RawMessage(`"unknown"`), protocol.CreateResult{Address: fixed})), info.SessionToken, testKey, 409)
	mismatch := fixed
	mismatch.ID = "other"
	checkPost(encode(protocol.Success(create.ID, protocol.CreateResult{Address: mismatch})), info.SessionToken, testKey, 409)
	checkPost(encode(protocol.Success(create.ID, struct{}{})), info.SessionToken, testKey, 400)
	checkPost(encode(created), info.SessionToken, testKey, 204)
	checkPost(encode(created), info.SessionToken, testKey, 409)
	invoke := next(protocol.Call)
	params, rpcErr = invoke.Parameters(protocol.ToProvider)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	ip := params.(protocol.CallParams)
	if ip.Address != fixed || string(ip.Input) != `{"text":"ping"}` {
		t.Fatal(ip)
	}
	checkPost(
		encode(protocol.Success(invoke.ID, protocol.CallResult{Address: fixed, Value: json.RawMessage(`{"text":"pong"}`)})),
		info.SessionToken, testKey, 204,
	)
	var pong protocol.CallResult
	response := awaitCall(first)
	if response.Error != nil ||
		json.Unmarshal(response.Result, &pong) != nil ||
		pong.Address != fixed ||
		string(pong.Value) != `{"text":"pong"}` {
		t.Fatal("invalid Pong", response)
	}
	// Fixed generation reuses the instance and returns business failure via DTO.
	second := call(fixed, `null`)
	invoke = next(protocol.Call)
	checkPost(
		encode(protocol.ErrorResponse(invoke.ID, protocol.NewError(protocol.RemoteError, "handler failed", &fixed))),
		info.SessionToken, testKey, 204,
	)
	assertFailure := func(m protocol.Message, address protocol.Address) {
		t.Helper()
		var data protocol.ErrorData
		if m.Error == nil ||
			m.Error.Code != protocol.RemoteError ||
			json.Unmarshal(m.Error.Data, &data) != nil ||
			data.Kind != "remote_error" ||
			data.Address == nil ||
			*data.Address != address {
			t.Fatalf("business failure lost evidence: %+v", m)
		}
	}
	assertFailure(awaitCall(second), fixed)
	failed := cold
	failed.ID = "create-failure"
	third := call(failed, `null`)
	create = next(protocol.Create)
	params, rpcErr = create.Parameters(protocol.ToProvider)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	failedFixed := params.(protocol.CreateParams).Address
	checkPost(
		encode(protocol.ErrorResponse(create.ID, protocol.NewError(protocol.RemoteError, "factory failed", &failedFixed))),
		info.SessionToken, testKey, 204,
	)
	assertFailure(awaitCall(third), failedFixed)
	// The next event must be the independent ID's create, never invoke on the
	// failed ID. Disconnect while this new create is pending, then join its Call.
	held := cold
	held.ID = "disconnect"
	fourth := call(held, `null`)
	create = next(protocol.Create)
	params, rpcErr = create.Parameters(protocol.ToProvider)
	if rpcErr != nil || params.(protocol.CreateParams).Address.ID != held.ID {
		t.Fatal("failed ID invoked or wrong create")
	}
	disconnect()
	stream.Body.Close()
	select {
	case <-readDone:
	case <-ctx.Done():
		t.Fatal("SSE reader did not exit")
	}
	ended := awaitCall(fourth)
	if ended.Error == nil {
		t.Fatal("disconnected create succeeded")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for len(g.Actors()) != 0 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("disconnect did not retract directory")
		}
	}
	checkPost(encode(created), info.SessionToken, testKey, 403)
	// Close the Gateway while another authenticated registration stream is live.
	closingStream, err := post(
		ctx, request(protocol.Register, "close-register", protocol.RegisterParams{Type: typ + "-close"}),
		"", "text/event-stream, application/json", testKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer closingStream.Body.Close()
	reader := bufio.NewReader(closingStream.Body)
	if line, err := reader.ReadString('\n'); err != nil || !strings.HasPrefix(line, "data: ") {
		t.Fatal("closing stream registration", err)
	}
	if err := g.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatal("Gateway.Close failed to end SSE", err)
	}
	if len(g.Actors()) != 0 {
		t.Fatal("closed Gateway retained directory")
	}
	if _, err := rt.Addr(typ, "still-open"); err != nil {
		t.Fatal("Gateway closed borrowed Runtime", err)
	}
	t.Run("write failure retracts registration", func(t *testing.T) {
		g2, err := remote.NewGateway(rt)
		if err != nil {
			t.Fatal(err)
		}
		h2, err := NewHandler(g2, Options{APIKey: testKey})
		if err != nil {
			t.Fatal(err)
		}
		exited := make(chan struct{})
		failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(exited)
			h2.ServeHTTP(failFlushWriter{w}, r)
		}))
		defer failing.Close()
		defer func() {
			if err := g2.Close(ctx); err != nil {
				t.Error(err)
			}
		}()
		req, err := http.NewRequestWithContext(
			ctx, "POST", failing.URL+"/runtime/rpc",
			bytes.NewReader(request(protocol.Register, "write-failure", protocol.RegisterParams{Type: typ + "-write"})),
		)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+testKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "text/event-stream, application/json")
		res, err := failing.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		select {
		case <-exited:
		case <-ctx.Done():
			t.Fatal("write failure did not end handler")
		}
		if len(g2.Actors()) != 0 {
			t.Fatal("write failure retained registration")
		}
	})
}

type failFlushWriter struct{ http.ResponseWriter }

func (w failFlushWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (failFlushWriter) FlushError() error { return fmt.Errorf("injected flush failure") }
