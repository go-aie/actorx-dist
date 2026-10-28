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

func TestHTTPStreamNATS(t *testing.T) {
	url := os.Getenv("ACTORX_REMOTE_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_REMOTE_NATS_URL to an isolated JetStream server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rt, err := actorxdist.NewRuntime(ctx, actorxdist.WithNATS(url))
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close(context.Background())
	g, err := remote.NewGateway(rt)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close(context.Background())
	h, _ := NewHandler(g, Options{APIKey: testKey})
	server := httptest.NewServer(h)
	defer server.Close()

	post := func(m protocol.Message, session string) (*http.Response, error) {
		raw, err := m.Encode()
		if err != nil {
			return nil, err
		}
		req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/runtime/rpc", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+testKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(ProtocolHeader, protocol.Version)
		req.Header.Set("Accept", "text/event-stream, application/json")
		if session != "" {
			req.Header.Set(SessionHeader, session)
		}
		return server.Client().Do(req)
	}

	send := func(m protocol.Message, session string) error {
		r, err := post(m, session)
		if err != nil {
			return err
		}
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		if r.StatusCode != 204 {
			return fmt.Errorf("POST %d: %s", r.StatusCode, body)
		}
		return nil
	}

	next := func(scanner *bufio.Scanner) (protocol.Message, error) {
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			if !strings.HasPrefix(line, "data: ") {
				return protocol.Message{}, fmt.Errorf("invalid frame")
			}
			m, e := protocol.ParseMessage([]byte(strings.TrimPrefix(line, "data: ")))
			if e != nil {
				return m, e
			}
			return m, nil
		}
		return protocol.Message{}, fmt.Errorf("missing terminal: %v", scanner.Err())
	}

	// Keep the provider SSE connection open for create, invoke and credits.
	typ := fmt.Sprintf("remote-stream-%x", time.Now().UnixNano())
	register, _ := protocol.NewRequest(protocol.Register, "register", protocol.RegisterParams{Type: typ})
	provider, err := post(register, "")
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Body.Close()
	providerReader := bufio.NewScanner(provider.Body)
	m, err := next(providerReader)
	if err != nil {
		t.Fatal(err)
	}
	var info protocol.RegistrationInfo
	_ = json.Unmarshal(m.Result, &info)
	if info.Protocol != protocol.Version {
		t.Fatal("unexpected protocol", info.Protocol)
	}

	providerDone := make(chan error, 1)
	go func() {
		finish := func(err error) { providerDone <- err }
		create, err := next(providerReader)
		if err != nil {
			finish(err)
			return
		}
		var created protocol.CreateParams
		_ = json.Unmarshal(create.Params, &created)
		if create.Method != protocol.Create {
			finish(fmt.Errorf("create must precede stream"))
			return
		}
		if err = send(protocol.Success(create.ID, protocol.CreateResult{Address: created.Address}), info.SessionToken); err != nil {
			finish(err)
			return
		}
		invoke, err := next(providerReader)
		if err != nil {
			finish(err)
			return
		}
		params, e := invoke.Parameters(protocol.ToProvider)
		if e != nil {
			finish(e)
			return
		}
		p := params.(protocol.InvokeStreamParams)
		// The first four chunks fill the provider window. Further chunks
		// require credit propagated from the consumer through the native stream.
		for seq := uint64(1); seq <= 6; seq++ {
			if seq > 4 {
				credit, err := next(providerReader)
				if err != nil {
					finish(err)
					return
				}
				if credit.Method != protocol.Credit {
					finish(fmt.Errorf("expected credit"))
					return
				}
			}
			if err = send(protocol.Notification(protocol.ChunkMethod, protocol.ChunkParams{
				RequestID:   invoke.ID,
				StreamToken: p.StreamToken,
				Address:     p.Address,
				Seq:         seq,
				Value:       json.RawMessage(`null`),
			}), info.SessionToken); err != nil {
				finish(err)
				return
			}
		}
		finish(send(protocol.Success(invoke.ID, protocol.StreamResult{Address: p.Address, FinalSeq: 6}), info.SessionToken))
	}()

	// Consume all six chunks, preserving the selected identity across the stream.
	request, _ := protocol.NewRequest(protocol.Stream, "consumer", protocol.CallParams{
		Address: protocol.Address{Version: 1, Type: typ, ID: "one"},
		Input:   json.RawMessage("null"),
	})
	consumer, err := post(request, "")
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Body.Close()
	reader := bufio.NewScanner(consumer.Body)
	open, err := next(reader)
	if err != nil || open.Method != protocol.StreamOpen {
		t.Fatal(open, err)
	}
	var opened protocol.OpenParams
	_ = json.Unmarshal(open.Params, &opened)
	var fixed protocol.Address
	for seq := uint64(1); seq <= 6; seq++ {
		m, err := next(reader)
		if err != nil {
			t.Fatal(err)
		}
		var chunk protocol.ChunkParams
		_ = json.Unmarshal(m.Params, &chunk)
		if m.Method != protocol.ChunkMethod || chunk.Seq != seq || string(chunk.Value) != "null" || chunk.Address.Validate(true) != nil {
			t.Fatal(m)
		}
		if seq == 1 {
			fixed = chunk.Address
		} else if fixed != chunk.Address {
			t.Fatal("generation switched")
		}
		control := protocol.Notification(protocol.Credit, protocol.ControlParams{
			RequestID:   request.ID,
			StreamToken: opened.StreamToken,
			AckSeq:      seq,
		})
		if seq == 1 {
			wrong := control
			var p protocol.ControlParams
			_ = json.Unmarshal(control.Params, &p)
			p.AckSeq = 2
			wrong = protocol.Notification(protocol.Credit, p)
			res, err := post(wrong, "")
			if err != nil {
				t.Fatal(err)
			}
			_ = res.Body.Close()
			if res.StatusCode != 409 {
				t.Fatal(res.Status)
			}
		}
		if err := send(control, ""); err != nil {
			t.Fatal(err)
		}
	}
	terminal, err := next(reader)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Error != nil || protocol.ParseStreamResult(terminal.Result, fixed, 6) != nil {
		t.Fatal(terminal)
	}
	if err := <-providerDone; err != nil {
		t.Fatal(err)
	}
}
