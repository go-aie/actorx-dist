package actorxremote

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func TestLocalConfigurationValidation(t *testing.T) {
	for _, origin := range []string{
		"file:///tmp/rpc",
		"http://user:secret@gateway.test",
		"http://gateway.test/?key=secret",
		"http://gateway.test/?",
		"http://gateway.test/#secret",
		"http://gateway.test/#",
		"http://gateway.test/other",
		"http://gateway.test/%2f",
		"http://gateway.test:bad",
		"http://gateway.test:65536",
		"http://",
		"//gateway.test",
	} {
		if _, err := NewRuntime(origin, Options{APIKey: "key"}); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("%q: %v", origin, err)
		}
	}
	for _, key := range []string{"", "space key", "line\nbreak", "secret?"} {
		if _, err := NewRuntime("http://gateway.test", Options{APIKey: key}); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal(err)
		}
	}

	r, a := newTestRuntime(t, func(*http.Request) (*http.Response, error) {
		t.Error("unexpected network")
		return nil, errors.New("unexpected")
	})
	if r.transport.Proxy != nil || r.http.Timeout != 0 || !r.transport.Protocols.HTTP1() || r.transport.Protocols.HTTP2() {
		t.Fatal("unexpected owned transport configuration")
	}
	for _, tc := range [][2]string{
		{"Invalid", "x"},
		{"demo..ping", "x"},
		{strings.Repeat("x", 65), "x"},
		{"demo.ping", ""},
		{"demo.ping", "\n"},
		{"demo.ping", string([]byte{0xff})},
		{"demo.ping", strings.Repeat("x", 129)},
	} {
		if _, err := r.Addr(tc[0], tc[1]); err == nil {
			t.Fatal(tc)
		}
	}
	snap := a.Address()
	snap.Type = "changed"
	if a.Address().Type != selected.Type {
		t.Fatal("shared snapshot")
	}
}

func TestCloseCanceledWaitRetainsCleanupAndJoinsCalls(t *testing.T) {
	entered := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	release := make(chan struct{})
	r, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
		entered <- struct{}{}
		<-req.Context().Done()
		canceled <- struct{}{}
		<-release // Simulate transport cleanup which has not yet returned.
		return nil, req.Context().Err()
	})
	done := make(chan error, 1)
	go func() {
		_, err := a.Call(context.Background(), nil)
		done <- err
	}()
	waitValue(t, entered)

	// A canceled waiter leaves the blocked transport owned by Runtime.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.WaitClosed(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := r.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	waitValue(t, canceled)
	if _, err := r.Addr(selected.Type, selected.ID); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if _, err := a.Call(context.Background(), nil); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	select {
	case <-r.closed:
		t.Fatal("cleanup completed before work exited")
	default:
	}
	if err := r.WaitClosed(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// Releasing the transport lets all concurrent closers finish.
	close(release)
	if err := waitValue(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := r.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := r.WaitClosed(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !a.uncertain {
		t.Fatal("shutdown lost uncertain evidence")
	}
}
