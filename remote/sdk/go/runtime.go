package actorxremote

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// Runtime owns its HTTP client, registrations and in-flight operations.
// Use NewRuntime; the zero value is not usable.
type Runtime struct {
	mu            sync.Mutex
	closing       bool
	ctx           context.Context
	cancel        context.CancelFunc
	closed        chan struct{}
	work          sync.WaitGroup
	failure       error
	registrations map[string]*registration

	http      *http.Client
	transport *http.Transport
	url       string
	key       string
}

// NewRuntime validates a gateway origin and API key without network activity.
func NewRuntime(origin string, opts Options) (*Runtime, error) {
	endpoint, err := validateOptions(origin, opts.APIKey)
	if err != nil {
		return nil, err
	}

	// Own the full configuration instead of inheriting a mutable global client
	// or transport (including its proxy and TLS settings).
	t := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}

	// HTTP/2 can internally redispatch an unread POST on an unusable pooled
	// connection. Use HTTP/1 and non-replayable bodies for the v1 no-retry rule.
	t.Protocols = new(http.Protocols)
	t.Protocols.SetHTTP1(true)

	ctx, cancel := context.WithCancel(context.Background())
	return &Runtime{
		ctx:           ctx,
		cancel:        cancel,
		closed:        make(chan struct{}),
		registrations: make(map[string]*registration),
		url:           endpoint,
		key:           opts.APIKey,
		transport:     t,
		http: &http.Client{
			Transport: t,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// Addr constructs an independent logical handle without selecting a generation.
func (r *Runtime) Addr(actorType, id string) (*Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return nil, remoteError(1006, nil)
	}
	return newAddr(r, actorType, id)
}

// begin admits work atomically with Close, which may safely wait on work after
// marking the runtime closing. Cancellation joins both caller and runtime life.
func (r *Runtime) begin(parent context.Context) (context.Context, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return nil, nil, remoteError(1006, nil)
	}
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(r.ctx, cancel)
	r.work.Add(1)
	return ctx, func() {
		stop()
		cancel()
		r.work.Done()
	}, nil
}

// Close rejects new work, cancels current operations and joins them in the
// background. The context limits this wait, not ownership of cleanup.
func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	r.startCloseLocked(nil)
	r.mu.Unlock()
	return r.WaitClosed(ctx)
}

// A provider failure shuts down all registrations and consumer operations.
func (r *Runtime) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.startCloseLocked(err)
}

func (r *Runtime) startCloseLocked(err error) {
	if !r.closing {
		r.closing = true
		r.failure = err
		r.cancel()
		go func() {
			r.work.Wait()
			r.transport.CloseIdleConnections()
			close(r.closed)
		}()
	}
}

// WaitClosed waits for full cleanup without initiating shutdown. It returns the
// provider failure that caused shutdown, or nil after an explicit Close.
func (r *Runtime) WaitClosed(ctx context.Context) error {
	select {
	case <-r.closed:
		return r.failure
	default:
	}
	select {
	case <-r.closed:
		return r.failure
	case <-ctx.Done():
		return ctx.Err()
	}
}
