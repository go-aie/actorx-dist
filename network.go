package actorxdist

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/errwire"
	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/go-aie/actorx-dist/internal/strictjson"
	"github.com/nats-io/nats.go"
)

type natsConfig struct {
	url     string
	options []nats.Option
}

// WithNATS snapshots connection options. The last declaration replaces the whole
// preceding declaration. Runtime exclusively owns the resulting connection.
func WithNATS(serverURL string, options ...nats.Option) Option {
	snapshot := append([]nats.Option(nil), options...)
	return func(cfg *runtimeOptions) { cfg.nats = natsConfig{serverURL, snapshot} }
}

func connectionOptions(ctx context.Context, c natsConfig) (out nats.Options, err error) {
	defer func() {
		if recover() != nil {
			out = nats.Options{}
			err = actorx.ErrSetup
		}
	}()

	out = nats.GetDefaultOptions()
	out.Url = c.url
	if out.Url == "" {
		out.Url = nats.DefaultURL
	}
	for _, option := range c.options {
		if option == nil {
			return out, actorx.ErrSetup
		}
		if err = option(&out); err != nil {
			return out, errors.Join(actorx.ErrSetup, err)
		}
	}

	if out.ClosedCB != nil ||
		out.DisconnectedCB != nil ||
		out.DisconnectedErrCB != nil ||
		out.ReconnectedCB != nil ||
		out.ConnectedCB != nil ||
		out.AsyncErrorCB != nil ||
		out.DiscoveredServersCB != nil ||
		out.LameDuckModeHandler != nil ||
		out.CustomReconnectDelayCB != nil ||
		out.RetryOnFailedConnect ||
		out.NoEcho ||
		out.InProcessServer != nil ||
		out.CustomDialer != nil ||
		out.Dialer != nil {
		return out, fmt.Errorf("%w: SDK lifecycle, dialer, retry and no-echo overrides are unsupported", actorx.ErrSetup)
	}

	for _, server := range append([]string{out.Url}, out.Servers...) {
		for _, address := range strings.Split(server, ",") {
			u, e := url.Parse(address)
			if e != nil || u.Hostname() == "" || u.Fragment != "" || u.RawQuery != "" {
				return out, actorx.ErrSetup
			}
			if u.Scheme != "nats" && u.Scheme != "tls" && u.Scheme != "ws" && u.Scheme != "wss" {
				return out, actorx.ErrSetup
			}
			if u.Scheme == "nats" || u.Scheme == "tls" {
				if u.Path != "" {
					return out, actorx.ErrSetup
				}
			}
			if u.Port() != "" {
				p, e := strconv.Atoi(u.Port())
				if e != nil || p <= 0 || p > 65535 {
					return out, actorx.ErrSetup
				}
			}
		}
	}

	remaining := time.Until(deadline(ctx))
	if remaining <= 0 {
		return out, ctx.Err()
	}
	if out.Timeout <= 0 || out.Timeout > remaining {
		out.Timeout = remaining
	}

	// Disable reconnect buffering so an uncertain request cannot be replayed later.
	out.ReconnectBufSize = -1
	out.UseOldRequestStyle = true
	out.SubChanLen = 8
	out.NoCallbacksAfterClientClose = true
	return out, nil
}

func deadline(ctx context.Context) time.Time {
	d, _ := ctx.Deadline()
	return d
}

// Diagnostic describes a bounded transport event without a business payload.
type Diagnostic struct {
	Operation string
	Message   string
}

// Diagnostics drains up to 64 events and returns the cumulative overflow count.
// SDK callbacks never execute application code. Records contain no payload or credentials.
func (rt *Runtime) Diagnostics() ([]Diagnostic, uint64) {
	if rt == nil || rt.net == nil {
		return nil, 0
	}
	n := rt.net
	out := make([]Diagnostic, 0, 64)
	for range 64 {
		select {
		case e := <-n.diagnostics:
			out = append(out, e)
		default:
			return out, n.dropped.Load()
		}
	}
	return out, n.dropped.Load()
}

type incoming struct {
	msg          *nats.Msg
	typ, token   string
	registration *Registration
	control      bool
	business     *businessTarget
	overloaded   bool
	discovery    bool
}

type network struct {
	rt       *Runtime
	nc       *nats.Conn
	registry *registry
	ready    readiness

	diagnostics chan Diagnostic
	dropped     atomic.Uint64

	reconnect   chan struct{}
	ordinary    chan incoming
	control     chan incoming
	stop        chan struct{}
	wg          sync.WaitGroup
	quiesceOnce sync.Once
	contracts   *contractCache // Own lock; independent of stream/lease accounting.

	// rpcMu protects inbox setup, reply routing and retained reply-buffer slots.
	rpcMu          sync.Mutex
	inbox          string
	replies        *nats.Subscription
	streamControls *nats.Subscription
	pending        map[string]chan *nats.Msg
	replyHeld      map[string]bool
	streamReplies  map[string]*receiveStream

	// businessMu protects stream/lease accounting.
	streams        int
	businessMu     sync.Mutex
	outgoing, acks int
	leases         map[string]*businessReply

	// Test-only before/after-publish and reply-loss boundaries; installed before use.
	businessFault func(string, businessFrame) error
	codecSlots    chan struct{}
	payloadSlots  chan struct{}

	// Admission and callback joining share discoveryMu. The failure counter is
	// a common receive-loss baseline, not per-query state.
	discoveryMu        sync.Mutex
	discoveryClosed    bool
	discoveryCallbacks sync.WaitGroup
	discoveryQueries   *nats.Subscription
	discoveryReplies   *nats.Subscription
	discoveryInbox     string
	discoveryFailures  atomic.Uint64
}

func (n *network) report(op string, err error) {
	message := ""
	if err != nil {
		message = err.Error()
	}
	if len(message) > 960 {
		message = message[:960]
	}
	select {
	case n.diagnostics <- Diagnostic{op, message}:
	default:
		n.dropped.Add(1)
	}
}

func (rt *Runtime) connect(ctx context.Context, opts nats.Options) error {
	n := &network{
		rt:           rt,
		diagnostics:  make(chan Diagnostic, 64),
		reconnect:    make(chan struct{}, 1),
		ordinary:     make(chan incoming, rt.limits.payloads()),
		control:      make(chan incoming, rt.limits.controls()),
		stop:         make(chan struct{}),
		codecSlots:   make(chan struct{}, 8),
		payloadSlots: make(chan struct{}, rt.limits.payloads()),
		leases:       map[string]*businessReply{},
		contracts:    newContractCache(rt.limits.transport.ContractCacheTypes),
	}
	rt.net = n

	// SDK callbacks only close gates, interrupt streams or enqueue diagnostics/work.
	opts.DisconnectedErrCB = func(_ *nats.Conn, err error) {
		n.ready.Store(false)
		n.interruptStreams()
		n.report("disconnect", err)
	}
	opts.ReconnectedCB = func(_ *nats.Conn) {
		select {
		case n.reconnect <- struct{}{}:
		default:
		}
	}
	opts.ClosedCB = func(_ *nats.Conn) {
		n.ready.Store(false)
		n.interruptStreams()
		n.report("closed", ErrOwnerUnavailable)
	}
	opts.AsyncErrorCB = func(_ *nats.Conn, sub *nats.Subscription, err error) {
		if sub != nil && sub.Subject == n.rt.names.DiscoveryReplySubject(n.rt.runtimeID) &&
			errors.Is(err, nats.ErrSlowConsumer) {
			n.discoveryReceiveFailure()
		}
		n.ready.Store(false)
		n.interruptStreams()
		if sub != nil {
			err = fmt.Errorf("%w: subject %s", err, sub.Subject)
		}
		n.report("async", err)
	}

	// A context-aware standard dialer bounds cancellation during initial TCP dial.
	opts.CustomDialer = &initDialer{ctx: ctx, timeout: opts.Timeout}
	nc, err := opts.Connect()
	if err != nil {
		return err
	}
	n.nc = nc
	if nc.MaxPayload() != 128<<10 {
		return fmt.Errorf("%w: server max_payload must be 131072", actorx.ErrSetup)
	}

	n.registry = newRegistry(nc, rt.names)
	if err = n.registry.initialize(ctx); err != nil {
		return err
	}

	return nil
}

// After initialization, reconnect uses a fresh bounded context, not initCtx.
type initDialer struct {
	ctx         context.Context
	timeout     time.Duration
	initialized atomic.Bool
}

func (d *initDialer) Dial(network, address string) (net.Conn, error) {
	ctx := d.ctx
	if d.initialized.Load() {
		ctx = context.Background()
	}
	dialer := net.Dialer{Timeout: d.timeout}
	return dialer.DialContext(ctx, network, address)
}

func (n *network) factorySubject(typ string) string { return n.rt.names.FactorySubject(typ) }

func (n *network) controlSubject(typ, token string) string {
	return n.rt.names.ControlSubject(typ, token)
}

func (n *network) subscribe(ctx context.Context) error {
	epoch := n.ready.epoch()

	if err := n.subscribeDiscovery(ctx); err != nil {
		return err
	}
	if err := n.nc.LastError(); err != nil {
		return err
	}
	if !n.nc.IsConnected() {
		return ErrOwnerUnavailable
	}
	n.nc.Opts.CustomDialer.(*initDialer).initialized.Store(true)
	for range 8 {
		n.wg.Add(1)
		go n.dispatch(n.ordinary)
	}
	for range 4 {
		n.wg.Add(1)
		go n.dispatch(n.control)
	}
	n.wg.Add(1)
	go n.supervise()
	if !n.ready.resume(epoch) {
		return ErrOwnerUnavailable
	}
	return nil
}

func (n *network) callback(r *Registration, control bool) nats.MsgHandler {
	return func(msg *nats.Msg) {
		if len(msg.Data) > 128<<10 {
			n.report("frame", ErrProtocol)
			return
		}
		queue := n.ordinary
		if control {
			queue = n.control
		}

		// SDK owns this Msg exclusively until handed to one bounded queue. No copy,
		// no callback I/O, and no goroutine per frame.
		select {
		case queue <- incoming{msg: msg, typ: r.spec.name, token: r.token, registration: r, control: control}:
		default:
			n.report("dispatch", actorx.ErrFull)
		}
	}
}

func (n *network) dispatch(queue <-chan incoming) {
	defer n.wg.Done()
	for {
		select {
		case <-n.stop:
			return
		case in := <-queue:
			if in.discovery {
				n.handleDiscovery(in)
			} else if in.business != nil {
				n.handleBusiness(in)
			} else {
				n.handle(in)
			}
		}
	}
}

func (n *network) supervise() {
	defer n.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	index := 0
	for {
		select {
		case <-n.stop:
			return
		case <-n.reconnect:
			ctx, cancel := context.WithTimeout(n.rt.life, 2*time.Second)
			epoch := n.ready.epoch()
			err := n.registry.initialize(ctx)
			if err == nil {
				err = n.verifyOwnership(ctx)
			}
			if err == nil {
				err = n.nc.FlushWithContext(ctx)
			}
			cancel()
			if err == nil && n.nc.IsConnected() && n.rt.checkOpen() == nil {
				n.ready.resume(epoch)
			} else {
				n.report("reconnect", err)
			}
		case now := <-ticker.C:
			n.rt.mu.Lock()
			hasTypes := len(n.rt.registrations) > 0
			n.rt.mu.Unlock()
			if !hasTypes || !n.ready.Load() {
				continue
			}
			ctx, cancel := context.WithTimeout(n.rt.life, time.Second)
			err := n.registry.scanPage(ctx, index, now)
			cancel()
			index = 1 - index
			if err != nil && n.rt.life.Err() == nil {
				n.report("maintenance", err)
			}
		}
	}
}

func (n *network) closeAdmission() {
	n.ready.Store(false)
	n.closeDiscovery()
	n.rt.mu.Lock()
	defer n.rt.mu.Unlock()
	for _, r := range n.rt.registrations {
		if r.shared != nil {
			_ = r.shared.Unsubscribe()
		}
	}
}

// quiesce runs after terminal/creation work, while management can still receive
// in-flight control completions. Control callbacks cannot enqueue afterwards.
func (n *network) quiesce() {
	n.quiesceOnce.Do(func() {
		n.closeDiscovery()
		n.rt.mu.Lock()
		for _, r := range n.rt.registrations {
			if r.control != nil {
				_ = r.control.Unsubscribe()
			}
		}
		n.rt.mu.Unlock()
		close(n.stop)
		n.wg.Wait()
	})
}

func (n *network) close() {
	n.quiesce()
	n.rpcMu.Lock()
	if n.replies != nil {
		_ = n.replies.Unsubscribe()
	}
	if n.streamControls != nil {
		_ = n.streamControls.Unsubscribe()
	}
	n.rpcMu.Unlock()
	// Runtime.Close has joined admitted work before releasing cached snapshots.
	n.contracts.close()
	n.businessMu.Lock()
	n.leases = nil
	n.businessMu.Unlock()
	if n.nc != nil {
		n.nc.Close()
	}

	// Synchronous unsubscribe plus the connection close drops queued SDK data.
	for {
		select {
		case in := <-n.ordinary:
			if in.discovery {
				<-n.payloadSlots
			}
		default:
			goto control
		}
	}
control:
	for {
		select {
		case <-n.control:
		default:
			return
		}
	}
}

type factoryFrame struct {
	Version     int             `json:"version"`
	Kind        string          `json:"kind"`
	Operation   string          `json:"operation"`
	Type        string          `json:"type"`
	ID          string          `json:"id"`
	Generation  string          `json:"generation,omitempty"`
	Factory     string          `json:"factory,omitempty"`
	Correlation string          `json:"correlation"`
	Deadline    int64           `json:"deadline"`
	Fingerprint string          `json:"fingerprint,omitempty"`
	Reply       string          `json:"reply,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Status      string          `json:"status,omitempty"`
	Error       json.RawMessage `json:"error,omitempty"`
}

func decodeFactoryFrame(data []byte, out *factoryFrame) error {
	if strictjson.Validate(data, 128<<10, 16, 1024) != nil {
		return ErrProtocol
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return ErrProtocol
	}
	return nil
}

func safeReply(s string) bool {
	if len(s) == 0 || len(s) > 256 {
		return false
	}
	for _, c := range s {
		if c <= 32 || c >= 127 || c == '*' || c == '>' {
			return false
		}
	}
	return true
}

func (n *network) handle(in incoming) {
	r := in.registration
	if r == nil {
		return
	}
	var req factoryFrame
	if strictjson.Validate(in.msg.Data, 128<<10, 16, 1024) != nil {
		return
	}
	d := json.NewDecoder(bytes.NewReader(in.msg.Data))
	d.DisallowUnknownFields()
	if d.Decode(&req) != nil {
		return
	}
	if !safeReply(in.msg.Reply) || req.Reply != in.msg.Reply {
		return
	}
	err := error(nil)
	if req.Version != 1 ||
		req.Kind != "request" ||
		req.Status != "" ||
		len(req.Error) != 0 ||
		!opaque(req.Correlation) ||
		req.Type != in.typ ||
		identity.Validate(req.Type, req.ID) != nil ||
		req.Deadline <= 0 {
		err = ErrProtocol
	}
	if err == nil {
		if in.control {
			if req.Factory != in.token ||
				!opaque(req.Generation) ||
				in.msg.Subject != strings.TrimSuffix(n.controlSubject(in.typ, in.token), "*")+req.Operation ||
				(req.Operation != "confirm" && req.Operation != "stop") {
				err = ErrProtocol
			}
		} else if req.Operation != "activate" || req.Generation != "" || req.Factory != "" || !validDigest(req.Fingerprint) {
			err = ErrProtocol
		}
	}
	if err == nil && req.Fingerprint != "" && req.Fingerprint != r.spec.contract.fingerprint {
		err = ErrContract
	}
	if err == nil && !in.control && !n.ready.Load() {
		err = ErrOwnerUnavailable
	}
	if err == nil && !in.control {
		n.rt.mu.Lock()
		if n.rt.closed {
			err = actorx.ErrClosed
		} else if !r.ready || r.draining || n.rt.registrations[in.typ] != r {
			err = ErrTypeUnavailable
		}
		n.rt.mu.Unlock()
	}
	var payload any
	limit := time.Now().Add(60 * time.Second)
	requested := time.Unix(0, req.Deadline)
	if requested.Before(limit) {
		limit = requested
	}

	// Control remains available during Close until all owned work is settled.
	ctx, cancel := context.WithDeadline(context.Background(), limit)
	defer cancel()
	bounded, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	if err == nil {
		err = bounded.Err()
	}
	if err == nil {
		if req.Operation == "confirm" {
			// Factory schedules KV I/O through StartTask, never its handler or SDK callback.
			payload, err = r.factory.Call[owned](bounded, confirmRequest{ActorKey{req.Type, req.ID}, req.Generation, in.token})
			if err == nil {
				confirmed := payload.(owned)
				req.Generation = confirmed.value.Generation
				payload = map[string]any{
					"state":      confirmed.value.State,
					"revision":   confirmed.revision,
					"generation": confirmed.value.Generation,
					"factory":    confirmed.value.Factory,
				}
			}
		} else if req.Operation == "activate" {
			var intent struct {
				Create *bool `json:"create"`
			}
			if decodeRecord(req.Payload, &intent) != nil || intent.Create == nil {
				err = ErrProtocol
			} else if req.Fingerprint != r.spec.contract.fingerprint {
				err = ErrContract
			} else if !*intent.Create {
				payload, err = n.lookup(bounded, ActorKey{req.Type, req.ID})
			} else {
				var entry *creation
				entry, err = r.factory.Call[*creation](bounded, activateRequest{ActorKey{req.Type, req.ID}})
				// The Factory may already have admitted Runtime-owned work when its
				// caller stops waiting. A timeout is not a definite refusal.
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					err = errors.Join(ErrActivationUnknown, err)
				}
				if err == nil {
					select {
					case <-entry.activation.selected:
						payload, err = entry.activation.owner, entry.activation.err
						if err == nil {
							payload, err = n.confirmOwner(bounded, entry.activation.owner)
						}
					case <-bounded.Done():
						err = errors.Join(ErrActivationUnknown, bounded.Err())
					}
				}
			}
		} else {
			var cause error
			if len(req.Payload) != 0 {
				cause, err = errwire.Decode(req.Payload)
			}
			if err == nil {
				_, err = r.factory.Call[any](bounded, stopActivationRequest{key: ActorKey{req.Type, req.ID}, generation: req.Generation, factory: in.token, cause: cause})
			}
		}
	}
	if o, ok := payload.(owned); ok && err == nil {
		req.Generation, req.Factory = o.value.Generation, o.value.Factory
		payload = selection{o.value.State, o.revision, o.value.Generation, o.value.Factory}
	}
	req.Kind = "complete"
	if req.Factory == "" {
		req.Factory = r.token
	}
	req.Reply = ""
	req.Payload = nil
	if err != nil {
		req.Status = "error"
		req.Error, _ = errwire.Encode(err, errwire.Source{Operation: req.Operation, Type: req.Type, ID: req.ID, Generation: req.Generation})
	} else {
		req.Status = "ok"
		req.Payload, _ = json.Marshal(payload)
	}
	data, marshalErr := json.Marshal(req)
	if marshalErr == nil && len(data) <= 128<<10 {
		if e := n.nc.Publish(in.msg.Reply, data); e != nil {
			n.report("reply", e)
		}
	}
}

type confirmRequest struct {
	key                 ActorKey
	generation, factory string
}

func (f *factory) confirm(ctx actorx.Context, msg confirmRequest, reply actorx.Reply[owned]) error {
	_, err := actorx.StartTask(ctx, func(taskCtx context.Context) (owned, error) {
		return f.rt.net.confirmCurrent(taskCtx, msg.key, msg.generation, msg.factory)
	}, func(value owned, err error) bool {
		reply.Complete(value, err)
		return true
	}, nil)
	return err
}

// A fault advances the epoch and closes its loss signal under the same lock.
// A delayed reconnect cannot overwrite a newer fault; observe returns a matching
// epoch/signal pair even if a disconnect and reconnect happen before it is used.
type readiness struct {
	state atomic.Uint64
	mu    sync.Mutex
	lost  chan struct{}
}

func (s *readiness) Load() bool { return s.state.Load()&1 != 0 }

func (s *readiness) Store(ready bool) {
	if ready {
		panic("use resume with an observed epoch")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.state.Load()
	s.state.Store((old + 2) &^ 1)
	if s.lost != nil {
		close(s.lost)
		s.lost = nil
	}
}

func (s *readiness) epoch() uint64 { return s.state.Load() }

func (s *readiness) resume(epoch uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.CompareAndSwap(epoch, epoch|1)
}

func (s *readiness) observe() (uint64, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lost == nil {
		s.lost = make(chan struct{})
	}
	return s.state.Load(), s.lost
}

func (n *network) verifyOwnership(ctx context.Context) error {
	n.rt.mu.Lock()
	specs := make(map[string]registration, len(n.rt.specs))
	for name, spec := range n.rt.specs {
		specs[name] = spec
	}
	n.rt.mu.Unlock()
	for name, spec := range specs {
		entry, err := n.registry.get(ctx, n.registry.names.TypesBucket, "t."+identity.Token(name), true)
		if err != nil {
			return err
		}
		if !bytes.Equal(entry.Value(), spec.contract.data) {
			return ErrContract
		}
	}
	n.rt.mu.Lock()
	held := make([]struct {
		key        ActorKey
		generation string
		token      string
	}, 0, len(n.rt.ledger))
	for _, entry := range n.rt.ledger {
		held = append(held, struct {
			key        ActorKey
			generation string
			token      string
		}{entry.key, entry.generation, entry.registration.token})
	}
	n.rt.mu.Unlock()
	for _, entry := range held {
		o, err := n.registry.owner(ctx, entry.key)
		if err != nil {
			return err
		}
		if o.value.Generation != entry.generation || o.value.Factory != entry.token || o.value.State == "INACTIVE" {
			return ErrActivationUnknown
		}
	}
	return nil
}

// Each registration owns its subscriptions. Dedicated control is flushed before
// exposing the shared creation route; callbacks carry this registration identity.
func (n *network) installRegistration(ctx context.Context, r *Registration) error {
	n.rt.mu.Lock()
	if n.rt.closed || r.draining {
		n.rt.mu.Unlock()
		return actorx.ErrClosed
	}
	sub, err := n.nc.Subscribe(n.controlSubject(r.spec.name, r.token), n.callback(r, true))
	r.control = sub
	n.rt.mu.Unlock()
	if err != nil {
		return err
	}
	if err = sub.SetPendingLimits(8, 1<<20); err != nil {
		return err
	}
	if err = n.nc.FlushWithContext(ctx); err != nil {
		return err
	}
	n.rt.mu.Lock()
	if n.rt.closed || r.draining {
		n.rt.mu.Unlock()
		return actorx.ErrClosed
	}
	subject := n.factorySubject(r.spec.name)
	sub, err = n.nc.QueueSubscribe(subject, subject+".workers", n.callback(r, false))
	r.shared = sub
	n.rt.mu.Unlock()
	if err != nil {
		return err
	}
	if err = sub.SetPendingLimits(8, 1<<20); err != nil {
		return err
	}
	if err = n.nc.FlushWithContext(ctx); err != nil {
		return err
	}
	if err = n.nc.LastError(); err != nil {
		return err
	}
	if !n.nc.IsConnected() || !n.ready.Load() {
		return ErrOwnerUnavailable
	}
	return nil
}
