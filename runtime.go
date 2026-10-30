package actorxdist

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/go-aie/actorx-dist/internal/naming"
)

type runtimeOptions struct {
	namespace string
	nats      natsConfig
	localTest bool
	execution ExecutionLimits
	transport TransportLimits
}

// Option configures namespace, connection and resource limits before initialization.
type Option func(*runtimeOptions)

// Runtime owns local execution, NATS subscriptions and a JetStream Registry.
// A Runtime must be constructed with NewRuntime and must not be copied.
type Runtime struct {
	mu          sync.Mutex
	initialized bool
	closed      bool
	life        context.Context
	cancel      context.CancelFunc
	limits      runtimeLimits
	names       naming.Names
	runtimeID   string
	discovery   actorx.Addr

	business, management *actorx.Runtime
	execution            *actorx.LocalChannel
	registrations        map[string]*Registration
	factories            map[string]actorx.Addr
	specs                map[string]registration

	// mu protects admission and accounting; work joins all admitted work on Close.
	ledger         map[string]*creation
	creating       int
	perType        map[string]int
	work           sync.WaitGroup
	resolvers      int
	resolutionJobs int

	once     sync.Once
	done     chan struct{}
	closeErr error
	net      *network

	// Tests may install an internal adapter; production always uses NATS.
	invoke     func(context.Context, Addr, actorx.Kind, any, reflect.Type) (any, error)
	openStream func(context.Context, Addr, any, reflect.Type) (*receiveStream, error)
}

// NewRuntime initializes execution, connection and Registry resources.
// Use Runtime.Register after creation to provide Actor types.
// initCtx bounds initialization only; Close owns the successful lifetime.
// Success does not register any Actor types or construct business objects.
func NewRuntime(initCtx context.Context, options ...Option) (*Runtime, error) {
	if initCtx == nil {
		return nil, actorx.ErrSetup
	}
	initCtx, initCancel := context.WithTimeout(initCtx, 10*time.Second)
	defer initCancel()

	cfg := runtimeOptions{namespace: naming.DefaultNamespace}
	for _, option := range options {
		if option == nil {
			return nil, actorx.ErrSetup
		}
		option(&cfg)
	}
	names, err := naming.New(cfg.namespace)
	if err != nil {
		return nil, err
	}
	limits, err := normalizeLimits(cfg.execution, cfg.transport)
	if err != nil {
		return nil, err
	}
	runtimeID, err := identity.NewOpaque()
	if err != nil {
		return nil, err
	}

	rt := &Runtime{
		initialized:   true,
		registrations: map[string]*Registration{},
		limits:        limits,
		names:         names,
		runtimeID:     runtimeID,
		specs:         map[string]registration{},
		factories:     map[string]actorx.Addr{},
		ledger:        map[string]*creation{},
		perType:       map[string]int{},
		done:          make(chan struct{}),
	}
	if err := initCtx.Err(); err != nil {
		return nil, err
	}
	opts, err := connectionOptions(initCtx, cfg.nats)
	if err != nil {
		return nil, err
	}

	// Business execution and management Actors have independent resource limits.
	rt.life, rt.cancel = context.WithCancel(context.Background())
	rt.execution = actorx.NewLocalChannel()
	rt.business, err = actorx.NewRuntime(initCtx,
		actorx.WithChannel(rt.execution),
		actorx.WithContextFactory(rt.contextFactory),
		actorx.WithMailboxCapacity(limits.execution.MailboxCapacity),
		actorx.WithPendingLimit(limits.execution.Pending),
		actorx.WithTaskLimit(limits.execution.Tasks),
		actorx.WithCallLimit(limits.transport.Requests),
		actorx.WithStreamLimits(8, 64<<10),
		actorx.WithInstanceLimit(limits.execution.Instances),
	)
	if err == nil {
		rt.management, err = actorx.NewRuntime(initCtx,
			actorx.WithMailboxCapacity(limits.managementMailbox()),
			actorx.WithPendingLimit(limits.managementPending()),
			actorx.WithTaskLimit(limits.managementTasks()),
			actorx.WithCallLimit(limits.managementCalls()),
			actorx.WithStreamLimits(1, 4<<10),
			actorx.WithInstanceLimit(limits.managementInstances()),
		)
	}
	if err == nil {
		// Discovery is always local to this Runtime, including an empty directory.
		// Install it before network subscriptions can forward snapshot requests.
		rt.discovery, err = rt.management.Spawn(&discoveryActor{rt: rt, snapshot: rt.snapshotLocal})
	}

	if err == nil && !cfg.localTest {
		err = rt.connect(initCtx, opts)
	}
	if err == nil && rt.net != nil {
		err = rt.net.subscribe(initCtx)
	}
	if err == nil && rt.net != nil {
		rt.invoke = rt.net.invokeBusiness
		rt.openStream = rt.net.openStream
	}
	if err == nil {
		err = initCtx.Err()
	}
	if err != nil {
		_ = rt.Close(context.Background())
		return nil, err
	}

	return rt, nil
}

// Close closes admission synchronously, then joins business execution, creation
// handoffs, Factories and management, in that order. Timeout ends only this wait.
func (rt *Runtime) Close(ctx context.Context) error {
	if rt == nil || !rt.initialized || ctx == nil {
		return actorx.ErrSetup
	}
	rt.beginClose()
	select {
	case <-rt.done:
		return rt.closeErr
	default:
	}
	select {
	case <-rt.done:
		return rt.closeErr
	case <-ctx.Done():
		return fmt.Errorf("dist: Close: %w; local cleanup continues", ctx.Err())
	}
}

func (rt *Runtime) checkOpen() error {
	if rt == nil || !rt.initialized {
		return actorx.ErrSetup
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return actorx.ErrClosed
	}
	return nil
}

func (rt *Runtime) beginClose() {
	rt.once.Do(func() {
		rt.mu.Lock()
		rt.closed = true
		rt.cancel()
		registrations := make([]*Registration, 0, len(rt.registrations))
		for _, r := range rt.registrations {
			registrations = append(registrations, r)
		}
		rt.mu.Unlock()

		for _, r := range registrations {
			r.beginUnregister()
		}
		go func() {
			if rt.net != nil {
				rt.net.closeAdmission()
				rt.net.stopActivations()
			}

			var errs []error
			if rt.business != nil {
				errs = append(errs, rt.business.Close(context.Background()))
			}
			rt.work.Wait() // Management is still alive to receive/discard late results.

			if rt.net != nil {
				rt.net.quiesce()
			}
			for _, r := range registrations {
				<-r.done
				errs = append(errs, r.err)
			}
			if rt.management != nil {
				errs = append(errs, rt.management.Close(context.Background()))
			}
			if rt.net != nil {
				rt.net.close()
			}

			rt.closeErr = errors.Join(errs...)
			close(rt.done)
		}()
	})
}
