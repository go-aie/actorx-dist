package actorxdist

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/go-aie/actorx"
)

func TestLimitOptionsDefaultsAndReplacement(t *testing.T) {
	e := ExecutionLimits{Types: 12, Creating: 20, CreatingPerType: 10, Instances: 2, MailboxCapacity: 3}
	x := WithExecutionLimits(e)
	e.Instances = -1 // The option owns a value snapshot.
	rt := testRuntime(t,
		WithExecutionLimits(ExecutionLimits{Pending: 7, Tasks: 8}), x,
		WithTransportLimits(TransportLimits{Requests: 3, Streams: 1, ContractCacheTypes: 1024}),
		WithTransportLimits(TransportLimits{Requests: 5}),
	)
	if rt.limits.execution != (ExecutionLimits{Types: 12, Creating: 20, CreatingPerType: 10, Instances: 2, MailboxCapacity: 3, Pending: 32, Tasks: 4}) ||
		rt.limits.transport != (TransportLimits{Requests: 5, Streams: 16, ContractCacheTypes: 64}) {
		t.Fatal(rt.limits)
	}
	defaults := testRuntime(t, WithExecutionLimits(ExecutionLimits{}), WithTransportLimits(TransportLimits{}))
	if defaults.limits.execution != (ExecutionLimits{Creating: 16, CreatingPerType: 4, Instances: 64, MailboxCapacity: 16, Pending: 32, Tasks: 4}) || defaults.limits.transport != (TransportLimits{Requests: 128, Streams: 16, ContractCacheTypes: 64}) {
		t.Fatal(defaults.limits)
	}
}

func TestInvalidLimitsBeforeConnection(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for name, option := range map[string]Option{
		"Types":              WithExecutionLimits(ExecutionLimits{Types: -1}),
		"Creating":           WithExecutionLimits(ExecutionLimits{Creating: -1}),
		"CreatingPerType":    WithExecutionLimits(ExecutionLimits{CreatingPerType: -1}),
		"Instances":          WithExecutionLimits(ExecutionLimits{Instances: -1}),
		"MailboxCapacity":    WithExecutionLimits(ExecutionLimits{MailboxCapacity: -1}),
		"Pending":            WithExecutionLimits(ExecutionLimits{Pending: -1}),
		"Tasks":              WithExecutionLimits(ExecutionLimits{Tasks: -1}),
		"Requests":           WithTransportLimits(TransportLimits{Requests: -1}),
		"Streams":            WithTransportLimits(TransportLimits{Streams: -1}),
		"ContractCacheTypes": WithTransportLimits(TransportLimits{ContractCacheTypes: -1}),
		"cache overflow":     WithTransportLimits(TransportLimits{ContractCacheTypes: maxInt/maxCachedContractBytes + 1}),
		"instance overflow":  WithExecutionLimits(ExecutionLimits{Instances: maxInt}),
		"mailbox overflow":   WithExecutionLimits(ExecutionLimits{MailboxCapacity: maxInt}),
		"pending overflow":   WithExecutionLimits(ExecutionLimits{Pending: maxInt}),
		"task overflow":      WithExecutionLimits(ExecutionLimits{Tasks: maxInt}),
		"request overflow":   WithTransportLimits(TransportLimits{Requests: maxInt}),
		"stream overflow":    WithTransportLimits(TransportLimits{Streams: maxInt}),
		"type overflow":      WithExecutionLimits(ExecutionLimits{Types: maxInt}),
		"creating overflow":  WithExecutionLimits(ExecutionLimits{Creating: maxInt}),
		"per-type overflow":  WithExecutionLimits(ExecutionLimits{CreatingPerType: maxInt}),
	} {
		t.Run(name, func(t *testing.T) {
			rt, err := NewRuntime(testContext(t), option, WithNATS("nats://127.0.0.1:1"))
			if rt != nil || !errors.Is(err, actorx.ErrSetup) ||
				!strings.Contains(err.Error(), strings.Fields(name)[0]) && !strings.Contains(name, "overflow") {
				t.Fatal(rt, err)
			}
		})
	}
}

func TestContractCacheLimits(t *testing.T) {
	maxCapacity := int(^uint(0)>>1) / maxCachedContractBytes
	for _, capacity := range []int{1, 1024, maxCapacity} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			transport := TransportLimits{ContractCacheTypes: capacity}
			option := WithTransportLimits(transport)
			transport.ContractCacheTypes = -1
			cfg := runtimeOptions{}
			WithTransportLimits(TransportLimits{Requests: 1, Streams: 1})(&cfg)
			option(&cfg)
			limits, err := normalizeLimits(cfg.execution, cfg.transport)
			if err != nil || limits.transport != (TransportLimits{Requests: 128, Streams: 16, ContractCacheTypes: capacity}) {
				t.Fatal(limits, err)
			}
			cache := newContractCache(limits.transport.ContractCacheTypes)
			if len(cache.types) != 0 || len(cache.pool) != 0 || cache.residentBytes != 0 {
				t.Fatal("capacity must not reserve contract storage")
			}
		})
	}
}

func TestConfiguredTypeCapacity(t *testing.T) {
	for _, capacity := range []int{0, 1, 12} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			count := capacity
			if count == 0 {
				count = 12 // Default registration is no longer capped at eight.
			}
			options := []any{WithExecutionLimits(ExecutionLimits{Types: capacity, Instances: 1})}
			for i := range count {
				options = append(options, counterRegistration(fmt.Sprintf("type%d", i)))
			}
			rt := testRuntime(t, options...)
			if len(rt.factories) != count {
				t.Fatal("missing Factories", len(rt.factories))
			}
			// Type registration does not consume business instance capacity.
			mustCreate(t, rt, ActorKey{fmt.Sprintf("type%d", count-1), "one"}, token(t))
		})
	}

	rt := testRuntime(t, WithExecutionLimits(ExecutionLimits{Types: 1}), counterRegistration("one"))
	if _, err := rt.Register(testContext(t), ActorSpec[*counter]{Type: "two", New: func(ActorKey) *counter { return &counter{} }}); !errors.Is(err, actorx.ErrFull) {
		t.Fatal(err)
	}

}

func TestConfiguredInstanceCapacity(t *testing.T) {
	for _, capacity := range []int{1, 65} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			rt := testRuntime(t, WithExecutionLimits(ExecutionLimits{Instances: capacity}), counterRegistration("counter"))
			var first *creation
			for i := range capacity {
				entry := mustCreate(t, rt, ActorKey{"counter", fmt.Sprint(i)}, token(t))
				if i == 0 {
					first = entry
				}
			}
			if _, err := rt.createLocal(testContext(t), ActorKey{"counter", "overflow"}, token(t)); !errors.Is(err, actorx.ErrFull) {
				t.Fatal("activation bypassed Instances", err)
			}
			if _, err := rt.business.Spawn(&counter{}); !errors.Is(err, actorx.ErrFull) {
				t.Fatal("local instance bypassed Instances", err)
			}
			if err := rt.releaseLocal(testContext(t), first.key, first.generation); err != nil {
				t.Fatal(err)
			}
			mustCreate(t, rt, ActorKey{"counter", "replacement"}, token(t))
		})
	}
}

func TestConfiguredInstanceCapacityRetainsFailedCreation(t *testing.T) {
	rt := testRuntime(t, WithExecutionLimits(ExecutionLimits{Instances: 1}), registerSpec(ActorSpec[*counter]{
		Type: "counter", New: func(ActorKey) *counter { return nil },
	}))
	key, gen := ActorKey{"counter", "failed"}, token(t)
	if _, err := rt.createLocal(testContext(t), key, gen); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}
	if _, err := rt.createLocal(testContext(t), ActorKey{"counter", "next"}, token(t)); !errors.Is(err, actorx.ErrFull) {
		t.Fatal("held failure released activation capacity", err)
	}
	if err := rt.releaseLocal(testContext(t), key, gen); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.createLocal(testContext(t), key, token(t)); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal("explicit release did not restore capacity", err)
	}
}

type limitHold struct{}
type limitDeferred struct{}
type limitTasks struct{ Count int }
type limitStream struct{}

type limitProbe struct {
	running chan struct{}
	unblock <-chan struct{}
	replies chan actorx.Reply[int]
}

func (p *limitProbe) Bindings() []actorx.Binding {
	return []actorx.Binding{
		actorx.Handle(p.hold),
		actorx.HandleDeferred(p.deferReply),
		actorx.Handle(p.tasks),
		actorx.HandleStream(p.stream),
		actorx.Handle(func(actorx.Context, read) (int, error) { return 1, nil }),
	}
}

func (p *limitProbe) hold(ctx actorx.Context, _ limitHold) (int, error) {
	close(p.running)
	select {
	case <-p.unblock:
		return 1, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (p *limitProbe) deferReply(_ actorx.Context, _ limitDeferred, reply actorx.Reply[int]) error {
	p.replies <- reply
	return nil
}

func (p *limitProbe) tasks(ctx actorx.Context, msg limitTasks) (int, error) {
	unblock := p.unblock
	for i := range msg.Count {
		_, err := actorx.StartTask(ctx, func(taskCtx context.Context) (int, error) {
			select {
			case <-unblock:
				return 1, nil
			case <-taskCtx.Done():
				return 0, taskCtx.Err()
			}
		}, func(int, error) bool { return true }, nil)
		if err != nil {
			if errors.Is(err, actorx.ErrFull) {
				return i, nil
			}
			return 0, err
		}
	}
	return msg.Count, nil
}

func (p *limitProbe) stream(ctx actorx.Context, _ limitStream, reply actorx.StreamReply[int]) error {
	unblock := p.unblock
	_, err := actorx.StartTask(ctx, func(taskCtx context.Context) (int, error) {
		if err := reply.Write(taskCtx, 1, 8); err != nil {
			return 0, err
		}
		select {
		case <-unblock:
			return 1, nil
		case <-taskCtx.Done():
			return 0, taskCtx.Err()
		}
	}, func(_ int, err error) bool { reply.Complete(err); return true }, nil)
	return err
}

func TestConfiguredMailboxAndPending(t *testing.T) {
	t.Run("mailbox", func(t *testing.T) {
		rt := testRuntime(t, WithExecutionLimits(ExecutionLimits{MailboxCapacity: 1}))
		unblock := make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(unblock) }) }
		defer release()
		p := &limitProbe{running: make(chan struct{}), unblock: unblock}
		addr, err := rt.business.Spawn(p)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		ctx := testContext(t)
		go func() { _, err := addr.Call[int](ctx, limitHold{}); done <- err }()
		await(t, p.running)
		if err := addr.Send(testContext(t), read{}); err != nil {
			t.Fatal(err)
		}
		if err := addr.Send(testContext(t), read{}); !errors.Is(err, actorx.ErrFull) {
			t.Fatal("mailbox accepted a second queued message", err)
		}
		release()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("pending", func(t *testing.T) {
		rt := testRuntime(t, WithExecutionLimits(ExecutionLimits{Pending: 1}))
		p := &limitProbe{replies: make(chan actorx.Reply[int], 1)}
		addr, err := rt.business.Spawn(p)
		if err != nil {
			t.Fatal(err)
		}
		ctx := testContext(t)
		done := make(chan error, 1)
		go func() { _, err := addr.Call[int](ctx, limitDeferred{}); done <- err }()
		select {
		case reply := <-p.replies:
			if _, err := addr.Call[int](ctx, read{}); !errors.Is(err, actorx.ErrFull) {
				t.Fatal("Deferred request did not retain Pending", err)
			}
			reply.Complete(1, nil)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if _, err := addr.Call[int](ctx, read{}); err != nil {
			t.Fatal("completion did not restore Pending", err)
		}
	})
}

func TestRequestsSizesCoreCalls(t *testing.T) {
	rt := testRuntime(t, WithTransportLimits(TransportLimits{Requests: 1}))
	p := &limitProbe{replies: make(chan actorx.Reply[int], 1)}
	addr, err := rt.business.Spawn(p)
	if err != nil {
		t.Fatal(err)
	}
	ctx := testContext(t)
	done := make(chan error, 1)
	go func() { _, err := addr.Call[int](ctx, limitDeferred{}); done <- err }()
	select {
	case reply := <-p.replies:
		if _, err := addr.Call[int](ctx, read{}); !errors.Is(err, actorx.ErrFull) {
			t.Fatal("Requests did not bound core caller associations", err)
		}
		reply.Complete(1, nil)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestConfiguredTaskLimitAndCompletionReservation(t *testing.T) {
	for _, limits := range []ExecutionLimits{
		{MailboxCapacity: 8, Tasks: 2},
		{MailboxCapacity: 2, Tasks: 8},
	} {
		t.Run(fmt.Sprintf("mailbox%d-tasks%d", limits.MailboxCapacity, limits.Tasks), func(t *testing.T) {
			rt := testRuntime(t, WithExecutionLimits(limits))
			unblock := make(chan struct{})
			defer close(unblock)
			addr, err := rt.business.Spawn(&limitProbe{unblock: unblock})
			if err != nil {
				t.Fatal(err)
			}
			if count, err := addr.Call[int](testContext(t), limitTasks{3}); count != 2 || err != nil {
				t.Fatal("task/completion reservation exceeded its limit", count, err)
			}
		})
	}
}
