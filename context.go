package actorxdist

import (
	"reflect"

	"github.com/go-aie/actorx"
)

// Context adds optional distributed addressing to the common execution context.
// Runtime supplies it to business handlers and their local descendants. Do not
// retain it across requests; retain Addr values for later communication instead.
type Context interface {
	actorx.Context
	Addr(actorType string, id ...string) (Addr, error)
	Self() (Addr, error)
}

// From detects distributed capability without I/O or core ownership inspection.
// A nil, typed-nil, or pure core context returns nil, false. A local child may
// have this capability without a logical Self.
func From(ctx actorx.Context) (Context, bool) {
	distributed, ok := ctx.(Context)
	if !ok || distributed == nil {
		return nil, false
	}
	value := reflect.ValueOf(distributed)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return nil, false
		}
	}
	return distributed, true
}

// MustFrom asserts required distributed capability. Missing capability is an
// integration error; addressing and communication errors still return normally.
func MustFrom(ctx actorx.Context) Context {
	if distributed, ok := From(ctx); ok {
		return distributed
	}
	panic("dist: missing distributed context")
}

// executionActor forwards the validated binding snapshot, without reading the
// original business Actor again. Only logical activations install fixed Self.
type executionActor struct {
	actorx.Actor
	self Addr
}

type distributedContext struct {
	actorx.Context
	rt   *Runtime
	self Addr
}

func (rt *Runtime) contextFactory(value actorx.Actor) (actorx.ContextWrapper, error) {
	var self Addr
	if instance, ok := value.(*executionActor); ok {
		if instance == nil || instance.self.IsZero() || instance.self.rt != rt || instance.self.generation() == "" {
			return nil, actorx.ErrSetup
		}
		self = instance.self
	}
	return func(base actorx.Context) actorx.Context {
		return &distributedContext{Context: base, rt: rt, self: self}
	}, nil
}

// Addr creates an independent logical handle, subject to Runtime closure.
func (ctx *distributedContext) Addr(actorType string, id ...string) (Addr, error) {
	if ctx == nil || ctx.rt == nil {
		return Addr{}, actorx.ErrSetup
	}
	return ctx.rt.Addr(actorType, id...)
}

// Self reads this activation's fixed identity, including after Runtime closure.
// Success does not prove READY or liveness. Local children have no logical Self.
func (ctx *distributedContext) Self() (Addr, error) {
	if ctx == nil || ctx.self.IsZero() {
		return Addr{}, actorx.ErrSetup
	}
	return ctx.self, nil
}
