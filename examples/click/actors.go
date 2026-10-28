//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen
package main

import (
	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
)

type Add struct {
	N int `json:"n"`
}

//actorx:actor
type Counter struct {
	count int
}

func (c *Counter) Add(_ actorx.Context, m Add) (int, error) {
	c.count += m.N
	return c.count, nil
}

var CounterSpec = actorxdist.ActorSpec[*Counter]{
	Type: "click.counter",
	New: func(actorxdist.ActorKey) *Counter {
		return &Counter{}
	},
}

type Click struct{}

//actorx:actor
type Button struct {
	counterID string
}

func (b *Button) Click(ctx actorx.Context, _ Click) (int, error) {
	target, err := actorxdist.MustFrom(ctx).Addr(CounterSpec.Type, b.counterID)
	if err != nil {
		return 0, err
	}

	// Carry the original request's deadline and cancellation into the next hop.
	return target.Call[int](ctx, Add{N: 1})
}

// ButtonSpec pairs each Button with a Counter using the same logical ID.
var ButtonSpec = actorxdist.ActorSpec[*Button]{
	Type: "click.button",
	New: func(key actorxdist.ActorKey) *Button {
		return &Button{counterID: key.ID}
	},
}
