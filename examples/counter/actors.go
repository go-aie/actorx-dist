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
	Type: "counter",
	New: func(actorxdist.ActorKey) *Counter {
		return &Counter{}
	},
}
