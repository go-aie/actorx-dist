package actorxdist

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/nats-io/nats.go"
)

func TestNATSOptionValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	opts, err := connectionOptions(ctx, natsConfig{})
	if err != nil ||
		opts.Url != "nats://127.0.0.1:4222" ||
		opts.ReconnectBufSize != -1 ||
		!opts.UseOldRequestStyle ||
		opts.SubChanLen != 8 {
		t.Fatal(opts, err)
	}

	for _, cfg := range []natsConfig{
		{url: "bad"},
		{url: "http://localhost:1"},
		{url: "nats://host:0"},
		{url: "nats://host/a"},
		{options: []nats.Option{nil}},
		{options: []nats.Option{nats.ClosedHandler(func(*nats.Conn) {})}},
		{options: []nats.Option{nats.NoEcho()}},
		{options: []nats.Option{func(*nats.Options) error { panic("bad option") }}},
	} {
		if _, err := connectionOptions(ctx, cfg); !errors.Is(err, actorx.ErrSetup) {
			t.Fatal(cfg, err)
		}
	}

	// WithNATS copies the options and replaces the previous declaration in full.
	original := []nats.Option{nats.Name("first")}
	option := WithNATS("nats://host:1234", original...)
	original[0] = nil
	cfg := runtimeOptions{}
	WithNATS("bad", nil)(&cfg)
	option(&cfg)
	opts, err = connectionOptions(ctx, cfg.nats)
	if err != nil || opts.Name != "first" || opts.Url != "nats://host:1234" {
		t.Fatal(opts, err)
	}
}

func TestReadyFaultWinsOverLateReconnect(t *testing.T) {
	var state readiness
	old := state.epoch()
	state.Store(false)
	if state.resume(old) || state.Load() {
		t.Fatal("late resume erased newer fault")
	}

	current := state.epoch()
	if !state.resume(current) || !state.Load() {
		t.Fatal("current epoch did not resume")
	}

	state.Store(false)
	if state.Load() {
		t.Fatal("fault did not close admission")
	}
}
