package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	actorxremote "github.com/go-aie/actorx-dist/remote/sdk/go"
)

// Counter keeps one total per logical ID. The SDK serializes Handle calls.
type Counter struct{ count int64 }

func (a *Counter) Handle(ctx context.Context, message any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Incoming JSON objects use map[string]any and json.Number, even when
	// the consumer sends a Go struct.
	object, ok := message.(map[string]any)
	if !ok || len(object) != 1 {
		return nil, errors.New("expected integer n")
	}
	number, ok := object["n"].(json.Number)
	if !ok {
		return nil, errors.New("expected integer n")
	}
	n, err := number.Int64()
	if err != nil {
		return nil, err
	}

	a.count += n
	fmt.Printf("Counter: %d\n", a.count)
	return a.count, nil
}

func run() (result error) {
	server := flag.String("server", "http://127.0.0.1:8080", "gateway origin")
	actorType := flag.String("type", "demo.counter", "Actor type")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rt, err := actorxremote.NewRuntime(*server, actorxremote.Options{APIKey: os.Getenv("REMOTE_API_KEY")})
	if err != nil {
		return err
	}
	defer func() {
		// A signal cancels ctx; closing still needs its own cleanup budget.
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result = errors.Join(result, rt.Close(cleanup))
	}()

	// Register installs the session; the factory runs only on first activation.
	if err := rt.Register(ctx, *actorType, func() (any, error) {
		fmt.Println("Counter created")
		return &Counter{}, nil
	}, actorxremote.WithDescription("Go counter")); err != nil {
		return err
	}

	fmt.Printf("Registered %s\n", *actorType)
	if err := rt.WaitClosed(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Provider closed")
}
