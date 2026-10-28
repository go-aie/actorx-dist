package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-aie/actorx-dist"
)

func main() {
	url := flag.String("nats", "nats://127.0.0.1:4222", "JetStream NATS URL")
	role := flag.String("role", "client", "worker or client")
	id := flag.String("id", "orders-42", "logical Counter ID")
	flag.Parse()

	if err := run(*url, *role, *id); err != nil {
		log.Fatal(err)
	}
}

func run(url, role, id string) (result error) {
	options := []actorxdist.Option{actorxdist.WithNATS(url)}
	if role != "worker" && role != "client" {
		return fmt.Errorf("unknown role %q: want worker or client", role)
	}
	rt, err := actorxdist.NewRuntime(context.Background(), options...)
	if err != nil {
		return err
	}

	// Close uses an independent deadline, even if a business request has expired.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result = errors.Join(result, rt.Close(ctx))
	}()

	if role == "worker" {
		if _, err := rt.Register(context.Background(), CounterSpec); err != nil {
			return err
		}
	}

	// Keep the worker available until interrupted, then let deferred Close join its Actors.
	if role != "client" {
		stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		log.Print("Counter worker ready")
		<-stop.Done()
		return nil
	}

	a, err := rt.Addr(CounterSpec.Type, id)
	if err != nil {
		return err
	}

	// Reuse the address so both rounds target the same remote generation.
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		// Send confirms admission; Call returns the accumulated business result.
		if err := a.Send(ctx, Add{N: 2}); err != nil {
			cancel()
			return err
		}

		n, err := a.Call[int](ctx, Add{N: 3})
		cancel()
		if err != nil {
			return err
		}

		fmt.Println(n)
	}

	return nil
}
