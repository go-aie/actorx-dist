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
	role := flag.String("role", "client", "button, counter or client")
	id := flag.String("id", "clicks-42", "logical Button ID")
	flag.Parse()

	if err := run(*url, *role, *id); err != nil {
		log.Fatal(err)
	}
}

func run(url, role, id string) (result error) {
	options := []actorxdist.Option{actorxdist.WithNATS(url)}
	if role != "button" && role != "counter" && role != "client" {
		return fmt.Errorf("unknown role %q: want button, counter or client", role)
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

	switch role {
	case "button":
		if _, err := rt.Register(context.Background(), ButtonSpec); err != nil {
			return err
		}
	case "counter":
		if _, err := rt.Register(context.Background(), CounterSpec); err != nil {
			return err
		}
	}

	// Each worker serves only its registered type until interrupted.
	if role != "client" {
		stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		log.Printf("%s worker ready", role)
		<-stop.Done()
		return nil
	}

	a, err := rt.Addr(ButtonSpec.Type, id)
	if err != nil {
		return err
	}

	// Each click crosses two process boundaries: client → Button → Counter.
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		n, err := a.Call[int](ctx, Click{})
		cancel()
		if err != nil {
			return err
		}

		fmt.Println(n)
	}

	return nil
}
