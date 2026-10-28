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
	id := flag.String("id", "assistant-42", "logical Assistant ID")
	delay := flag.Duration("consume-delay", 0, "delay in each client chunk callback (e.g. 100ms)")
	flag.Parse()

	if err := run(*url, *role, *id, *delay); err != nil {
		log.Fatal(err)
	}
}

func run(url, role, id string, delay time.Duration) (result error) {
	if delay < 0 {
		return errors.New("consume-delay must not be negative")
	}
	options := []actorxdist.Option{actorxdist.WithNATS(url)}
	if role != "worker" && role != "client" {
		return fmt.Errorf("unknown role %q: want worker or client", role)
	}

	stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	rt, err := actorxdist.NewRuntime(stop, options...)
	if err != nil {
		return err
	}
	defer func() {
		// Close must still join owned work after a request or signal is canceled.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result = errors.Join(result, rt.Close(ctx))
	}()

	if role == "worker" {
		if _, err := rt.Register(stop, AssistantSpec); err != nil {
			return err
		}
	}

	if role == "worker" {
		log.Print("Assistant worker ready")
		<-stop.Done()
		return nil
	}

	a, err := rt.Addr(AssistantSpec.Type, id)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(stop, 10*time.Second)
	defer cancel()

	if _, err := fmt.Print("answer: "); err != nil {
		return err
	}
	chunks := 0
	if err := a.Stream[TextChunk](ctx, Ask{Topic: "Actors"}, func(chunk TextChunk) error {
		if _, err := fmt.Print(chunk.Text); err != nil {
			return err
		}
		chunks++
		if delay > 0 {
			// A chunk remains unconsumed until this callback returns.
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}); err != nil {
		return err
	}
	_, err = fmt.Printf("\nchunks: %d\n", chunks)
	return err
}
