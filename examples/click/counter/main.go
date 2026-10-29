package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/examples/click"
)

func main() {
	url := flag.String("nats", "nats://127.0.0.1:4222", "JetStream NATS URL")
	flag.Parse()

	if err := run(*url); err != nil {
		log.Fatal(err)
	}
}

func run(url string) (result error) {
	stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rt, err := actorxdist.NewRuntime(stop, actorxdist.WithNATS(url))
	if err != nil {
		return err
	}
	defer func() {
		// Close must join owned Actors even after the signal context is canceled.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result = errors.Join(result, rt.Close(ctx))
	}()

	if _, err := rt.Register(stop, click.CounterSpec); err != nil {
		return err
	}

	log.Print("counter provider ready")
	<-stop.Done()
	return nil
}
