package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/examples/click"
)

func main() {
	url := flag.String("nats", "nats://127.0.0.1:4222", "JetStream NATS URL")
	id := flag.String("id", "clicks-42", "logical Button ID")
	flag.Parse()

	if err := run(*url, *id); err != nil {
		log.Fatal(err)
	}
}

func run(url, id string) (result error) {
	rt, err := actorxdist.NewRuntime(context.Background(), actorxdist.WithNATS(url))
	if err != nil {
		return err
	}

	// Close uses an independent deadline, even if a business request has expired.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result = errors.Join(result, rt.Close(ctx))
	}()

	a, err := rt.Addr(click.ButtonSpec.Type, id)
	if err != nil {
		return err
	}

	// Each click crosses two process boundaries: consumer → Button → Counter.
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		n, err := a.Call[int](ctx, click.Click{})
		cancel()
		if err != nil {
			return err
		}

		fmt.Println(n)
	}

	return nil
}
