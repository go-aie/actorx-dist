package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	actorxremote "github.com/go-aie/actorx-dist/remote/sdk/go"
)

func run() (result error) {
	server := flag.String("server", "http://127.0.0.1:8080", "gateway origin")
	actorType := flag.String("type", "demo.counter", "Actor type")
	id := flag.String("id", "orders-42", "logical Actor ID")
	flag.Parse()
	rt, err := actorxremote.NewRuntime(*server, actorxremote.Options{APIKey: os.Getenv("REMOTE_API_KEY")})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result = errors.Join(result, rt.Close(cleanup))
	}()

	ctx := context.Background()
	actor, err := rt.Addr(*actorType, *id)
	if err != nil {
		return err
	}

	// Reusing this Addr keeps adding to the same remote counter.
	for range 2 {
		value, err := actor.Call(ctx, map[string]int{"n": 5})
		if err != nil {
			return err
		}
		fmt.Println(value)
	}

	// A different logical ID has an independent actor and starts at zero.
	other, err := rt.Addr(*actorType, *id+"-other")
	if err != nil {
		return err
	}
	value, err := other.Call(ctx, map[string]int{"n": 5})
	if err != nil {
		return err
	}
	fmt.Println(value)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
