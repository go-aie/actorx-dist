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
	actorType := flag.String("type", "demo.chat", "Actor type")
	id := flag.String("id", "assistant-42", "logical Actor ID")
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

	message := struct {
		Topic string `json:"topic"`
	}{"Actors"}

	// Both operations use the same remote actor instance.
	answer, err := actor.Call(ctx, message)
	if err != nil {
		return err
	}
	fmt.Printf("call turn: %v\n", answer.(map[string]any)["turn"])

	chunks, err := actor.Stream(ctx, message)
	if err != nil {
		return err
	}
	// Close releases the stream even if iteration exits early.
	defer func() { result = errors.Join(result, chunks.Close()) }()

	fmt.Print("answer: ")
	for chunks.Next() {
		value := chunks.Value().(map[string]any)
		fmt.Print(value["text"])
	}
	fmt.Println()
	return chunks.Err()
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
