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

	actorxremote "github.com/go-aie/actorx-dist/remote/sdk/go"
)

// ChatActor shares its turn count between Call and Stream. The SDK runs both
// methods under the same instance lock, so the actor needs no extra mutex.
type ChatActor struct{ turns int }

type Reply struct {
	Text string `json:"text"`
	Turn int    `json:"turn"`
}

func (a *ChatActor) begin(ctx context.Context, message any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	object, ok := message.(map[string]any)
	if !ok || len(object) != 1 || object["topic"] != "Actors" {
		return errors.New("expected topic Actors")
	}
	a.turns++
	fmt.Printf("Chat turn %d\n", a.turns)
	return nil
}

func (a *ChatActor) Handle(ctx context.Context, message any) (any, error) {
	if err := a.begin(ctx, message); err != nil {
		return nil, err
	}
	return Reply{Text: "Actors use messages to coordinate work.", Turn: a.turns}, nil
}

func (a *ChatActor) HandleStream(ctx context.Context, message any, stream actorxremote.StreamWriter) error {
	if err := a.begin(ctx, message); err != nil {
		return err
	}

	// Simulate an answer arriving in fragments without a model service.
	for _, text := range []string{"Actors", " use", " messages", " to", " coordinate", " work."} {
		if err := stream.Write(Reply{Text: text, Turn: a.turns}); err != nil {
			return err
		}
	}
	return nil
}

func run() (result error) {
	server := flag.String("server", "http://127.0.0.1:8080", "gateway origin")
	actorType := flag.String("type", "demo.chat", "Actor type")
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
		fmt.Println("Chat actor created")
		return &ChatActor{}, nil
	}, actorxremote.WithDescription("Go chat")); err != nil {
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
