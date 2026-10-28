package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote"
	remotehttp "github.com/go-aie/actorx-dist/remote/http"
)

func main() {
	natsURL := flag.String("nats", "nats://127.0.0.1:4222", "internal JetStream NATS URL")
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
	flag.Parse()

	if err := run(*natsURL, *listen); err != nil {
		log.Fatal(err)
	}
}

func run(natsURL, listen string) (result error) {
	key := os.Getenv("REMOTE_API_KEY")
	if key == "" {
		return errors.New("REMOTE_API_KEY is required")
	}

	stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rt, err := actorxdist.NewRuntime(stop, actorxdist.WithNATS(natsURL))
	if err != nil {
		return err
	}
	defer func() {
		// Cleanup must still run after the signal context has been canceled.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result = errors.Join(result, rt.Close(ctx))
	}()

	gateway, err := remote.NewGateway(rt)
	if err != nil {
		return err
	}
	var server *http.Server
	defer func() {
		// Close registrations first so provider SSE streams can end, then join
		// HTTP handlers. The earlier defer closes the borrowed Runtime last.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result = errors.Join(result, gateway.Close(ctx))
		cancel()

		if server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				result = errors.Join(result, err, server.Close())
			}
		}
	}()

	handler, err := remotehttp.NewHandler(gateway, remotehttp.Options{APIKey: key})
	if err != nil {
		return err
	}

	// Each SSE frame has a write deadline in remote/http. A server-wide short
	// WriteTimeout would terminate healthy provider registrations.
	server = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}

	exited := make(chan error, 1)
	go func() { exited <- server.Serve(listener) }()
	fmt.Printf("gateway ready listen=%s\n", listener.Addr())

	select {
	case <-stop.Done():
		return nil
	case err := <-exited:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
