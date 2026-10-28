// dist-recover is an offline operator command, not a business Runtime API.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-aie/actorx-dist/internal/recovery"
	"github.com/nats-io/nats.go"
)

type durableAudit struct{ *os.File }

func (a durableAudit) Write(b []byte) (int, error) {
	n, e := a.File.Write(b)
	if e == nil {
		e = a.File.Sync()
	}
	return n, e
}
func run() error {
	url := flag.String("nats", nats.DefaultURL, "NATS URL (supports credentials in URL)")
	typ := flag.String("type", "", "logical type")
	id := flag.String("id", "", "logical ID")
	gen := flag.String("generation", "", "exact generation from inspection")
	rev := flag.Uint64("revision", 0, "exact current revision from inspection")
	apply := flag.Bool("apply", false, "apply conditional recovery; default is read-only inspection")
	stopped := flag.Bool("confirm-stopped", false, "operator has fenced the old process, work and subscriptions")
	reason := flag.String("reason", "", "bounded incident/fencing evidence")
	audit := flag.String("audit", "", "durable local audit path, required with -apply")
	creds := flag.String("creds", "", "NATS credentials file")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opts := []nats.Option{nats.NoReconnect(), nats.ReconnectBufSize(-1), nats.UseOldRequestStyle(), nats.Timeout(2 * time.Second)}
	if *creds != "" {
		opts = append(opts, nats.UserCredentials(*creds))
	}
	nc, err := nats.Connect(*url, opts...)
	if err != nil {
		return err
	}
	defer nc.Close()
	s, err := recovery.Inspect(ctx, nc, recovery.Key{Type: *typ, ID: *id})
	if err != nil {
		return err
	}
	if !*apply {
		return json.NewEncoder(os.Stdout).Encode(s)
	}
	if *audit == "" {
		return fmt.Errorf("-apply requires -audit")
	}
	f, err := os.OpenFile(*audit, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	// Persist a newly created audit name before any ownership mutation, as well
	// as fsyncing each record's contents through durableAudit.Write below.
	dir, err := os.Open(filepath.Dir(*audit))
	if err != nil {
		return err
	}
	err = dir.Sync()
	_ = dir.Close()
	if err != nil {
		return err
	}
	s.Generation = *gen
	s.Revision = *rev
	if err = recovery.Apply(ctx, nc, s, *stopped, *reason, durableAudit{f}); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "recovery complete; see audit for Exit evidence or released-without-exit")
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dist-recover:", err)
		os.Exit(1)
	}
}
