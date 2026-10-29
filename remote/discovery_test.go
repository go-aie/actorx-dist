package remote

import (
	"context"
	"os"
	"testing"

	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

func TestRemoteBridgeFingerprint(t *testing.T) {
	url := os.Getenv("ACTORX_REMOTE_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_REMOTE_NATS_URL")
	}
	rt, err := actorxdist.NewRuntime(context.Background(), actorxdist.WithNATS(url))
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close(context.Background())

	g, err := NewGateway(rt)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close(context.Background())

	if _, err := g.Register(context.Background(), protocol.RegisterParams{Type: "bridge", Description: "description"}); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Register(context.Background(), actorxdist.ActorSpec[*legacyCallProxy]{
		Type: "legacy-call",
		New:  func(actorxdist.ActorKey) *legacyCallProxy { return &legacyCallProxy{} },
	}); err != nil {
		t.Fatal(err)
	}

	report, err := rt.Discover(context.Background(), actorxdist.DiscoveryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Actors) != 2 ||
		report.Actors[0].Fingerprint != remoteV1Fingerprint ||
		report.Actors[0].Providers[0].Description != "description" {
		t.Fatalf("bridge contract or description changed: %+v", report)
	}

	got := remoteActors(report)
	if len(got.Actors) != 1 || got.Actors[0].Type != "bridge" {
		t.Fatal("legacy Call-only bridge was exposed", got, err)
	}
}

func TestRemoteDirectoryMapping(t *testing.T) {
	report := actorxdist.DiscoveryReport{
		Scope:      actorxdist.DiscoveryCluster,
		Responders: []string{"a", "b", "empty"},
		Truncated:  true,
		EndReason:  actorxdist.DiscoveryEndWindow,
		Actors: []actorxdist.DiscoveredActor{
			{
				Type:        "native",
				Fingerprint: "different",
				Providers:   []actorxdist.DiscoveryProvider{{RuntimeID: "a", Description: "native"}},
			},
			{
				Type:        "same",
				Fingerprint: remoteV1Fingerprint,
				Providers: []actorxdist.DiscoveryProvider{
					{RuntimeID: "a", Description: "same"},
					{RuntimeID: "b", Description: "same"},
				},
			},
			{
				Type:        "different",
				Fingerprint: remoteV1Fingerprint,
				Providers: []actorxdist.DiscoveryProvider{
					{RuntimeID: "a", Description: "first"},
					{RuntimeID: "b", Description: "second"},
				},
			},
		},
	}
	got := remoteActors(report)
	if len(got.Actors) != 2 ||
		got.Actors[0].Type != "different" ||
		got.Actors[0].Description != "" ||
		got.Actors[1].Description != "same" ||
		got.Actors[0].Protocol != protocol.Version {
		t.Fatal(got)
	}

	got.Actors[0].Description = "mutated"
	if remoteActors(report).Actors[0].Description != "" {
		t.Fatal("directory exposed mutable storage")
	}
}

func TestDirectoryFailureAndClose(t *testing.T) {
	entered := make(chan struct{})
	g := newGateway(backend{discover: func(ctx context.Context, q actorxdist.DiscoveryQuery) (actorxdist.DiscoveryReport, error) {
		close(entered)
		<-ctx.Done()
		return actorxdist.DiscoveryReport{}, ctx.Err()
	}})
	done := make(chan error, 1)
	go func() {
		_, err := g.Actors(testCtx(t))
		done <- err
	}()
	<-entered
	if err := g.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	rpcCode(t, await(t, done), protocol.Stopped)
	if _, err := g.Actors(testCtx(t)); err == nil {
		t.Fatal("closed gateway accepted discovery")
	}

	// Discovery failure takes precedence over any partial report it returned.
	failed := newGateway(backend{discover: func(context.Context, actorxdist.DiscoveryQuery) (actorxdist.DiscoveryReport, error) {
		return actorxdist.DiscoveryReport{Truncated: true}, actorxdist.ErrOwnerUnavailable
	}})
	defer failed.Close(testCtx(t))
	got, err := failed.Actors(testCtx(t))
	rpcCode(t, err, protocol.OwnerUnavailable)
	if got.Actors != nil {
		t.Fatal("returned partial report", got)
	}
}

func TestDirectoryUsesClusterDescriptionWithoutGatewayFilter(t *testing.T) {
	g := newGateway(backend{
		register: func(_ context.Context, p protocol.RegisterParams, _ *session) (registrationOwner, error) {
			if p.Description != "registration description" {
				t.Error("description lost before ActorSpec", p)
			}
			return &fakeOwner{}, nil
		},
		discover: func(_ context.Context, query actorxdist.DiscoveryQuery) (actorxdist.DiscoveryReport, error) {
			if query.Scope != actorxdist.DiscoveryCluster || query.Window != 0 {
				t.Fatal("directory must use default cluster observation", query)
			}
			return actorxdist.DiscoveryReport{
				Scope:      actorxdist.DiscoveryCluster,
				Responders: []string{"r"},
				EndReason:  actorxdist.DiscoveryEndWindow,
				Actors: []actorxdist.DiscoveredActor{
					{
						Type:        "own",
						Fingerprint: remoteV1Fingerprint,
						Providers:   []actorxdist.DiscoveryProvider{{RuntimeID: "r", Description: "core snapshot description"}},
					},
					{
						Type:        "other-gateway",
						Fingerprint: remoteV1Fingerprint,
						Providers:   []actorxdist.DiscoveryProvider{{RuntimeID: "r"}},
					},
				},
			}, nil
		},
	})
	defer g.Close(testCtx(t))
	if _, err := g.Register(testCtx(t), protocol.RegisterParams{Type: "own", Description: "registration description"}); err != nil {
		t.Fatal(err)
	}

	got, err := g.Actors(testCtx(t))
	if err != nil ||
		len(got.Actors) != 2 ||
		got.Actors[1].Type != "own" ||
		got.Actors[1].Description != "core snapshot description" {
		t.Fatal(got, err)
	}
}

func TestDirectoryRejectsTruncatedReports(t *testing.T) {
	for _, reason := range []actorxdist.DiscoveryEndReason{actorxdist.DiscoveryEndWindow, actorxdist.DiscoveryEndLimit} {
		t.Run(string(reason), func(t *testing.T) {
			g := newGateway(backend{discover: func(context.Context, actorxdist.DiscoveryQuery) (actorxdist.DiscoveryReport, error) {
				return actorxdist.DiscoveryReport{
					Scope:     actorxdist.DiscoveryCluster,
					Truncated: true,
					EndReason: reason,
					Actors: []actorxdist.DiscoveredActor{{
						Type:        "partial",
						Fingerprint: remoteV1Fingerprint,
						Providers:   []actorxdist.DiscoveryProvider{{RuntimeID: "r"}},
					}},
				}, nil
			}})
			defer g.Close(testCtx(t))
			got, err := g.Actors(testCtx(t))
			rpcCode(t, err, protocol.Full)
			if got.Actors != nil {
				t.Fatal("truncated directory returned", got)
			}
		})
	}
}
