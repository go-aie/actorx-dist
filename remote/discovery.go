package remote

import (
	"context"
	"sort"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

// This exact immutable Call+Stream bridge contract declares remote/v1
// compatibility. It is not provider authentication or per-instance method support.
// TestRemoteBridgeFingerprint checks it against a real Runtime registration.
const remoteV1Fingerprint = "9c43b5a6f9be0d9a72f1210fdc44d2411b6b360b620cf4cdbd60ca807b6c4d31"

// Actors observes compatible Remote registrations across reachable Runtimes
// without activating instances. The report is a finite cluster observation.
func (g *Gateway) Actors(ctx context.Context) (protocol.ActorsResult, error) {
	if g == nil || ctx == nil {
		return protocol.ActorsResult{}, actorx.ErrSetup
	}

	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return protocol.ActorsResult{}, mapError(actorx.ErrClosed, nil)
	}
	if g.backend.discover == nil {
		g.mu.Unlock()
		return protocol.ActorsResult{}, actorx.ErrSetup
	}
	// Admission and Close share the lock, so every admitted query is counted
	// before Close starts waiting for discovery work to finish.
	g.discoveryWork.Add(1)
	g.mu.Unlock()
	defer g.discoveryWork.Done()

	operation, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(g.discoveryLife, cancel)
	defer stop()
	report, err := g.backend.discover(operation, actorxdist.DiscoveryQuery{
		Scope: actorxdist.DiscoveryCluster,
	})

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return protocol.ActorsResult{}, mapError(actorx.ErrClosed, nil)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return protocol.ActorsResult{}, mapError(err, nil)
	}

	// The Remote directory has no partial-result shape. A known truncation
	// must fail even when the omitted entries might be native types.
	if report.Truncated {
		return protocol.ActorsResult{}, mapError(actorx.ErrFull, nil)
	}
	return remoteActors(report), nil
}

func remoteActors(report actorxdist.DiscoveryReport) protocol.ActorsResult {
	result := protocol.ActorsResult{Actors: make([]protocol.Actor, 0, len(report.Actors))}
	for _, a := range report.Actors {
		if a.Fingerprint != remoteV1Fingerprint || len(a.Providers) == 0 {
			continue
		}

		// Provider descriptions may differ without a contract conflict. Expose
		// a description only when all observed providers agree.
		description := a.Providers[0].Description
		for _, provider := range a.Providers[1:] {
			if provider.Description != description {
				description = ""
				break
			}
		}
		result.Actors = append(result.Actors, protocol.Actor{
			Type:        a.Type,
			Description: description,
			Protocol:    protocol.Version,
		})
	}
	sort.Slice(result.Actors, func(i, j int) bool { return result.Actors[i].Type < result.Actors[j].Type })
	return result
}
