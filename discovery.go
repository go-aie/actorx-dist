package actorxdist

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/go-aie/actorx"
)

// DiscoveryScope selects which Runtime registrations to observe.
type DiscoveryScope uint8

const (
	// DiscoveryLocal observes only this Runtime and is the default scope.
	DiscoveryLocal DiscoveryScope = iota

	// DiscoveryCluster observes snapshots collected from reachable Runtimes.
	DiscoveryCluster
)

// DiscoveryQuery selects the scope and collection window of an observation.
type DiscoveryQuery struct {
	// Scope selects the registrations to observe; zero means this Runtime.
	Scope DiscoveryScope

	// Window bounds cluster collection (default 200 ms, maximum 2 s).
	// Local queries require zero. The caller's context bounds the entire operation.
	Window time.Duration
}

// DiscoveryEndReason describes why a successful observation ended.
type DiscoveryEndReason string

const (
	// DiscoveryEndSnapshot means the local snapshot was returned without truncation.
	DiscoveryEndSnapshot DiscoveryEndReason = "snapshot"

	// DiscoveryEndWindow means the cluster collection window elapsed.
	DiscoveryEndWindow DiscoveryEndReason = "window"

	// DiscoveryEndLimit means a result budget truncated the observation.
	DiscoveryEndLimit DiscoveryEndReason = "limit"
)

// DiscoveryProvider describes one Runtime's registration of an Actor type.
type DiscoveryProvider struct {
	// RuntimeID identifies the Runtime lifetime, changing when it is recreated.
	RuntimeID string

	// Description is that registration's optional human-readable explanation.
	Description string
}

// DiscoveredActor is an observed Actor type, its contract and its providers.
type DiscoveredActor struct {
	// Type is the registered logical type name used for Actor addressing.
	Type string

	// Fingerprint identifies the type's immutable message contract.
	Fingerprint string

	// Providers retains each observed Runtime's registration information.
	Providers []DiscoveryProvider
}

// DiscoveryReport is a bounded observation of ready registrations, not instances.
// Actors are sorted by Type; Providers and Responders are sorted by RuntimeID.
// Its slices belong to the caller. A failed query returns the zero report.
type DiscoveryReport struct {
	// Scope is the scope used for this observation.
	Scope DiscoveryScope

	// Actors contains the observed types and their registration sources.
	Actors []DiscoveredActor

	// Responders includes Runtimes with empty directories; it is not a membership list.
	Responders []string

	// Truncated means a snapshot or report budget omitted entries.
	Truncated bool

	// EndReason describes the successful stopping condition; it is empty on failure.
	EndReason DiscoveryEndReason
}

const (
	maxDiscoveryEntries       = 256
	maxDiscoverySnapshotBytes = 64 << 10
)

// Discover observes this Runtime's ready registrations without constructing or
// activating business Actors. The zero query selects local discovery. Cluster
// discovery is a finite observation, without a complete membership guarantee.
func (rt *Runtime) Discover(ctx context.Context, query DiscoveryQuery) (DiscoveryReport, error) {
	if rt == nil || !rt.initialized || ctx == nil || query.Scope > DiscoveryCluster ||
		query.Window < 0 || query.Window > 2*time.Second ||
		query.Scope == DiscoveryLocal && query.Window != 0 {
		return DiscoveryReport{}, actorx.ErrSetup
	}

	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return DiscoveryReport{}, actorx.ErrClosed
	}
	rt.work.Add(1)
	rt.mu.Unlock()
	defer rt.work.Done()

	if err := ctx.Err(); err != nil {
		return DiscoveryReport{}, err
	}

	operation, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(rt.life, cancel)
	defer stop()

	report, err := rt.discovery.Call[DiscoveryReport](operation, discoverRequest{ctx: operation, query: query})
	if stopped := rt.discoveryStatus(ctx); stopped != nil {
		return DiscoveryReport{}, stopped
	}
	if err != nil {
		return DiscoveryReport{}, err
	}

	return report, nil
}

func (rt *Runtime) discoveryStatus(ctx context.Context) error {
	rt.mu.Lock()
	closed := rt.closed
	rt.mu.Unlock()
	if closed {
		return actorx.ErrClosed
	}

	return ctx.Err()
}

// This local snapshot has no transport addressing or query state. Its JSON
// representation bounds retained description bytes independently of entry count.
type discoverySnapshot struct {
	RuntimeID string           `json:"runtime_id"`
	Entries   []discoveryEntry `json:"entries"`
	Truncated bool             `json:"truncated"`
}

type discoveryEntry struct {
	Type        string `json:"type"`
	Fingerprint string `json:"fingerprint"`
	Description string `json:"description,omitempty"`
}

// jsonArrayBytes includes the separator before every entry except the first.
// Entries contain only strings, so JSON encoding cannot fail.
func (entry discoveryEntry) jsonArrayBytes(index int) int {
	data, _ := json.Marshal(entry)
	size := len(data)
	if index > 0 {
		size++
	}
	return size
}

// jsonBytes charges the snapshot envelope and each entry without encoding the
// whole directory into one buffer. Preserve null versus [] for empty snapshots.
func (snapshot discoverySnapshot) jsonBytes() int {
	entries := snapshot.Entries
	snapshot.Entries = entries[:0]
	data, _ := json.Marshal(snapshot)
	size := len(data)
	for i, entry := range entries {
		size += entry.jsonArrayBytes(i)
	}
	return size
}

func (rt *Runtime) snapshotLocal(ctx context.Context) (discoverySnapshot, error) {
	snapshot := discoverySnapshot{
		RuntimeID: rt.runtimeID,
		Entries:   make([]discoveryEntry, 0, maxDiscoveryEntries),
	}

	// Select and copy registration values under the same lock as installation
	// and withdrawal. Encoding below uses only this bounded, detached snapshot.
	err := func() error {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		if rt.closed {
			return actorx.ErrClosed
		}

		for _, r := range rt.registrations {
			if err := ctx.Err(); err != nil {
				return err
			}
			if rt.life.Err() != nil {
				return actorx.ErrClosed
			}
			if !r.ready || r.draining {
				continue
			}

			entry := discoveryEntry{r.spec.name, r.spec.contract.fingerprint, r.spec.description}
			i, _ := slices.BinarySearchFunc(snapshot.Entries, entry, func(a, b discoveryEntry) int {
				return strings.Compare(a.Type, b.Type)
			})

			// Keep only the sorted first 256 candidates, even while scanning an
			// arbitrarily large registration table. Never copy the whole directory.
			if len(snapshot.Entries) == maxDiscoveryEntries {
				snapshot.Truncated = true
				if i == len(snapshot.Entries) {
					continue
				}
			} else {
				snapshot.Entries = append(snapshot.Entries, discoveryEntry{})
			}
			copy(snapshot.Entries[i+1:], snapshot.Entries[i:len(snapshot.Entries)-1])
			snapshot.Entries[i] = entry
		}

		return nil
	}()
	if err != nil {
		return discoverySnapshot{}, err
	}

	// Encode one bounded entry at a time outside rt.mu. The empty envelope
	// includes false (one byte larger than true), so truncation stays within budget.
	empty := discoverySnapshot{RuntimeID: snapshot.RuntimeID, Entries: []discoveryEntry{}}
	bytes := empty.jsonBytes()
	for i, entry := range snapshot.Entries {
		if err := ctx.Err(); err != nil {
			return discoverySnapshot{}, err
		}

		cost := entry.jsonArrayBytes(i)
		if bytes+cost > maxDiscoverySnapshotBytes {
			snapshot.Entries = snapshot.Entries[:i]
			snapshot.Truncated = true
			break
		}
		bytes += cost
	}

	if err := rt.discoveryStatus(ctx); err != nil {
		return discoverySnapshot{}, err
	}

	return snapshot, nil
}

func (snapshot discoverySnapshot) localReport() DiscoveryReport {
	report := DiscoveryReport{
		Scope:      DiscoveryLocal,
		Actors:     make([]DiscoveredActor, len(snapshot.Entries)),
		Responders: []string{snapshot.RuntimeID},
		Truncated:  snapshot.Truncated,
		EndReason:  DiscoveryEndSnapshot,
	}
	if snapshot.Truncated {
		report.EndReason = DiscoveryEndLimit
	}

	for i, entry := range snapshot.Entries {
		report.Actors[i] = DiscoveredActor{
			Type:        entry.Type,
			Fingerprint: entry.Fingerprint,
			Providers:   []DiscoveryProvider{{RuntimeID: snapshot.RuntimeID, Description: entry.Description}},
		}
	}

	return report
}
