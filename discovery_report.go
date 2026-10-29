package actorxdist

import (
	"encoding/json"
	"slices"
	"strings"
)

const (
	maxDiscoveryQueries     = 4
	maxDiscoveryResponders  = 256
	maxDiscoverySources     = 4096
	maxDiscoveryReportBytes = 1 << 20
)

// All mutations belong to the Discovery Actor. A node is validated and charged
// in full before committing anything, including its responder ID.
type discoveryBuilder struct {
	nodes  map[string]discoverySnapshot // canonical snapshots for duplicate checks
	actors map[string]*DiscoveredActor

	// Sources counts providers across all types; bytes charges the encoded report.
	sources, bytes int
	truncated      bool
}

func newDiscoveryBuilder() *discoveryBuilder {
	empty, _ := json.Marshal(DiscoveryReport{
		Scope:      DiscoveryCluster,
		Actors:     []DiscoveredActor{},
		Responders: []string{},
		EndReason:  DiscoveryEndWindow,
	})
	return &discoveryBuilder{
		nodes:  make(map[string]discoverySnapshot),
		actors: make(map[string]*DiscoveredActor),
		bytes:  len(empty),
	}
}

// merge returns false only when the whole node would exceed the report budget.
// Truncated snapshots still count as responders and do not end collection.
func (b *discoveryBuilder) merge(snapshot discoverySnapshot) (bool, error) {
	if !opaque(snapshot.RuntimeID) || !validDiscoveryEntries(snapshot.Entries) {
		return false, ErrProtocol
	}
	if snapshot.jsonBytes() > maxDiscoverySnapshotBytes {
		return false, ErrProtocol
	}

	entries := slices.Clone(snapshot.Entries)
	slices.SortFunc(entries, func(a, b discoveryEntry) int {
		return strings.Compare(a.Type, b.Type)
	})
	snapshot.Entries = entries
	if old, ok := b.nodes[snapshot.RuntimeID]; ok {
		if old.Truncated != snapshot.Truncated || !slices.Equal(old.Entries, entries) {
			return false, ErrProtocol
		}
		return true, nil
	}

	// Check every contract before checking capacity, so a contradictory node
	// cannot be disguised as an ordinary truncation.
	id, _ := json.Marshal(snapshot.RuntimeID)
	cost, newActors := len(id), 0
	if len(b.nodes) > 0 {
		cost++
	}
	for _, entry := range entries {
		provider := DiscoveryProvider{RuntimeID: snapshot.RuntimeID, Description: entry.Description}
		if actor := b.actors[entry.Type]; actor != nil {
			if actor.Fingerprint != entry.Fingerprint {
				return false, ErrContract
			}
			data, _ := json.Marshal(provider)
			cost += len(data) + 1 // provider comma
		} else {
			data, _ := json.Marshal(DiscoveredActor{entry.Type, entry.Fingerprint, []DiscoveryProvider{provider}})
			cost += len(data)
			if len(b.actors)+newActors > 0 {
				cost++
			}
			newActors++
		}
	}
	if len(b.nodes)+1 > maxDiscoveryResponders ||
		b.sources+len(entries) > maxDiscoverySources ||
		b.bytes+cost > maxDiscoveryReportBytes {
		return false, nil
	}

	// Commit only after validation and all budget checks succeed. A rejected
	// node must leave neither a responder ID nor a partial set of providers.
	b.nodes[snapshot.RuntimeID] = snapshot
	b.bytes += cost // false and window reserve more bytes than true and limit
	b.sources += len(entries)
	b.truncated = b.truncated || snapshot.Truncated
	for _, entry := range entries {
		actor := b.actors[entry.Type]
		if actor == nil {
			actor = &DiscoveredActor{Type: entry.Type, Fingerprint: entry.Fingerprint}
			b.actors[entry.Type] = actor
		}
		actor.Providers = append(actor.Providers, DiscoveryProvider{snapshot.RuntimeID, entry.Description})
	}

	return true, nil
}

func (b *discoveryBuilder) report(reason DiscoveryEndReason) DiscoveryReport {
	report := DiscoveryReport{
		Scope:      DiscoveryCluster,
		Actors:     make([]DiscoveredActor, 0, len(b.actors)),
		Responders: make([]string, 0, len(b.nodes)),
		EndReason:  reason,
		Truncated:  b.truncated || reason == DiscoveryEndLimit,
	}
	for id := range b.nodes {
		report.Responders = append(report.Responders, id)
	}
	slices.Sort(report.Responders)

	// Detach nested provider slices before sorting or handing them to the caller.
	for _, actor := range b.actors {
		copy := *actor
		copy.Providers = slices.Clone(actor.Providers)
		slices.SortFunc(copy.Providers, func(a, b DiscoveryProvider) int {
			return strings.Compare(a.RuntimeID, b.RuntimeID)
		})
		report.Actors = append(report.Actors, copy)
	}
	slices.SortFunc(report.Actors, func(a, b DiscoveredActor) int {
		return strings.Compare(a.Type, b.Type)
	})
	return report
}
