package actorxdist

import (
	"fmt"

	"github.com/go-aie/actorx"
)

// ExecutionLimits configures Actor type, instance and execution capacity on one
// Runtime. Zero fields select defaults; negative fields are invalid.
type ExecutionLimits struct {
	// Types bounds locally registered Actor types. The default is unlimited.
	// It does not bound the consumer's known-contract cache or other Runtimes.
	Types int
	// Creating bounds concurrent instance creation and startup across all types.
	// The default is 16; creation also occupies an Instances slot.
	Creating int
	// CreatingPerType bounds concurrent instance creation and startup per type.
	// The default is 4; Creating and Instances can impose smaller bounds.
	CreatingPerType int
	// Instances bounds both business instances (including local children) and
	// held distributed activations. Each budget is counted independently;
	// unfinished startup, failures and cleanup can continue to occupy capacity.
	// The default is 64.
	Instances int
	// MailboxCapacity bounds queued work plus reserved task completions per Actor.
	// A request leaves the queue before its handler runs. The default is 16.
	MailboxCapacity int
	// Pending bounds admitted, unsettled requests per Actor, including Send and
	// Deferred requests whose handlers have returned. The default is 32.
	Pending int
	// Tasks bounds tasks per Actor until result delivery or discard completes.
	// The default is 4.
	Tasks int
}

// TransportLimits bounds network work and cached contracts on one Runtime.
// Zero fields select defaults; negative fields are invalid. Limits are ceilings, not reservations:
// other execution, buffer and storage budgets can reject work first.
type TransportLimits struct {
	// Requests bounds outgoing and incoming unsettled Send/Call/Stream requests
	// separately. It also sizes core business Call/Stream associations and shared
	// reply correlations (which include activation/control requests).
	// The default is 128.
	Requests int
	// Streams bounds incoming and outgoing streams combined, including active
	// consumption callbacks. Each stream also uses request capacity.
	// The default is 16; the wire window remains four items / 64 KiB per stream.
	Streams int
	// ContractCacheTypes bounds cached logical Type entries. The default is 64.
	// Shared canonical contracts each occupy at most 64 KiB, so resident contract
	// bytes are bounded by ContractCacheTypes * 64 KiB, excluding index/object
	// overhead and in-flight snapshots. Capacity does not preallocate that memory.
	ContractCacheTypes int
}

// WithExecutionLimits snapshots Actor capacity limits. The last declaration
// replaces the whole previous group; zero fields then select defaults.
func WithExecutionLimits(limits ExecutionLimits) Option {
	return func(cfg *runtimeOptions) { cfg.execution = limits }
}

// WithTransportLimits snapshots transport limits. The last declaration replaces
// the whole previous group; zero fields then select defaults.
func WithTransportLimits(limits TransportLimits) Option {
	return func(cfg *runtimeOptions) { cfg.transport = limits }
}

type runtimeLimits struct {
	execution ExecutionLimits
	transport TransportLimits
}

func normalizeLimits(e ExecutionLimits, t TransportLimits) (runtimeLimits, error) {
	fields := []struct {
		name  string
		value *int
		def   int
	}{
		{"Types", &e.Types, 0},
		{"Creating", &e.Creating, 16},
		{"CreatingPerType", &e.CreatingPerType, 4},
		{"Instances", &e.Instances, 64},
		{"MailboxCapacity", &e.MailboxCapacity, 16},
		{"Pending", &e.Pending, 32},
		{"Tasks", &e.Tasks, 4},
		{"Requests", &t.Requests, 128},
		{"Streams", &t.Streams, 16},
		{"ContractCacheTypes", &t.ContractCacheTypes, 64},
	}
	for _, field := range fields {
		if *field.value < 0 {
			return runtimeLimits{}, fmt.Errorf("%w: %s must be nonnegative", actorx.ErrSetup, field.name)
		}
		if *field.value == 0 {
			*field.value = field.def
		}
	}

	// Check aggregate slot and byte arithmetic before allocating any resource or
	// reading declarations. Capacities remain finite, including on 32-bit hosts.
	maxInt := int(^uint(0) >> 1)
	if e.Types == maxInt {
		return runtimeLimits{}, fmt.Errorf("%w: management instance capacity overflow", actorx.ErrSetup)
	}

	limits := runtimeLimits{e, t}
	for _, factors := range [][]int{
		{max(1, e.Types), 2, 1 << 20}, // per-Type subscription pending bytes
		{e.Creating, 2},
		{e.CreatingPerType, 2},
		{e.Instances, e.MailboxCapacity},
		{e.Instances, e.Pending},
		{e.Instances, e.Tasks},
		{e.Instances, limits.replyFrames(), 128 << 10}, // business subscription pending bytes
		{t.Requests, 2, 128 << 10},                     // request queues and reply buffers
		{t.Streams, streamItems, streamBytes},
		{t.ContractCacheTypes, maxCachedContractBytes},
	} {
		product := 1
		for _, factor := range factors {
			if product > maxInt/factor {
				return runtimeLimits{}, fmt.Errorf("%w: resource capacity overflow", actorx.ErrSetup)
			}
			product *= factor
		}
	}
	if max(1, limits.managementInstances()) > maxInt/limits.managementPending() {
		return runtimeLimits{}, fmt.Errorf("%w: management capacity overflow", actorx.ErrSetup)
	}
	return limits, nil
}

// Internal capacities retain the default profile's headroom and grow with the
// public request/stream ceilings. Dispatch workers and Registry I/O stay bounded
// independently; changing concurrency does not change wire or storage contracts.
func (l runtimeLimits) sendACKs() int { return (l.transport.Requests + 1) / 2 }
func (l runtimeLimits) payloads() int { return max(32, (l.transport.Requests+3)/4) }
func (l runtimeLimits) controls() int { return max(64, (l.transport.Requests+1)/2) }
func (l runtimeLimits) replyFrames() int {
	return max(8, (l.transport.Requests+15)/16, (l.transport.Streams+1)/2)
}
func (l runtimeLimits) waiters() int { return max(256, 2*l.transport.Requests) }

// Factories need completion slots for every admitted creation plus independent
// headroom for control/cleanup tasks. Business Tasks does not size management.
func (l runtimeLimits) managementTasks() int {
	return max(8, min(l.execution.Creating, l.execution.CreatingPerType, l.execution.Instances)+4)
}
func (l runtimeLimits) managementMailbox() int { return max(64, 2*l.managementTasks()) }
func (l runtimeLimits) managementPending() int { return max(128, 2*l.managementTasks()) }
func (l runtimeLimits) managementCalls() int   { return max(64, l.execution.Creating) }

// Discovery has one reserved system instance; zero still means unlimited Types.
func (l runtimeLimits) managementInstances() int {
	if l.execution.Types == 0 {
		return 0
	}
	return l.execution.Types + 1
}
