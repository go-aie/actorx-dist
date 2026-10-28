package actorxdist

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/go-aie/actorx-dist/internal/strictjson"
	"github.com/nats-io/nats.go"
)

const (
	typesBucket   = "AD_TYPES_V1"
	currentBucket = "AD_ACTIVATIONS_V1"
	exitsBucket   = "AD_EXITS_V1"
)

var bucketConfigs = []nats.KeyValueConfig{
	{
		Bucket:       typesBucket,
		MaxValueSize: 64 << 10,
		MaxBytes:     16 << 20,
		History:      1,
		Storage:      nats.FileStorage,
		Replicas:     1,
	},
	{
		Bucket:       currentBucket,
		MaxValueSize: 16 << 10,
		MaxBytes:     128 << 20,
		History:      1,
		Storage:      nats.FileStorage,
		Replicas:     1,
	},
	{
		Bucket:       exitsBucket,
		MaxValueSize: 16 << 10,
		MaxBytes:     128 << 20,
		History:      1,
		Storage:      nats.FileStorage,
		Replicas:     1,
	},
}

// registry is an ordinary component. Callers already own a bounded worker.
// Each operation owns one SDK slot and a fresh context-bound JS/KV handle.
type registry struct {
	nc       *nats.Conn
	ordinary chan struct{}
	all      chan struct{}

	scanMu sync.Mutex
	cursor [2]uint64

	// Test-only transport fault boundary: before send / after durable reply.
	// Installed before concurrent use; no production callback or retry goroutine.
	writeFault  func(stage, bucket, key string) error
	readFault   func(bucket, key string) error
	recordFault func(stage, bucket, key string, data []byte) // process-crash tests only
}

func newRegistry(nc *nats.Conn) *registry {
	return &registry{nc: nc, ordinary: make(chan struct{}, 6), all: make(chan struct{}, 8)}
}

func (r *registry) io(ctx context.Context, control bool, work func(nats.JetStreamContext) error) error {
	if ctx == nil {
		return actorx.ErrSetup
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	// Ordinary work leaves two SDK slots available for control and cleanup.
	if !control {
		select {
		case r.ordinary <- struct{}{}:
			defer func() { <-r.ordinary }()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case r.all <- struct{}{}:
		defer func() { <-r.all }()
	case <-ctx.Done():
		return ctx.Err()
	}

	js, err := r.nc.JetStream(nats.Context(ctx))
	if err != nil {
		return err
	}
	return work(js)
}

func streamName(bucket string) string { return "KV_" + bucket }

func kvSubject(bucket, key string) string { return "$KV." + bucket + "." + key }

func currentKey(key ActorKey) string {
	return "a." + identity.Token(key.Type) + "." + identity.Token(key.ID)
}

func (r *registry) initialize(ctx context.Context) error {
	for _, cfg := range bucketConfigs {
		err := r.io(ctx, true, func(js nats.JetStreamContext) error {
			info, err := js.StreamInfo(streamName(cfg.Bucket))
			if errors.Is(err, nats.ErrStreamNotFound) {
				// Avoid CreateKeyValue's compatibility auto-update on an existing bucket.
				// AddStream is create/idempotent only and never rewrites configuration.
				want := nats.StreamConfig{
					Name:              streamName(cfg.Bucket),
					Subjects:          []string{"$KV." + cfg.Bucket + ".>"},
					Retention:         nats.LimitsPolicy,
					Storage:           nats.FileStorage,
					Replicas:          1,
					MaxMsgs:           -1,
					MaxConsumers:      -1,
					MaxMsgsPerSubject: 1,
					MaxBytes:          cfg.MaxBytes,
					MaxMsgSize:        cfg.MaxValueSize,
					Discard:           nats.DiscardNew,
					AllowRollup:       true,
					DenyDelete:        true,
					AllowDirect:       true,
					Duplicates:        2 * time.Minute,
				}
				info, err = js.AddStream(&want)
				if err != nil {
					info, err = js.StreamInfo(want.Name)
				}
			}
			if err != nil {
				return err
			}
			c := info.Config
			if c.Name != streamName(cfg.Bucket) ||
				!reflect.DeepEqual(c.Subjects, []string{"$KV." + cfg.Bucket + ".>"}) ||
				c.Retention != nats.LimitsPolicy ||
				c.Storage != nats.FileStorage ||
				c.Discard != nats.DiscardNew ||
				c.MaxMsgsPerSubject != 1 ||
				c.MaxMsgs != -1 ||
				c.MaxAge != 0 ||
				c.MaxBytes != cfg.MaxBytes ||
				c.MaxMsgSize != cfg.MaxValueSize ||
				c.Replicas != 1 ||
				!c.AllowRollup ||
				!c.DenyDelete ||
				c.DenyPurge ||
				c.Sealed ||
				c.Mirror != nil ||
				len(c.Sources) != 0 ||
				c.RePublish != nil ||
				c.SubjectTransform != nil ||
				c.Compression != nats.NoCompression {
				return fmt.Errorf("%w: incompatible KV configuration %s", actorx.ErrSetup, cfg.Bucket)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *registry) get(ctx context.Context, bucket, key string, control bool) (nats.KeyValueEntry, error) {
	var entry nats.KeyValueEntry
	if r.readFault != nil {
		if err := r.readFault(bucket, key); err != nil {
			return nil, err
		}
	}
	err := r.io(ctx, control, func(js nats.JetStreamContext) error {
		kv, err := js.KeyValue(bucket)
		if err == nil {
			entry, err = kv.Get(key)
		}
		return err
	})
	return entry, err
}

func (r *registry) write(ctx context.Context, bucket, key string, data []byte, revision uint64, control bool) (uint64, error) {
	max := 16 << 10
	if bucket == typesBucket {
		max = 64 << 10
	}
	if len(data) > max {
		return 0, actorx.ErrChunkSize
	}
	var rev uint64
	err := r.io(ctx, control, func(js nats.JetStreamContext) error {
		kv, err := js.KeyValue(bucket)
		if err != nil {
			return err
		}
		if r.writeFault != nil {
			if err = r.writeFault("before", bucket, key); err != nil {
				return err
			}
		}
		if r.recordFault != nil {
			r.recordFault("before", bucket, key, data)
		}
		if revision == 0 {
			rev, err = kv.Create(key, data)
		} else {
			rev, err = kv.Update(key, data, revision)
		}
		if err == nil && r.writeFault != nil {
			err = r.writeFault("after", bucket, key)
		}
		if err == nil && r.recordFault != nil {
			r.recordFault("after", bucket, key, data)
		}
		return err
	})
	return rev, err
}

// writeVerified never refreshes an expected revision. Lost acknowledgements are
// resolved only by the exact persisted bytes, including the caller's operation ID.
func (r *registry) writeVerified(ctx context.Context, bucket, key string, data []byte, revision uint64, control bool) (uint64, error) {
	rev, err := r.write(ctx, bucket, key, data, revision, control)
	if err == nil {
		return rev, nil
	}
	entry, readErr := r.get(ctx, bucket, key, control)
	if readErr == nil && bytes.Equal(entry.Value(), data) {
		return entry.Revision(), nil
	}
	return 0, errors.Join(err, readErr)
}

func (r *registry) capacity(ctx context.Context, types bool) error {
	return r.io(ctx, false, func(js nats.JetStreamContext) error {
		buckets := []string{currentBucket, exitsBucket}
		percent := uint64(70)
		if types {
			buckets = []string{typesBucket}
			percent = 90
		}
		for _, bucket := range buckets {
			info, err := js.StreamInfo(streamName(bucket))
			if err != nil {
				return errors.Join(actorx.ErrFull, err)
			}
			if info.Config.MaxBytes <= 0 || info.State.Bytes*100 >= uint64(info.Config.MaxBytes)*percent {
				return fmt.Errorf("%w: %s soft storage watermark", actorx.ErrFull, bucket)
			}
		}
		info, err := js.AccountInfo()
		if err != nil {
			return errors.Join(actorx.ErrFull, err)
		}
		if info.Limits.MaxStore <= 0 || info.Store*100 >= uint64(info.Limits.MaxStore)*70 {
			return fmt.Errorf("%w: account storage watermark or missing quota", actorx.ErrFull)
		}
		return nil
	})
}

func (r *registry) publishType(ctx context.Context, name string, c wireContract) error {
	key := "t." + identity.Token(name)
	existing, err := r.get(ctx, typesBucket, key, false)
	if err == nil {
		if !bytes.Equal(existing.Value(), c.data) {
			return ErrContract
		}
		return nil
	}
	if !errors.Is(err, nats.ErrKeyNotFound) {
		return err
	}
	if err = r.capacity(ctx, true); err != nil {
		return err
	}
	_, err = r.writeVerified(ctx, typesBucket, key, c.data, 0, false)
	if err != nil {
		existing, readErr := r.get(ctx, typesBucket, key, false)
		if readErr == nil {
			if bytes.Equal(existing.Value(), c.data) {
				return nil
			}
			return ErrContract
		}
	}
	return err
}

type ownership struct {
	Version     int      `json:"version"`
	Operation   string   `json:"operation"`
	Key         ActorKey `json:"key"`
	Generation  string   `json:"generation"`
	Factory     string   `json:"factory"`
	Fingerprint string   `json:"fingerprint"`
	State       string   `json:"state"`
	ExitKey     string   `json:"exit_key,omitempty"`
	Digest      string   `json:"digest,omitempty"`
	Prepared    uint64   `json:"prepared_revision,omitempty"`
}

type owned struct {
	value    ownership
	revision uint64
}

func opaque(s string) bool {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(s)
	return err == nil && len(raw) == 16 && len(s) == 22
}

func validOwner(v ownership) bool {
	if v.Version != 1 ||
		identity.Validate(v.Key.Type, v.Key.ID) != nil ||
		!opaque(v.Generation) ||
		!opaque(v.Factory) ||
		!opaque(v.Operation) ||
		!validDigest(v.Fingerprint) {
		return false
	}
	switch v.State {
	case "STARTING", "READY", "STOPPING":
		return v.ExitKey == "" && v.Digest == "" && v.Prepared == 0
	case "FAILED", "INACTIVE":
		return v.ExitKey == "e."+v.Generation && validDigest(v.Digest) && v.Prepared > 0
	}
	return false
}

func decodeRecord(data []byte, out any) error {
	if strictjson.Validate(data, 16<<10, 16, 1024) != nil {
		return ErrProtocol
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return ErrProtocol
	}
	return nil
}

func (r *registry) owner(ctx context.Context, key ActorKey) (owned, error) {
	entry, err := r.get(ctx, currentBucket, currentKey(key), true)
	if err != nil {
		return owned{}, err
	}
	var v ownership
	if decodeRecord(entry.Value(), &v) != nil || !validOwner(v) || v.Key != key {
		return owned{}, ErrProtocol
	}
	return owned{v, entry.Revision()}, nil
}

// claim accepts an observed INACTIVE revision or absence, never an active Get.
// The candidate and operation are retained by its caller across uncertain writes.
func (r *registry) claim(ctx context.Context, v ownership, previous *owned) (owned, error) {
	if !validOwner(v) || v.State != "STARTING" {
		return owned{}, ErrProtocol
	}
	expected := uint64(0)
	if previous != nil {
		if !validOwner(previous.value) || previous.value.Key != v.Key || previous.value.State != "INACTIVE" || previous.revision == 0 {
			return owned{}, ErrActivationUnknown
		}
		expected = previous.revision
	}
	if err := r.capacity(ctx, false); err != nil {
		return owned{}, err
	}
	data, _ := json.Marshal(v)
	rev, err := r.writeVerified(ctx, currentBucket, currentKey(v.Key), data, expected, false)
	if err != nil {
		return owned{}, errors.Join(ErrActivationUnknown, err)
	}
	return owned{v, rev}, nil
}

func sameGeneration(a, b ownership) bool {
	return a.Key == b.Key && a.Generation == b.Generation && a.Factory == b.Factory && a.Fingerprint == b.Fingerprint
}

func (r *registry) change(ctx context.Context, old owned, next ownership) (owned, error) {
	if old.revision == 0 ||
		!validOwner(old.value) ||
		!validOwner(next) ||
		!sameGeneration(old.value, next) ||
		old.value.State == "INACTIVE" {
		return owned{}, ErrActivationUnknown
	}

	allowed := old.value.State == next.State ||
		(old.value.State == "STARTING" && (next.State == "READY" || next.State == "STOPPING" || next.State == "FAILED")) ||
		(old.value.State == "READY" && next.State == "STOPPING") ||
		((old.value.State == "STOPPING" || old.value.State == "FAILED") && next.State == "INACTIVE")
	if old.value.State == "FAILED" &&
		next.State == "FAILED" &&
		(old.value.Digest != next.Digest || old.value.Prepared != next.Prepared || old.value.ExitKey != next.ExitKey) {
		return owned{}, ErrProtocol
	}
	if !allowed {
		return owned{}, ErrProtocol
	}

	data, _ := json.Marshal(next)
	rev, err := r.writeVerified(ctx, currentBucket, currentKey(next.Key), data, old.revision, true)
	if err != nil {
		return owned{}, errors.Join(ErrActivationUnknown, err)
	}
	return owned{next, rev}, nil
}

func (r *registry) confirm(ctx context.Context, candidate owned, factory string) (owned, error) {
	if candidate.value.Factory != factory || candidate.value.State == "INACTIVE" || !validOwner(candidate.value) {
		return owned{}, ErrActivationUnknown
	}
	next := candidate.value
	var err error
	next.Operation, err = identity.NewOpaque()
	if err != nil {
		return owned{}, err
	}
	return r.change(ctx, candidate, next)
}
