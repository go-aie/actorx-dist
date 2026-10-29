package actorxdist

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// removeRevision never follows a changed candidate. Purge has an exclusive
// sequence bound, so a new value written after the marker always survives.
func (r *registry) removeRevision(ctx context.Context, bucket, key string, revision uint64) error {
	err := r.io(ctx, true, func(js nats.JetStreamContext) error {
		kv, err := js.KeyValue(bucket)
		if err == nil {
			err = kv.Delete(key, nats.LastRevision(revision))
		}
		return err
	})
	raw, readErr := r.rawLast(ctx, bucket, key)
	if readErr != nil {
		return errors.Join(err, readErr)
	}
	if raw.Header.Get("KV-Operation") != "DEL" || raw.Sequence <= revision {
		return err
	}
	return r.purgeMarker(ctx, bucket, key, raw.Sequence)
}

func (r *registry) rawLast(ctx context.Context, bucket, key string) (*nats.RawStreamMsg, error) {
	var raw *nats.RawStreamMsg
	err := r.io(ctx, true, func(js nats.JetStreamContext) error {
		var err error
		raw, err = js.GetLastMsg(streamName(bucket), kvSubject(bucket, key))
		return err
	})
	return raw, err
}

func (r *registry) purgeMarker(ctx context.Context, bucket, key string, sequence uint64) error {
	if sequence == 0 || sequence == ^uint64(0) {
		return ErrProtocol
	}
	return r.io(ctx, true, func(js nats.JetStreamContext) error {
		return js.PurgeStream(streamName(bucket), &nats.StreamPurgeRequest{Subject: kvSubject(bucket, key), Sequence: sequence + 1})
	})
}

func (r *registry) cleanupCurrent(ctx context.Context, candidate owned) error {
	v := candidate.value
	if !validOwner(v) || v.State != "INACTIVE" {
		return ErrProtocol
	}
	p, err := r.exit(ctx, v.Key, v.Generation)
	if err != nil {
		return err
	}
	if p.value.Status != "COMMITTED" ||
		p.value.Disposition != "RELEASED" ||
		p.value.Factory != v.Factory ||
		p.value.Digest != v.Digest ||
		p.value.Prepared != v.Prepared ||
		p.value.ReleaseOperation != v.Operation ||
		p.value.ReleaseRevision != candidate.revision {
		return ErrExitUnknown
	}
	return r.removeRevision(ctx, r.names.CurrentBucket, currentKey(v.Key), candidate.revision)
}

func (r *registry) cleanupExit(ctx context.Context, p terminalEntry, created, timeNow time.Time) error {
	v := p.value
	if !validTerminal(v) ||
		v.Status != "COMMITTED" ||
		v.Disposition != "RELEASED" ||
		timeNow.Sub(created) < 24*time.Hour+500*time.Millisecond {
		return nil
	}
	o, err := r.owner(ctx, v.Key)
	if err == nil && o.value.Generation == v.Generation {
		if o.value.State != "INACTIVE" {
			return nil
		}
		if err = r.cleanupCurrent(ctx, o); err != nil {
			return err
		}
	} else if err != nil && !errors.Is(err, nats.ErrKeyNotFound) {
		return err
	}
	return r.removeRevision(ctx, r.names.ExitsBucket, "e."+v.Generation, p.revision)
}

// scanPage has no watch consumer and materializes at most 32 candidates. The
// owner calls at most one page per second; gaps also consume the 64-position budget.
func (r *registry) scanPage(ctx context.Context, index int, now time.Time) error {
	r.scanMu.Lock()
	defer r.scanMu.Unlock()
	bucket := []string{r.names.CurrentBucket, r.names.ExitsBucket}[index]
	var info *nats.StreamInfo
	if err := r.io(ctx, true, func(js nats.JetStreamContext) error {
		var err error
		info, err = js.StreamInfo(streamName(bucket))
		return err
	}); err != nil {
		return err
	}
	cursor := r.cursor[index]
	if cursor < info.State.FirstSeq || cursor > info.State.LastSeq {
		cursor = info.State.FirstSeq
	}
	candidates := make([]*nats.RawStreamMsg, 0, 32)
	for n := 0; n < 64 && len(candidates) < 32 && cursor <= info.State.LastSeq && cursor > 0; n++ {
		seq := cursor
		cursor++
		var raw *nats.RawStreamMsg
		err := r.io(ctx, true, func(js nats.JetStreamContext) error {
			var err error
			raw, err = js.GetMsg(streamName(bucket), seq)
			return err
		})
		if errors.Is(err, nats.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			r.cursor[index] = seq
			return err
		}
		if len(candidates) < 32 && len(raw.Data) <= 16<<10 && strings.HasPrefix(raw.Subject, "$KV."+bucket+".") {
			candidates = append(candidates, raw)
		}
	}
	r.cursor[index] = cursor
	for _, raw := range candidates {
		key := strings.TrimPrefix(raw.Subject, "$KV."+bucket+".")
		if raw.Header.Get("KV-Operation") == "DEL" {
			// Recheck exact latest marker; never purge a newer value by subject alone.
			latest, err := r.rawLast(ctx, bucket, key)
			if err != nil {
				continue
			}
			if latest.Sequence == raw.Sequence && latest.Header.Get("KV-Operation") == "DEL" {
				if err = r.purgeMarker(ctx, bucket, key, raw.Sequence); err != nil {
					return err
				}
			}
			continue
		}
		if raw.Header.Get("KV-Operation") != "" {
			continue
		}
		if index == 0 {
			var v ownership
			if decodeRecord(raw.Data, &v) != nil || !validOwner(v) || currentKey(v.Key) != key || v.State != "INACTIVE" {
				continue
			}
			latest, err := r.owner(ctx, v.Key)
			if err != nil || latest.revision != raw.Sequence {
				continue
			}
			if err = r.cleanupCurrent(ctx, latest); err != nil {
				return err
			}
		} else {
			var v terminal
			if decodeRecord(raw.Data, &v) != nil || !validTerminal(v) || "e."+v.Generation != key {
				continue
			}
			latest, err := r.exit(ctx, v.Key, v.Generation)
			if err != nil || latest.revision != raw.Sequence {
				continue
			}
			if err = r.cleanupExit(ctx, latest, raw.Time, now); err != nil {
				return err
			}
		}
	}
	return nil
}
