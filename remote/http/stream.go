package remotehttp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-aie/actorx-dist/remote"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

func (h *handler) control(w http.ResponseWriter, ctx context.Context, m protocol.Message) {
	err := h.gateway.Control(ctx, m)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	status := http.StatusBadRequest
	if errors.Is(err, remote.ErrSession) {
		status = http.StatusForbidden
	} else if errors.Is(err, remote.ErrCorrelation) {
		status = http.StatusConflict
	}
	http.Error(w, "Stream control rejected", status)
}

func (h *handler) stream(w http.ResponseWriter, ctx context.Context, m protocol.Message) {
	params, e := m.Parameters(protocol.ToGateway)
	if e != nil {
		writeRPC(w, protocol.ErrorResponse(m.ID, e))
		return
	}
	request := params.(protocol.CallParams)

	// Keep the original request budget across all frames and credit waits.
	deadline := time.Now().Add(budget)
	if request.DeadlineUnixMS != nil {
		deadline = time.Now().Add(60 * time.Second)
		if requested := time.UnixMilli(*request.DeadlineUnixMS); requested.Before(deadline) {
			deadline = requested
		}
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	deadline, _ = ctx.Deadline()

	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Now().Add(budget)); err != nil {
		http.Error(w, "SSE write deadlines unavailable", 500)
		return
	}
	defer rc.SetWriteDeadline(time.Time{})
	stream, err := h.gateway.OpenStream(ctx, m.ID, request)
	if err != nil {
		writeRPC(w, protocol.ErrorResponse(m.ID, rpcError(err)))
		return
	}
	defer stream.Close()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	write := func(m protocol.Message) error {
		raw, err := m.Encode()
		if err != nil {
			return err
		}
		// A slow socket gets a per-frame write budget within the stream deadline.
		frameDeadline := time.Now().Add(budget)
		if deadline.Before(frameDeadline) {
			frameDeadline = deadline
		}
		if err = rc.SetWriteDeadline(frameDeadline); err != nil {
			return err
		}
		if _, err = fmt.Fprintf(w, "data: %s\n\n", raw); err != nil {
			return err
		}
		if err = rc.Flush(); err != nil {
			return err
		}
		return rc.SetWriteDeadline(time.Time{})
	}

	if write(stream.Open()) != nil {
		return
	}
	terminal := stream.Run(write)
	_ = write(terminal)
}
