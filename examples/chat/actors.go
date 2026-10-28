//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen
package main

import (
	"context"
	"strings"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
)

type Ask struct {
	Topic string `json:"topic"`
}

type TextChunk struct {
	Text string `json:"text"`
}

// Assistant streams a simulated answer without a model or credentials.
//
//actorx:actor
type Assistant struct{}

func (a *Assistant) Answer(ctx actorx.Context, msg Ask, stream actorx.StreamReply[TextChunk]) error {
	// Write may wait for a slow reader, so produce chunks outside the handler.
	words := strings.Fields(msg.Topic + " use messages to coordinate work.")
	_, err := actorx.StartTask(ctx,
		func(work context.Context) (struct{}, error) {
			for i, word := range words {
				text := word
				if i > 0 {
					text = " " + text
				}
				if err := stream.Write(work, TextChunk{Text: text}, len(text)); err != nil {
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		},
		func(_ struct{}, err error) bool {
			stream.Complete(err)
			return true
		},
		nil,
	)
	return err
}

var AssistantSpec = actorxdist.ActorSpec[*Assistant]{
	Type: "chat.assistant",
	New: func(actorxdist.ActorKey) *Assistant {
		return &Assistant{}
	},
}
