package actorxdist

import (
	"context"
	"reflect"

	"github.com/go-aie/actorx"
)

// invokeLocal is the internal local integration probe. Production business
// delivery uses LocalChannel.Deliver/Responder after wire validation.
func invokeLocal(
	ctx context.Context,
	local actorx.Addr,
	descriptions []actorx.CallDescriptor,
	kind actorx.Kind,
	msg any,
	result reflect.Type,
	yield func(any) error,
) (any, error) {
	var selected *actorx.CallDescriptor
	for i := range descriptions {
		if descriptions[i].MessageType() == reflect.TypeOf(msg) {
			selected = &descriptions[i]
			break
		}
	}
	if selected == nil {
		return nil, actorx.ErrUnhandled
	}
	if err := selected.Check(msg, kind); err != nil {
		return nil, err
	}
	if kind != actorx.SendKind && selected.ResultType() != result {
		if selected.ResultType() != nil || result != reflect.TypeFor[any]() {
			return nil, actorx.ErrReplyType
		}
	}

	switch kind {
	case actorx.SendKind:
		return nil, local.Send(ctx, msg)
	case actorx.CallKind:
		return local.Call[any](ctx, msg)
	case actorx.StreamKind:
		if yield == nil {
			return nil, actorx.ErrSetup
		}
		return nil, local.Stream[any](ctx, msg, yield)
	default:
		return nil, actorx.ErrMode
	}
}
