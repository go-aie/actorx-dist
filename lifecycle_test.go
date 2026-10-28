package actorxdist

import (
	"context"
	"errors"
	"testing"

	"github.com/go-aie/actorx"
)

func TestLifecycleZero(t *testing.T) {
	var a Addr
	if !errors.Is(a.Stop(nil), actorx.ErrSetup) {
		t.Fatal("zero stop")
	}
	if _, e := a.Wait(context.Background()); !errors.Is(e, actorx.ErrSetup) {
		t.Fatal(e)
	}
}
