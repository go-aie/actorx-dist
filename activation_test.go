package actorxdist

import (
	"context"
	"testing"
	"time"
)

func TestRequestBudgetUsesOneDeadline(t *testing.T) {
	ctx, cancel := requestBudget(context.Background())
	defer cancel()
	remaining := time.Until(deadline(ctx))
	if remaining < 4*time.Second || remaining > 5*time.Second {
		t.Fatal(remaining)
	}
	same, stop := requestBudget(ctx)
	defer stop()
	if !deadline(same).Equal(deadline(ctx)) {
		t.Fatal("reset budget")
	}
	longer, end := context.WithTimeout(context.Background(), time.Hour)
	defer end()
	bounded, done := requestBudget(longer)
	defer done()
	if time.Until(deadline(bounded)) > 60*time.Second {
		t.Fatal("unbounded request")
	}
}
