//go:build sim

package sim

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/suraciii/gor"
)

func TestClassifyOutcomeUsesStableCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "node dead", err: fmt.Errorf("forwarded node failure: %w", gor.ErrNodeDead), want: "cluster-node-dead"},
		{name: "persistence conflict", err: fmt.Errorf("forwarded conflict: %w", gor.ErrPersistenceConflict), want: "store-conflict"},
		{name: "persistence failed", err: fmt.Errorf("forwarded write failure: %w", gor.ErrPersistenceFailed), want: "store-write-unknown"},
		{name: "runtime closed", err: fmt.Errorf("forwarded shutdown: %w", gor.ErrRuntimeClosed), want: "closed"},
		{name: "overloaded", err: fmt.Errorf("forwarded queue failure: %w", gor.ErrOverloaded), want: "overloaded"},
		{name: "no owner", err: fmt.Errorf("forwarded routing failure: %w", gor.ErrNoOwner), want: "wrong-owner"},
		{name: "canceled", err: fmt.Errorf("caller stopped waiting: %w", context.Canceled), want: "canceled"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := classifyOutcome(test.err)
			if err != nil {
				t.Fatalf("classifyOutcome() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("classifyOutcome() = %q, want %q", got, test.want)
			}
		})
	}

	got, err := classifyOutcome(errors.New(gor.ErrNoOwner.Error()))
	if err == nil || got != "" {
		t.Fatalf("text-only error classified as %q with error %v, want unclassified", got, err)
	}

	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "local write failure", err: errWriteFailure, want: "store-write-error"},
		{name: "local applied write failure", err: errAppliedWriteFailure, want: "store-write-applied-then-error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := classifyOutcome(test.err)
			if err != nil || got != test.want {
				t.Fatalf("classifyOutcome() = (%q, %v), want (%q, nil)", got, err, test.want)
			}
		})
	}

	if output := counterOperationOutputFor(7, fmt.Errorf("remote diagnostic: %w", gor.ErrPersistenceFailed)); output.status != counterOperationUnknown {
		t.Fatalf("remote persistence failure status = %v, want unknown", output.status)
	}
}
