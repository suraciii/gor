package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrCallCycle reports that a call targeted an entity already occupied by the
// same call chain, so the call could never start. The wrapped message names
// the entities in the cycle.
var ErrCallCycle = errors.New("call cycle detected")

type occupiedKey struct{}

// OccupiedFrom returns the entities the current call chain already occupies,
// outermost first. The chain travels on the context because Go has no
// AsyncLocal; a call whose target is on the chain would close a call cycle.
func OccupiedFrom(ctx context.Context) []GrainId {
	chain, _ := ctx.Value(occupiedKey{}).([]GrainId)
	return chain
}

// WithOccupied returns a context that carries chain as its occupied entities.
func WithOccupied(ctx context.Context, chain []GrainId) context.Context {
	return context.WithValue(ctx, occupiedKey{}, chain)
}

// withOccupied returns a context whose occupied chain is extended with id.
func withOccupied(ctx context.Context, id GrainId) context.Context {
	chain := OccupiedFrom(ctx)
	next := make([]GrainId, 0, len(chain)+1)
	next = append(next, chain...)
	next = append(next, id)
	return WithOccupied(ctx, next)
}

// checkCycle rejects a call whose target already occupies the context's
// chain. The error names the cycle: the occupied entities from the first
// occurrence of the target to the end of the chain, then the target again.
func checkCycle(ctx context.Context, id GrainId) error {
	chain := OccupiedFrom(ctx)
	start := -1
	for index, occupied := range chain {
		if occupied == id {
			start = index
			break
		}
	}
	if start < 0 {
		return nil
	}
	var message strings.Builder
	for index := start; index < len(chain); index++ {
		if index > start {
			message.WriteString(" -> ")
		}
		fmt.Fprintf(&message, "%s/%s", chain[index].GrainType, chain[index].GrainKey)
	}
	fmt.Fprintf(&message, " -> %s/%s", id.GrainType, id.GrainKey)
	return fmt.Errorf("%w: %s", ErrCallCycle, message.String())
}
