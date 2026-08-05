//go:build sim

package sim

import (
	"context"
	"fmt"
	"sort"

	"github.com/anishathalye/porcupine"
	"github.com/suraciii/gor"
	"github.com/suraciii/gor/store"
)

type counterOperationStatus uint8

const (
	counterOperationSucceeded counterOperationStatus = iota
	counterOperationFailed
	counterOperationUnknown
)

type counterOperationOutput struct {
	value  int64
	status counterOperationStatus
}

func counterOperationOutputFor(value int64, err error) counterOperationOutput {
	switch {
	case err == nil:
		return counterOperationOutput{value: value, status: counterOperationSucceeded}
	case simErrorIs(err, errAppliedWriteFailure), simErrorIs(err, gor.ErrPersistenceFailed), simErrorIs(err, context.Canceled):
		return counterOperationOutput{value: value, status: counterOperationUnknown}
	case simErrorIs(err, errWriteFailure):
		return counterOperationOutput{value: value, status: counterOperationFailed}
	default:
		return counterOperationOutput{value: value, status: counterOperationFailed}
	}
}

var counterModel = (&porcupine.NondeterministicModel{
	Init: func() []interface{} {
		return []interface{}{int64(0)}
	},
	Step: func(state interface{}, input interface{}, output interface{}) []interface{} {
		value := state.(int64)
		delta := input.(int64)
		result := output.(counterOperationOutput)
		switch result.status {
		case counterOperationUnknown:
			return []interface{}{value, value + delta}
		case counterOperationFailed:
			return []interface{}{value}
		case counterOperationSucceeded:
			if result.value != value+delta {
				return nil
			}
			return []interface{}{result.value}
		default:
			return nil
		}
	},
	Equal: func(first interface{}, second interface{}) bool {
		return first.(int64) == second.(int64)
	},
	Hash: func(state interface{}) uint64 {
		return uint64(state.(int64))
	},
}).ToModel()

type counterHistory struct {
	operations map[store.Identity][]porcupine.Operation
}

func newCounterHistory() *counterHistory {
	return &counterHistory{
		operations: make(map[store.Identity][]porcupine.Operation),
	}
}

func (h *counterHistory) add(id store.Identity, operation porcupine.Operation) {
	h.operations[id] = append(h.operations[id], operation)
}

func (h *counterHistory) check() error {
	ids := make([]store.Identity, 0, len(h.operations))
	for id := range h.operations {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].Type != ids[j].Type {
			return ids[i].Type < ids[j].Type
		}
		return ids[i].Key < ids[j].Key
	})
	for _, id := range ids {
		if !porcupine.CheckOperations(counterModel, h.operations[id]) {
			return fmt.Errorf("counter history is not linearizable for %s/%s", id.Type, id.Key)
		}
	}
	return nil
}
