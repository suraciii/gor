//go:build sim

package sim

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

func TestCounterHistoryTimestampsOrderNonlinearOperations(t *testing.T) {
	first := porcupine.Operation{
		Input:  int64(1),
		Output: counterOperationOutput{value: 2, status: counterOperationSucceeded},
	}
	second := porcupine.Operation{
		Input:  int64(1),
		Output: counterOperationOutput{value: 1, status: counterOperationSucceeded},
	}

	oldTimestamps := []porcupine.Operation{
		first,
		second,
	}
	if !porcupine.CheckOperations(counterModel, oldTimestamps) {
		t.Fatal("porcupine rejected the non-linear history when all timestamps were equal")
	}

	strictTimestamps := []porcupine.Operation{
		{Input: first.Input, Call: 1, Return: 2, Output: first.Output},
		{Input: second.Input, Call: 3, Return: 4, Output: second.Output},
	}
	if porcupine.CheckOperations(counterModel, strictTimestamps) {
		t.Fatal("porcupine accepted the non-linear history when timestamps ordered operations")
	}
}
