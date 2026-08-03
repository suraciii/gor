//go:build sim

package sim

import (
	"bytes"
	"testing"
	"testing/synctest"
)

func TestSim_ReplaysEventLog(t *testing.T) {
	first := runSimulationInBubble(t, simulationSeed)
	second := runSimulationInBubble(t, simulationSeed)
	if !bytes.Equal([]byte(decisionLines(first)), []byte(decisionLines(second))) {
		t.Fatalf("same seed produced different decisions:\nfirst:\n%s\nsecond:\n%s", decisionLines(first), decisionLines(second))
	}
}

func TestSim_ChecksSeedBatch(t *testing.T) {
	for offset := uint64(0); offset < 64; offset++ {
		runSimulationInBubble(t, simulationSeed+offset)
	}
}

func runSimulationInBubble(t *testing.T, seed uint64) string {
	t.Helper()
	var (
		output string
		runErr error
	)
	synctest.Test(t, func(_ *testing.T) {
		output, runErr = runSimulation(seed)
	})
	if runErr != nil {
		t.Fatalf("simulation seed=%08x failed: %v\nlog:\n%s", seed, runErr, output)
	}
	return output
}
