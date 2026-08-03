//go:build sim

package sim

import (
	"bytes"
	"strings"
	"testing"
	"testing/synctest"
)

func TestSim_ReplaysEventLog(t *testing.T) {
	first := runSimulationInBubble(t, simulationSeed, clusterNodeCount)
	second := runSimulationInBubble(t, simulationSeed, clusterNodeCount)
	firstDecisions := decisionLines(first)
	secondDecisions := decisionLines(second)
	if !bytes.Equal([]byte(firstDecisions), []byte(secondDecisions)) {
		t.Fatalf("same seed produced different decisions:\nfirst:\n%s\nsecond:\n%s", decisionLines(first), decisionLines(second))
	}
	if !strings.Contains(firstDecisions, "decision crash node=") {
		t.Fatal("simulation never crashed a node")
	}
	if !strings.Contains(firstDecisions, "decision restart node=") {
		t.Fatal("simulation never restarted a node")
	}
	if !strings.Contains(firstDecisions, "nodes=[0,1]") && !strings.Contains(firstDecisions, "nodes=[1,0]") {
		t.Fatal("simulation never sent calls to both nodes")
	}
}

func TestSim_ChecksSeedBatch(t *testing.T) {
	for offset := uint64(0); offset < 64; offset++ {
		runSimulationInBubble(t, simulationSeed+offset, clusterNodeCount)
	}
}

func runSimulationInBubble(t *testing.T, seed uint64, nodeCount int) string {
	t.Helper()
	var (
		output string
		runErr error
	)
	synctest.Test(t, func(_ *testing.T) {
		output, runErr = runSimulation(seed, nodeCount)
	})
	if runErr != nil {
		t.Fatalf("simulation seed=%08x failed: %v\nlog:\n%s", seed, runErr, output)
	}
	return output
}
