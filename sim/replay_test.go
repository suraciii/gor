//go:build sim

package sim

import (
	"bytes"
	"fmt"
	"strconv"
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
	hasNodeZero := strings.Contains(firstDecisions, "nodes=[0,") || strings.Contains(firstDecisions, "nodes=[1,0]")
	hasNodeOne := strings.Contains(firstDecisions, "nodes=[1,") || strings.Contains(firstDecisions, "nodes=[0,1]")
	if !hasNodeZero || !hasNodeOne {
		t.Fatal("simulation never sent calls to both nodes")
	}
}

func TestSim_ChecksSeedBatch(t *testing.T) {
	seenStats := make(map[string]bool)
	var replayFailures []string
	for offset := uint64(0); offset < 64; offset++ {
		seed := simulationSeed + offset
		first := runSimulationInBubble(t, seed, clusterNodeCount)
		// Every seed the batch runs is run twice and its decision lines
		// compared. A one-run batch walks many trajectories but checks none
		// for replay fidelity, so a leak that breaks only some seeds would
		// pass green forever; the gate must match the contract.
		second := runSimulationInBubble(t, seed, clusterNodeCount)
		firstDecisions := decisionLines(first)
		secondDecisions := decisionLines(second)
		if firstDecisions != secondDecisions {
			replayFailures = append(replayFailures, replayFailure(seed, first, firstDecisions, second, secondDecisions))
		}
		for _, stat := range []string{
			"list-errors",
			"list-delays",
			"claim-errors",
			"claim-applied-errors",
		} {
			if scheduleStatPositive(first, stat) {
				seenStats[stat] = true
			}
		}
		for _, stat := range []string{
			"list-errors",
			"cas-errors",
			"cas-applied-errors",
			"delays",
			"dead-writes",
		} {
			if memberStatPositive(first, stat) {
				seenStats["member-"+stat] = true
			}
		}
		for _, stat := range []string{"held", "completed"} {
			if networkStatPositive(first, stat) {
				seenStats["network-"+stat] = true
			}
		}
		for _, stat := range []string{"drop-requests", "drop-replies"} {
			if networkStatPositive(first, stat) {
				seenStats["network-"+stat] = true
			}
		}
	}
	if len(replayFailures) > 0 {
		t.Fatalf("seed batch did not replay byte-identically for %d seeds:\n%s", len(replayFailures), strings.Join(replayFailures, "\n"))
	}
	for _, stat := range []string{
		"list-errors",
		"list-delays",
		"claim-errors",
		"claim-applied-errors",
	} {
		if !seenStats[stat] {
			t.Fatalf("seed batch never triggered schedule stat %s", stat)
		}
	}
	for _, stat := range []string{
		"list-errors",
		"cas-errors",
		"cas-applied-errors",
		"delays",
		"dead-writes",
	} {
		if !seenStats["member-"+stat] {
			t.Fatalf("seed batch never triggered member stat %s", stat)
		}
	}
	for _, stat := range []string{"held", "completed"} {
		if !seenStats["network-"+stat] {
			t.Fatalf("seed batch never triggered network stat %s", stat)
		}
	}
	for _, stat := range []string{"drop-requests", "drop-replies"} {
		if !seenStats["network-"+stat] {
			t.Fatalf("seed batch never triggered network stat %s", stat)
		}
	}
}

// replayFailure renders a diverging seed for the failure report: the whole
// log of both runs — the observation half exists for exactly this moment —
// and the first differing decision line marked.
func replayFailure(seed uint64, first, firstDecisions, second, secondDecisions string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed %08x: decision lines differ between two runs of the same seed:\n", seed)
	firstLines := strings.Split(strings.TrimSuffix(firstDecisions, "\n"), "\n")
	secondLines := strings.Split(strings.TrimSuffix(secondDecisions, "\n"), "\n")
	diverged := false
	for index := 0; index < len(firstLines) || index < len(secondLines); index++ {
		a, bLine := "<end>", "<end>"
		if index < len(firstLines) {
			a = firstLines[index]
		}
		if index < len(secondLines) {
			bLine = secondLines[index]
		}
		if a != bLine && !diverged {
			fmt.Fprintf(&b, "first divergence at line %d:\n", index)
			diverged = true
		}
	}
	b.WriteString("first run (full log):\n")
	b.WriteString(first)
	b.WriteString("second run (full log):\n")
	b.WriteString(second)
	return b.String()
}

func networkStatPositive(log, name string) bool {
	for _, line := range strings.Split(log, "\n") {
		if !strings.Contains(line, "observe network ") {
			continue
		}
		for _, field := range strings.Fields(line) {
			prefix := name + "="
			if !strings.HasPrefix(field, prefix) {
				continue
			}
			value, err := strconv.Atoi(strings.TrimPrefix(field, prefix))
			if err == nil && value > 0 {
				return true
			}
		}
	}
	return false
}

func scheduleStatPositive(log, name string) bool {
	for _, line := range strings.Split(log, "\n") {
		if !strings.Contains(line, "observe schedules ") {
			continue
		}
		for _, field := range strings.Fields(line) {
			prefix := name + "="
			if !strings.HasPrefix(field, prefix) {
				continue
			}
			value, err := strconv.Atoi(strings.TrimPrefix(field, prefix))
			if err == nil && value > 0 {
				return true
			}
		}
	}
	return false
}

func memberStatPositive(log, name string) bool {
	for _, line := range strings.Split(log, "\n") {
		if !strings.Contains(line, "observe members ") {
			continue
		}
		for _, field := range strings.Fields(line) {
			prefix := name + "="
			if !strings.HasPrefix(field, prefix) {
				continue
			}
			value, err := strconv.Atoi(strings.TrimPrefix(field, prefix))
			if err == nil && value > 0 {
				return true
			}
		}
	}
	return false
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
