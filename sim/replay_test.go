//go:build sim

package sim

import (
	"bytes"
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
	for offset := uint64(0); offset < 64; offset++ {
		output := runSimulationInBubble(t, simulationSeed+offset, clusterNodeCount)
		for _, stat := range []string{
			"list-errors",
			"list-delays",
			"claim-errors",
			"claim-applied-errors",
		} {
			if scheduleStatPositive(output, stat) {
				seenStats[stat] = true
			}
		}
		for _, stat := range []string{
			"list-errors",
			"cas-errors",
			"cas-applied-errors",
			"delays",
			"dead-writes",
			"delayed-dead-cas",
		} {
			if memberStatPositive(output, stat) {
				seenStats["member-"+stat] = true
			}
		}
	}
	delayedStats := runDelayedMemberWriteScenario(t)
	if delayedStats.delayedDeadCAS > 0 {
		seenStats["member-delayed-dead-cas"] = true
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
		"delayed-dead-cas",
	} {
		if !seenStats["member-"+stat] {
			t.Fatalf("seed batch never triggered member stat %s", stat)
		}
	}
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
