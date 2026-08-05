package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckReportsForbiddenProductionCallsAndImports(t *testing.T) {
	root := writeSources(t, map[string]string{
		"alias.go": `package example

import (
	wall "time"
	_ "golang.org/x/sync/singleflight"
)

func useWallClock() {
	wall.Now()
	wall.Since(wall.Now())
	wall.Until(wall.Now())
	wall.After(0)
	wall.Tick(0)
	wall.NewTimer(0)
	wall.NewTicker(0)
	wall.Sleep(0)
	f := wall.Sleep
	f()
}
`,
		"dot.go": `package example

import . "time"

func useDotImport() {
	Now()
	f := Now
	f()
	Sleep(0)
	g := Sleep
	g()
}
`,
	})

	violations, err := Check(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 16 {
		t.Fatalf("got %d violations, want 16: %v", len(violations), violations)
	}
	for _, want := range []string{
		"direct time.Now violates the injected Clock rule",
		"direct time.Since violates the injected Clock rule",
		"direct time.Until violates the injected Clock rule",
		"direct time.After violates the injected Clock rule",
		"direct time.Tick violates the injected Clock rule",
		"direct time.NewTimer violates the injected Clock rule",
		"direct time.NewTicker violates the injected Clock rule",
		"direct time.Sleep violates the injected Clock rule",
		"importing golang.org/x/sync/singleflight violates the channel-wait rule",
	} {
		if !hasViolation(violations, want) {
			t.Errorf("missing violation %q in %v", want, violations)
		}
	}
	for _, violation := range violations {
		if violation.Line == 0 || violation.Column == 0 {
			t.Errorf("violation has no source position: %v", violation)
		}
	}
}

func TestCheckReportsTestSleepAndSkip(t *testing.T) {
	root := writeSources(t, map[string]string{
		"example_test.go": `package example

import "time"

func TestExample(t *testing.T) {
	time.Sleep(time.Second)
	t.Skip("not today")
	t.Skipf("not today")
	t.SkipNow()
}
`,
	})

	violations, err := Check(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 4 {
		t.Fatalf("got %d violations, want 4: %v", len(violations), violations)
	}
	for _, want := range []string{
		"test time.Sleep violates the deterministic-test rule",
		"t.Skip* violates the deterministic-test rule",
	} {
		if !hasViolation(violations, want) {
			t.Errorf("missing violation %q in %v", want, violations)
		}
	}
}

func TestCheckAllowsRealClockImplementationOnly(t *testing.T) {
	root := writeSources(t, map[string]string{
		"clock/clock.go": `package clock

import "time"

type Real struct{}

func (Real) Now() time.Time {
	return time.Now()
}

func (Real) NewTicker(d time.Duration) *time.Ticker {
	return time.NewTicker(d)
}
`,
		"clock/other.go": `package clock

import "time"

func wallClock() time.Time {
	return time.Now()
}
`,
		"runtime.go": `package example

type Clock interface {
	Now()
	NewTicker()
}

func useInjectedClock(clock Clock) {
	clock.Now()
	clock.NewTicker()
}
`,
	})

	violations, err := Check(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 1 {
		t.Fatalf("got %d violations, want 1: %v", len(violations), violations)
	}
	if !hasViolationAt(violations, "clock/other.go", "direct time.Now violates the injected Clock rule") {
		t.Fatalf("got %v", violations)
	}
}

func TestCheckAllowsInjectedClockUsage(t *testing.T) {
	root := writeSources(t, map[string]string{
		"runtime.go": `package example

type Clock interface {
	Now()
	NewTicker()
}

func useInjectedClock(clock Clock) {
	clock.Now()
	clock.NewTicker()
}
`,
	})

	violations, err := Check(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("got %d violations, want none: %v", len(violations), violations)
	}
}

func TestCheckClassifiesSimBuildConstraints(t *testing.T) {
	root := writeSources(t, map[string]string{
		"sim/store.go": `//go:build sim

package sim

import "time"

func injectedDelay() {
	time.Sleep(0)
}
`,
		"sim/store_test.go": `//go:build sim

package sim

import "time"

func TestInjectedDelay(t *testing.T) {
	time.Sleep(0)
}
`,
		"sim/clock.go": `//go:build sim

package sim

import "time"

func wallClock() time.Time {
	return time.Now()
}
`,
		"sim/legacy.go": `// +build sim

package sim

import "time"

func legacyInjectedDelay() {
	time.Sleep(0)
}
`,
	})

	violations, err := Check(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 2 {
		t.Fatalf("got %d violations, want 2: %v", len(violations), violations)
	}
	if !hasViolationAt(violations, "sim/store_test.go", "test time.Sleep violates the deterministic-test rule") {
		t.Fatalf("missing sim test Sleep violation in %v", violations)
	}
	if !hasViolationAt(violations, "sim/clock.go", "direct time.Now violates the injected Clock rule") {
		t.Fatalf("missing sim time.Now violation in %v", violations)
	}
}

func writeSources(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for path, source := range files {
		fullPath := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte(source), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func hasViolation(violations []Violation, want string) bool {
	for _, violation := range violations {
		if strings.Contains(violation.Message, want) {
			return true
		}
	}
	return false
}

func hasViolationAt(violations []Violation, path, message string) bool {
	for _, violation := range violations {
		if violation.Path == path && strings.Contains(violation.Message, message) {
			return true
		}
	}
	return false
}
