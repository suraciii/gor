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
}
`,
		"dot.go": `package example

import . "time"

func useDotImport() {
	Now()
}
`,
	})

	violations, err := Check(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 11 {
		t.Fatalf("got %d violations, want 11: %v", len(violations), violations)
	}
	for _, want := range []string{
		"direct time.Now violates the injected Clock rule",
		"direct time.Since violates the injected Clock rule",
		"direct time.Until violates the injected Clock rule",
		"direct time.After violates the injected Clock rule",
		"direct time.Tick violates the injected Clock rule",
		"direct time.NewTimer violates the injected Clock rule",
		"direct time.NewTicker violates the injected Clock rule",
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
