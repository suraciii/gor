//go:build gen

package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestGenerateFixtureBuilds(t *testing.T) {
	root := moduleRoot(t)
	fixtureRoot := filepath.Join(root, "cmd", "gorgen", "testfixture")
	outputDir, err := os.MkdirTemp(fixtureRoot, "generated-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(outputDir) })

	runGorgen(t, root, "./cmd/gorgen/testfixture/domain", outputDir)
	generatedPath := filepath.Join(outputDir, "generated.go")
	if _, err := os.Stat(generatedPath); err != nil {
		t.Fatalf("generated file: %v", err)
	}

	buildPath, err := filepath.Rel(root, outputDir)
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "./"+filepath.ToSlash(buildPath))
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build generated package: %v\n%s", err, output)
	}
}

func TestGeneratedFixtureMatchesCommittedOutput(t *testing.T) {
	root := moduleRoot(t)
	fixtureRoot := filepath.Join(root, "cmd", "gorgen", "testfixture", "endtoend")
	outputDir, err := os.MkdirTemp(fixtureRoot, "generated-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(outputDir) })

	runGorgen(t, root, "./cmd/gorgen/testfixture/endtoend/domain", outputDir)
	got, err := os.ReadFile(filepath.Join(outputDir, "generated.go"))
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join(fixtureRoot, "gorgen", "generated.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("generated output differs from committed fixture:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestGenerateReportsContractLine(t *testing.T) {
	root := moduleRoot(t)
	outputDir, err := os.MkdirTemp(filepath.Join(root, "cmd", "gorgen", "testfixture"), "invalid-generated-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(outputDir) })

	command := exec.Command("go", "run", "./cmd/gorgen", "-pkg", "./cmd/gorgen/testfixture/invalid", "-out", outputDir)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("gorgen accepted an invalid entity method")
	}
	message := string(output)
	if !strings.Contains(message, "domain.go:5:") {
		t.Fatalf("error = %s, want fixture line number", message)
	}
	if !strings.Contains(message, "context.Context as its first parameter") {
		t.Fatalf("error = %s, want first-parameter contract", message)
	}
}

func TestGenerateRejectsPackageWithoutEntity(t *testing.T) {
	root := moduleRoot(t)
	outputDir, err := os.MkdirTemp(filepath.Join(root, "cmd", "gorgen", "testfixture"), "empty-generated-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(outputDir) })

	command := exec.Command("go", "run", "./cmd/gorgen", "-pkg", "./cmd/gorgen/testfixture/empty", "-out", outputDir)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("gorgen accepted a package without an entity interface")
	}
	if !strings.Contains(string(output), "contains no gor:entity interfaces") {
		t.Fatalf("error = %s, want missing-entity error", output)
	}
}

func TestGeneratedFixtureRejectsWrongParameterType(t *testing.T) {
	root := moduleRoot(t)
	fixtureRoot := filepath.Join(root, "cmd", "gorgen", "testfixture", "endtoend")
	outputDir, err := os.MkdirTemp(fixtureRoot, "typecheck-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(outputDir) })

	wrongSource := `package gorgen

import (
	"context"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/cmd/gorgen/testfixture/endtoend/domain"
)

func wrongParameterType(rt *gor.Runtime) {
	_, _ = gor.Ref[domain.Account](rt, "alice").Deposit(context.Background(), "wrong")
}
`
	if err := os.WriteFile(filepath.Join(outputDir, "wrong.go"), []byte(wrongSource), 0o644); err != nil {
		t.Fatal(err)
	}
	buildPath, err := filepath.Rel(root, outputDir)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "build", "./"+filepath.ToSlash(buildPath))
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("go build accepted a string argument for Deposit")
	}
	message := string(output)
	if !strings.Contains(message, "cannot use") || !strings.Contains(message, "int64") {
		t.Fatalf("go build error = %s, want a string-to-int64 type mismatch", message)
	}
}

func runGorgen(t *testing.T, root, packagePattern, outputDir string) {
	t.Helper()
	command := exec.Command("go", "run", "./cmd/gorgen", "-pkg", packagePattern, "-out", outputDir)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("go run gorgen: %v\n%s", err, output)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
