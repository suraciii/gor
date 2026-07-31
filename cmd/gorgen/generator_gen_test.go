//go:build gen

package main

import (
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
