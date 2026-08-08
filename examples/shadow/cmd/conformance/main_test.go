package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestValidateDatabasePathsRejectsEquivalentPaths(t *testing.T) {
	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	absolute := filepath.Join(workingDir, "runtime.db")
	for _, test := range []struct {
		name     string
		runtime  string
		business string
	}{
		{name: "same absolute", runtime: absolute, business: absolute},
		{name: "relative and absolute", runtime: "runtime.db", business: absolute},
		{name: "cleaned relative and absolute", runtime: filepath.Join("data", "..", "runtime.db"), business: absolute},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateDatabasePaths(test.runtime, test.business)
			if err == nil || !strings.Contains(err.Error(), "must be different") {
				t.Fatalf("validateDatabasePaths(%q, %q) = %v, want clear different-path error", test.runtime, test.business, err)
			}
		})
	}
}

func TestValidateDatabasePathsAcceptsSeparatePaths(t *testing.T) {
	if err := validateDatabasePaths("runtime.db", "business.db"); err != nil {
		t.Fatalf("validateDatabasePaths returned error for separate paths: %v", err)
	}
}

func TestValidateDatabasePathsRejectsSQLiteSidecarAliases(t *testing.T) {
	for _, sidecar := range []string{"-wal", "-shm"} {
		t.Run(sidecar, func(t *testing.T) {
			err := validateDatabasePaths("runtime.db", "runtime.db"+sidecar)
			if err == nil || !strings.Contains(err.Error(), "must be different") {
				t.Fatalf("validateDatabasePaths for %s = %v, want sidecar collision error", sidecar, err)
			}
		})
	}
	if err := validateDatabasePaths("runtime.db", "runtime-state.db-wal"); err == nil || !strings.Contains(err.Error(), "must be different") {
		t.Fatalf("validateDatabasePaths for Runtime State sidecar = %v, want sidecar collision error", err)
	}
}

func TestValidateDatabasePathsRejectsDerivedRuntimeStatePath(t *testing.T) {
	directory := t.TempDir()
	runtimePath := filepath.Join(directory, "runtime.db")
	businessPath := derivedRuntimeStatePath(runtimePath)
	if err := validateDatabasePaths(runtimePath, businessPath); err == nil || !strings.Contains(err.Error(), "runtime state") {
		t.Fatalf("validateDatabasePaths(%q, %q) = %v, want derived State alias error", runtimePath, businessPath, err)
	}
}

func TestValidateDatabasePathsRejectsSymlinkAliases(t *testing.T) {
	directory := t.TempDir()
	runtimePath := filepath.Join(directory, "runtime.db")
	businessPath := filepath.Join(directory, "business.db")
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(runtimePath, businessPath); err != nil {
		t.Fatalf("create symlink alias: %v", err)
	}
	if err := validateDatabasePaths(runtimePath, businessPath); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("validateDatabasePaths(%q, %q) = %v, want symlink error", runtimePath, businessPath, err)
	}

	danglingTarget := filepath.Join(directory, "not-created.db")
	danglingAlias := filepath.Join(directory, "dangling-business.db")
	if err := os.Symlink(danglingTarget, danglingAlias); err != nil {
		t.Fatalf("create dangling symlink alias: %v", err)
	}
	if err := validateDatabasePaths(danglingTarget, danglingAlias); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("validateDatabasePaths(%q, %q) = %v, want dangling symlink error", danglingTarget, danglingAlias, err)
	}
}

func TestValidateDatabasePathsRejectsHardLinkAliases(t *testing.T) {
	directory := t.TempDir()
	runtimePath := filepath.Join(directory, "runtime.db")
	businessPath := filepath.Join(directory, "business.db")
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(runtimePath, businessPath); err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.EXDEV) || errors.Is(err, syscall.EPERM) {
			t.Fatalf("hard links are required for this deterministic test: %v", err)
		}
		t.Fatal(err)
	}
	if err := validateDatabasePaths(runtimePath, businessPath); err == nil || !strings.Contains(err.Error(), "must not alias") {
		t.Fatalf("validateDatabasePaths(%q, %q) = %v, want hard-link alias error", runtimePath, businessPath, err)
	}
}

func TestValidateDatabasePathsRejectsSQLiteSidecarHardLinkAlias(t *testing.T) {
	directory := t.TempDir()
	runtimePath := filepath.Join(directory, "runtime.db")
	sidecarPath := runtimePath + "-wal"
	businessPath := filepath.Join(directory, "business.db")
	if err := os.WriteFile(sidecarPath, []byte("runtime wal"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(sidecarPath, businessPath); err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.EXDEV) || errors.Is(err, syscall.EPERM) {
			t.Fatalf("hard links are required for this deterministic test: %v", err)
		}
		t.Fatal(err)
	}
	if err := validateDatabasePaths(runtimePath, businessPath); err == nil || !strings.Contains(err.Error(), "must not alias") {
		t.Fatalf("validateDatabasePaths(%q, %q) = %v, want sidecar hard-link error", runtimePath, businessPath, err)
	}
}

func TestValidateDatabasePathsRejectsRuntimeStateHardLinkAlias(t *testing.T) {
	directory := t.TempDir()
	runtimePath := filepath.Join(directory, "runtime.db")
	statePath := derivedRuntimeStatePath(runtimePath)
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(runtimePath, statePath); err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.EXDEV) || errors.Is(err, syscall.EPERM) {
			t.Fatalf("hard links are required for this deterministic test: %v", err)
		}
		t.Fatal(err)
	}
	if err := validateDatabasePaths(runtimePath, filepath.Join(directory, "business.db")); err == nil || !strings.Contains(err.Error(), "must not alias") {
		t.Fatalf("validateDatabasePaths(%q, %q) = %v, want runtime/state hard-link error", runtimePath, statePath, err)
	}
}

func TestValidateDatabasePathsRejectsParentTraversalAndSymlinkAliases(t *testing.T) {
	directory := t.TempDir()
	runtimePath := filepath.Join(directory, "runtime.db")
	businessPath := filepath.Join(directory, "business.db")
	if err := os.Symlink(filepath.Join(directory, "target.db"), filepath.Join(directory, "link-one.db")); err != nil {
		t.Fatalf("create first symlink: %v", err)
	}
	if err := os.Symlink(filepath.Join(directory, "link-one.db"), filepath.Join(directory, "link-two.db")); err != nil {
		t.Fatalf("create second symlink: %v", err)
	}
	if err := validateDatabasePaths(filepath.Join(directory, "link-two.db"), businessPath); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("multi-hop symlink validation = %v, want symlink error", err)
	}

	traversalPath := filepath.Join(directory, "link-one.db") + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "runtime.db"
	if err := validateDatabasePaths(traversalPath, businessPath); err == nil || !strings.Contains(err.Error(), "must not contain '..'") {
		t.Fatalf("symlink-aware parent traversal validation = %v, want parent traversal error", err)
	}

	traversalRuntime := filepath.Join(directory, "runtime-alias.db")
	if err := os.Symlink(runtimePath, traversalRuntime); err != nil {
		t.Fatalf("create runtime symlink: %v", err)
	}
	if err := validateDatabasePaths(traversalRuntime, businessPath); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("runtime self-alias validation = %v, want symlink error", err)
	}
}
