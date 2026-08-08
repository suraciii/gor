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
			if err == nil || !strings.Contains(err.Error(), "aliases") {
				t.Fatalf("validateDatabasePaths(%q, %q) = %v, want clear alias-path error", test.runtime, test.business, err)
			}
		})
	}
}

func TestValidateDatabasePathsAcceptsSeparatePaths(t *testing.T) {
	if err := validateDatabasePaths("runtime.db", "business.db"); err != nil {
		t.Fatalf("validateDatabasePaths returned error for separate paths: %v", err)
	}
}

func TestValidateDatabasePathsRejectsDerivedRuntimeStatePath(t *testing.T) {
	directory := t.TempDir()
	runtimePath := filepath.Join(directory, "runtime.db")
	businessPath := derivedRuntimeStatePath(runtimePath)
	if err := validateDatabasePaths(runtimePath, businessPath); err == nil || !strings.Contains(err.Error(), "runtime State") {
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
	if err := validateDatabasePaths(runtimePath, businessPath); err == nil || !strings.Contains(err.Error(), "aliases") {
		t.Fatalf("validateDatabasePaths(%q, %q) = %v, want symlink alias error", runtimePath, businessPath, err)
	}

	danglingTarget := filepath.Join(directory, "not-created.db")
	danglingAlias := filepath.Join(directory, "dangling-business.db")
	if err := os.Symlink(danglingTarget, danglingAlias); err != nil {
		t.Fatalf("create dangling symlink alias: %v", err)
	}
	if err := validateDatabasePaths(danglingTarget, danglingAlias); err == nil || !strings.Contains(err.Error(), "aliases") {
		t.Fatalf("validateDatabasePaths(%q, %q) = %v, want dangling symlink alias error", danglingTarget, danglingAlias, err)
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
	if err := validateDatabasePaths(runtimePath, businessPath); err == nil || !strings.Contains(err.Error(), "existing") {
		t.Fatalf("validateDatabasePaths(%q, %q) = %v, want hard-link alias error", runtimePath, businessPath, err)
	}
}
