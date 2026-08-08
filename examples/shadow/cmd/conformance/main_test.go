package main

import (
	"os"
	"path/filepath"
	"strings"
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
