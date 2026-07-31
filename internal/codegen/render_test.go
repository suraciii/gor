package codegen

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRender_MatchesCompilingGolden(t *testing.T) {
	model := Model{
		PackageName:       "generated",
		SourcePackageName: "domain",
		SourceImportPath:  "github.com/suraciii/gor/internal/codegen/testfixture/domain",
		Interfaces: []Interface{{
			Name: "Account",
			Methods: []Method{
				{
					Name:    "Lookup",
					Params:  []Parameter{{Name: "ctx", Type: "context.Context"}, {Name: "key", Type: "string"}},
					Results: []string{"int64", "string", "error"},
				},
				{
					Name:    "Reset",
					Params:  []Parameter{{Name: "ctx", Type: "context.Context"}},
					Results: []string{"error"},
				},
			},
		}, {
			Name: "Ledger",
			Methods: []Method{
				{
					Name:    "Balance",
					Params:  []Parameter{{Name: "ctx", Type: "context.Context"}},
					Results: []string{"int64", "error"},
				},
			},
		}},
	}

	got, err := Render(model)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("testfixture", "generated", "generated.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("rendered source differs from golden:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}
