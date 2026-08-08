// Command gorgen generates Go support code for gor Grain interfaces.
//
// It loads the package named by -pkg, reads interfaces marked with
// //gor:grain, and writes generated proxies, dispatch functions, and an
// Install function to generated.go. Run it with, for example:
//
//	go tool gorgen -pkg ./domain
//
// (Add the generator to the module once with
// `go get -tool github.com/suraciii/gor/cmd/gorgen`; inside the gor
// repository itself, `go run ./cmd/gorgen` works the same.)
//
// Without -out, gorgen writes to the Grain package's gorgen subpackage,
// <package>/gorgen. Use -out to select another output directory. Import the
// generated package, call its Install function after creating a gor.Runtime
// and before using the generated Grain References, then register and invoke
// those Grains through the root gor package.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/suraciii/gor/internal/codegen"
)

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("gorgen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	packagePattern := flags.String("pkg", "", "package containing gor:grain interfaces")
	output := flags.String("out", "", "output directory (default: <package>/gorgen)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *packagePattern == "" {
		return errors.New("-pkg is required")
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}

	loaded, err := codegen.Load(*packagePattern)
	if err != nil {
		return err
	}
	source, err := codegen.Render(loaded.Model)
	if err != nil {
		return err
	}
	outputDir := *output
	if outputDir == "" {
		outputDir = filepath.Join(loaded.Dir, "gorgen")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory %s: %w", outputDir, err)
	}
	outputPath := filepath.Join(outputDir, "generated.go")
	if err := os.WriteFile(outputPath, source, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", outputPath, err)
	}
	return nil
}
