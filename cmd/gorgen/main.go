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
	packagePattern := flags.String("pkg", "", "package containing gor:entity interfaces")
	output := flags.String("out", "", "output directory (default: package/internal/gorgen)")
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
		outputDir = filepath.Join(loaded.Dir, "internal", "gorgen")
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
