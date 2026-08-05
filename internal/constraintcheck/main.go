package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Violation struct {
	Path    string
	Line    int
	Column  int
	Message string
}

func (v Violation) String() string {
	return fmt.Sprintf("%s:%d:%d: %s", v.Path, v.Line, v.Column, v.Message)
}

var forbiddenTimeFunctions = map[string]string{
	"After":     "time.After",
	"NewTicker": "time.NewTicker",
	"NewTimer":  "time.NewTimer",
	"Now":       "time.Now",
	"Since":     "time.Since",
	"Tick":      "time.Tick",
	"Until":     "time.Until",
}

var skippedTestFunctions = map[string]struct{}{
	"Skip":    {},
	"SkipNow": {},
	"Skipf":   {},
}

func Check(root string) ([]Violation, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}

	var paths []string
	err = filepath.WalkDir(absRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != absRoot && (entry.Name() == ".git" || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(entry.Name(), ".go") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}
	sort.Strings(paths)

	fileSet := token.NewFileSet()
	var violations []Violation
	for _, path := range paths {
		file, parseErr := parser.ParseFile(fileSet, path, nil, 0)
		if parseErr != nil {
			return nil, fmt.Errorf("parse %s: %w", path, parseErr)
		}
		relPath, relErr := filepath.Rel(absRoot, path)
		if relErr != nil {
			return nil, fmt.Errorf("make %s relative to %s: %w", path, absRoot, relErr)
		}
		violations = append(violations, inspectFile(fileSet, relPath, file)...)
	}

	sort.Slice(violations, func(i, j int) bool {
		left, right := violations[i], violations[j]
		if left.Path != right.Path {
			return left.Path < right.Path
		}
		if left.Line != right.Line {
			return left.Line < right.Line
		}
		if left.Column != right.Column {
			return left.Column < right.Column
		}
		return left.Message < right.Message
	})
	return violations, nil
}

func inspectFile(fileSet *token.FileSet, path string, file *ast.File) []Violation {
	timeAliases, dotTimeImport, singleflightImport := imports(file)
	violations := make([]Violation, 0)
	add := func(node ast.Node, message string) {
		position := fileSet.Position(node.Pos())
		violations = append(violations, Violation{
			Path:    filepath.ToSlash(path),
			Line:    position.Line,
			Column:  position.Column,
			Message: message,
		})
	}

	if singleflightImport {
		for _, importSpec := range file.Imports {
			importPath, err := strconv.Unquote(importSpec.Path.Value)
			if err == nil && importPath == "golang.org/x/sync/singleflight" {
				add(importSpec, "importing golang.org/x/sync/singleflight violates the channel-wait rule; use channel-owned state instead")
			}
		}
	}

	isTestFile := strings.HasSuffix(filepath.Base(path), "_test.go")
	if len(timeAliases) > 0 || dotTimeImport {
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			packageName, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if _, ok := timeAliases[packageName.Name]; !ok {
				return true
			}
			functionName := selector.Sel.Name
			if !isTestFile {
				if displayName, ok := forbiddenTimeFunctions[functionName]; ok && !allowedRealClockCall(path, file, selector, functionName) {
					add(selector, fmt.Sprintf("direct %s violates the injected Clock rule; use Clock.Now or Clock.NewTicker instead", displayName))
				}
			}
			if isTestFile && functionName == "Sleep" {
				add(selector, "test time.Sleep violates the deterministic-test rule; use a channel or testing/synctest instead")
			}
			return true
		})
		if dotTimeImport {
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				functionName, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				if !isTestFile {
					if displayName, ok := forbiddenTimeFunctions[functionName.Name]; ok && !allowedRealClockCall(path, file, functionName, functionName.Name) {
						add(functionName, fmt.Sprintf("direct %s violates the injected Clock rule; use Clock.Now or Clock.NewTicker instead", displayName))
					}
				}
				if isTestFile && functionName.Name == "Sleep" {
					add(functionName, "test time.Sleep violates the deterministic-test rule; use a channel or testing/synctest instead")
				}
				return true
			})
		}
	}

	if isTestFile {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok {
				if _, skipped := skippedTestFunctions[selector.Sel.Name]; skipped {
					add(selector, "t.Skip* violates the deterministic-test rule; make the test deterministic or fail explicitly")
				}
			}
			return true
		})
	}

	return violations
}

func imports(file *ast.File) (map[string]struct{}, bool, bool) {
	timeAliases := make(map[string]struct{})
	dotTimeImport := false
	singleflightImport := false
	for _, importSpec := range file.Imports {
		importPath, err := strconv.Unquote(importSpec.Path.Value)
		if err != nil {
			continue
		}
		switch importPath {
		case "time":
			switch {
			case importSpec.Name == nil:
				timeAliases["time"] = struct{}{}
			case importSpec.Name.Name == ".":
				dotTimeImport = true
			case importSpec.Name.Name != "_":
				timeAliases[importSpec.Name.Name] = struct{}{}
			}
		case "golang.org/x/sync/singleflight":
			singleflightImport = true
		}
	}
	return timeAliases, dotTimeImport, singleflightImport
}

type functionRange struct {
	start token.Pos
	end   token.Pos
	name  string
	recv  string
}

func functionRanges(file *ast.File) []functionRange {
	ranges := make([]functionRange, 0)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ranges = append(ranges, functionRange{
			start: function.Pos(),
			end:   function.End(),
			name:  function.Name.Name,
			recv:  receiverName(function),
		})
	}
	return ranges
}

func receiverName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) != 1 {
		return ""
	}
	switch receiver := function.Recv.List[0].Type.(type) {
	case *ast.Ident:
		return receiver.Name
	case *ast.StarExpr:
		if name, ok := receiver.X.(*ast.Ident); ok {
			return name.Name
		}
	}
	return ""
}

func allowedRealClockCall(path string, file *ast.File, node ast.Node, functionName string) bool {
	if filepath.ToSlash(path) != "clock/clock.go" || file.Name.Name != "clock" {
		return false
	}
	for _, function := range functionRanges(file) {
		if node.Pos() >= function.start && node.Pos() <= function.end && function.name == functionName && function.recv == "Real" {
			return functionName == "Now" || functionName == "NewTicker"
		}
	}
	return false
}

func main() {
	root := "."
	if len(os.Args) > 2 {
		fmt.Fprintln(os.Stderr, "usage: constraintcheck [root]")
		os.Exit(2)
	}
	if len(os.Args) == 2 {
		root = os.Args[1]
	}

	violations, err := Check(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "constraintcheck: %v\n", err)
		os.Exit(2)
	}
	for _, violation := range violations {
		fmt.Println(violation)
	}
	if len(violations) > 0 {
		os.Exit(1)
	}
}
