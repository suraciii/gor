package codegen

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/tools/go/packages"
)

type Loaded struct {
	Model Model
	Dir   string
}

func Load(pattern string) (Loaded, error) {
	packagesLoaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
	}, pattern)
	if err != nil {
		return Loaded{}, err
	}
	if len(packagesLoaded) != 1 {
		return Loaded{}, fmt.Errorf("pattern %q loaded %d packages, want one", pattern, len(packagesLoaded))
	}
	pkg := packagesLoaded[0]
	if len(pkg.Errors) > 0 {
		return Loaded{}, fmt.Errorf("load %s: %s", pkg.PkgPath, pkg.Errors[0])
	}
	if pkg.Types == nil || len(pkg.GoFiles) == 0 {
		return Loaded{}, fmt.Errorf("load %s: package has no type information", pkg.PkgPath)
	}

	model := Model{
		PackageName:       "gorgen",
		SourcePackageName: pkg.Name,
		SourceImportPath:  pkg.PkgPath,
	}
	imports := make(map[string]Import)
	var pending []pendingInterface
	for _, file := range pkg.Syntax {
		for _, declaration := range file.Decls {
			genDecl, ok := declaration.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.TYPE {
				continue
			}
			for _, specification := range genDecl.Specs {
				typeSpec := specification.(*ast.TypeSpec)
				if !hasGrainMarker(genDecl, typeSpec) {
					continue
				}
				if _, ok := typeSpec.Type.(*ast.InterfaceType); !ok {
					return Loaded{}, locatedError(pkg.Fset, typeSpec.Pos(), "%s.%s is marked gor:grain but is not an interface", pkg.Name, typeSpec.Name.Name)
				}
				entity, err := loadInterface(pkg, typeSpec, imports)
				if err != nil {
					return Loaded{}, err
				}
				pending = append(pending, entity)
			}
		}
	}
	if len(pending) == 0 {
		return Loaded{}, fmt.Errorf("package %s contains no gor:grain interfaces", pkg.PkgPath)
	}
	// Import names cannot be chosen while loading: whether two packages
	// collide is only known once every signature has been collected.
	aliases := effectiveImportNames(imports, pkg.Name, pkg.PkgPath)
	if alias, ok := aliases[pkg.PkgPath]; ok {
		model.SourceImportName = alias
	}
	model.Interfaces = make([]Interface, len(pending))
	for i, entity := range pending {
		model.Interfaces[i] = materialize(entity, aliases)
	}
	for path, imported := range imports {
		if alias, ok := aliases[path]; ok {
			imported.Name = alias
		}
		model.Imports = append(model.Imports, imported)
	}
	sort.Slice(model.Imports, func(i, j int) bool { return model.Imports[i].Path < model.Imports[j].Path })
	files := pkg.CompiledGoFiles
	if len(files) == 0 {
		files = pkg.GoFiles
	}
	return Loaded{Model: model, Dir: filepath.Dir(files[0])}, nil
}

// pendingInterface carries the raw go/types data of a Grain interface;
// type strings are only rendered once every import has been collected and
// aliases have been decided.
type pendingInterface struct {
	name    string
	methods []pendingMethod
}

type pendingMethod struct {
	name    string
	params  []pendingParameter
	results []types.Type
}

type pendingParameter struct {
	name string
	typ  types.Type
}

func loadInterface(pkg *packages.Package, specification *ast.TypeSpec, imports map[string]Import) (pendingInterface, error) {
	object := pkg.Types.Scope().Lookup(specification.Name.Name)
	if object == nil {
		return pendingInterface{}, locatedError(pkg.Fset, specification.Pos(), "type %s has no type information", specification.Name.Name)
	}
	typeName, ok := object.(*types.TypeName)
	if !ok {
		return pendingInterface{}, locatedError(pkg.Fset, specification.Pos(), "type %s has no type information", specification.Name.Name)
	}
	entity, ok := typeName.Type().Underlying().(*types.Interface)
	if !ok {
		return pendingInterface{}, locatedError(pkg.Fset, specification.Pos(), "type %s is not an interface", specification.Name.Name)
	}
	model := pendingInterface{name: specification.Name.Name, methods: make([]pendingMethod, entity.NumMethods())}
	for i := 0; i < entity.NumMethods(); i++ {
		method := entity.Method(i)
		signature, ok := method.Type().(*types.Signature)
		if !ok {
			return pendingInterface{}, locatedError(pkg.Fset, method.Pos(), "%s.%s has no method signature", model.name, method.Name())
		}
		if signature.Params().Len() == 0 || !isContext(signature.Params().At(0).Type()) {
			return pendingInterface{}, locatedError(pkg.Fset, method.Pos(), "%s.%s must have context.Context as its first parameter", model.name, method.Name())
		}
		if signature.Results().Len() == 0 || !isError(signature.Results().At(signature.Results().Len()-1).Type()) {
			return pendingInterface{}, locatedError(pkg.Fset, method.Pos(), "%s.%s must have error as its last result", model.name, method.Name())
		}
		loaded := pendingMethod{name: method.Name()}
		for parameter := 0; parameter < signature.Params().Len(); parameter++ {
			variable := signature.Params().At(parameter)
			name := variable.Name()
			if name == "" {
				name = fmt.Sprintf("arg%d", parameter)
			}
			recordImports(variable.Type(), imports)
			loaded.params = append(loaded.params, pendingParameter{name: name, typ: variable.Type()})
		}
		for result := 0; result < signature.Results().Len(); result++ {
			recordImports(signature.Results().At(result).Type(), imports)
			loaded.results = append(loaded.results, signature.Results().At(result).Type())
		}
		model.methods[i] = loaded
	}
	return model, nil
}

// recordImports adds every package referenced by value to imports, keyed by
// path. The rendered string is discarded; only the referenced set matters.
func recordImports(value types.Type, imports map[string]Import) {
	record := func(imported *types.Package) string {
		if imported == nil {
			return ""
		}
		imports[imported.Path()] = Import{Name: imported.Name(), Path: imported.Path()}
		return imported.Name()
	}
	_ = types.TypeString(value, record)
}

// materialize renders pending types into model type strings, qualifying each
// package by the alias effectiveImportNames assigned to it, if any.
func materialize(entity pendingInterface, aliases map[string]string) Interface {
	qualifier := func(imported *types.Package) string {
		if imported == nil {
			return ""
		}
		if alias, ok := aliases[imported.Path()]; ok {
			return alias
		}
		return imported.Name()
	}
	model := Interface{Name: entity.name, Methods: make([]Method, len(entity.methods))}
	for i, method := range entity.methods {
		loaded := Method{Name: method.name}
		for _, parameter := range method.params {
			loaded.Params = append(loaded.Params, Parameter{Name: parameter.name, Type: types.TypeString(parameter.typ, qualifier)})
		}
		for _, result := range method.results {
			loaded.Results = append(loaded.Results, types.TypeString(result, qualifier))
		}
		model.Methods[i] = loaded
	}
	return model
}

// templateImportPaths are the imports the generated file always emits itself.
// Entries for them are filtered out before rendering, so they keep their
// package name and are never aliased.
var templateImportPaths = map[string]bool{
	"context":                 true,
	"fmt":                     true,
	"github.com/suraciii/gor": true,
}

// effectiveImportNames decides the name under which each import appears in
// the generated file: the package's own name, or an alias when that name
// would collide with another import or with a name the generated file uses
// itself (the template's context, fmt and gor imports, and the source package
// name). Only colliding imports are aliased; everything else keeps its
// package name, so adding a new import never churns existing generated
// output. The source package's own import line is part of the same
// allocation: when its name is one of the template's fixed names, the map
// holds an alias for the source import path. The result is a deterministic
// function of the import set: the same input always yields the same aliases.
func effectiveImportNames(imports map[string]Import, sourcePackageName, sourceImportPath string) map[string]string {
	reserved := map[string]bool{
		"context": true,
		"fmt":     true,
		"gor":     true,
	}
	used := make(map[string]bool, len(reserved)+len(imports)+1)
	for name := range reserved {
		used[name] = true
	}
	used[sourcePackageName] = true
	for _, imported := range imports {
		used[imported.Name] = true
	}

	var conflicting []Import
	for path, imported := range imports {
		if templateImportPaths[path] || path == sourceImportPath {
			continue
		}
		if reserved[imported.Name] || imported.Name == sourcePackageName || countImportsNamed(imports, imported.Name) > 1 {
			conflicting = append(conflicting, imported)
		}
	}
	// The source import line can only collide with the template's fixed
	// names: a signature import that shares the source name is aliased away
	// instead, so aliasing the source here never churns existing artifacts.
	if reserved[sourcePackageName] {
		conflicting = append(conflicting, Import{Name: sourcePackageName, Path: sourceImportPath})
	}
	sort.Slice(conflicting, func(i, j int) bool { return conflicting[i].Path < conflicting[j].Path })

	// Assign aliases in rounds, one path segment deeper each time, so that
	// equally nested packages get equally shaped aliases (a/x/domain and
	// b/x/domain become axdomain and bxdomain, not xdomain and bxdomain).
	aliases := make(map[string]string)
	remaining := conflicting
	deepest := 0
	for _, imported := range remaining {
		if depth := len(strings.Split(imported.Path, "/")); depth > deepest {
			deepest = depth
		}
	}
	for depth := 1; depth <= deepest && len(remaining) > 0; depth++ {
		var next []Import
		proposed := make(map[string]int, len(remaining))
		candidates := make(map[string]string, len(remaining))
		for _, imported := range remaining {
			candidate := aliasCandidate(imported, depth)
			candidates[imported.Path] = candidate
			if candidate != "" {
				proposed[candidate]++
			}
		}
		for _, imported := range remaining {
			candidate := candidates[imported.Path]
			// A package's own name is in used, so the candidate equal to it
			// (the last path segment when it matches the package name) can
			// never be claimed.
			if candidate == "" || proposed[candidate] > 1 || used[candidate] {
				next = append(next, imported)
				continue
			}
			aliases[imported.Path] = candidate
			used[candidate] = true
		}
		remaining = next
	}
	for _, imported := range remaining {
		for suffix := 2; ; suffix++ {
			candidate := fmt.Sprintf("%s%d", imported.Name, suffix)
			if used[candidate] {
				continue
			}
			aliases[imported.Path] = candidate
			used[candidate] = true
			break
		}
	}
	return aliases
}

func countImportsNamed(imports map[string]Import, name string) int {
	count := 0
	for _, imported := range imports {
		if imported.Name == name {
			count++
		}
	}
	return count
}

// aliasCandidate returns the concatenation of the last depth path segments of
// imported.Path, sanitized into a Go identifier. It is empty when depth
// exceeds the number of segments.
func aliasCandidate(imported Import, depth int) string {
	segments := strings.Split(imported.Path, "/")
	if depth > len(segments) {
		return ""
	}
	return sanitizeIdentifier(strings.Join(segments[len(segments)-depth:], ""))
}

// sanitizeIdentifier keeps letters, digits and underscores and replaces every
// other character with an underscore, prefixing an underscore when the result
// would start with a digit.
func sanitizeIdentifier(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	for _, r := range value {
		if r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}
	result := builder.String()
	if result == "" {
		return result
	}
	if first, _ := utf8.DecodeRuneInString(result); unicode.IsDigit(first) {
		return "_" + result
	}
	return result
}

func isContext(value types.Type) bool {
	named, ok := value.(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "context" && named.Obj().Name() == "Context"
}

func isError(value types.Type) bool {
	return types.Identical(value, types.Universe.Lookup("error").Type())
}

func hasGrainMarker(declaration *ast.GenDecl, specification *ast.TypeSpec) bool {
	return commentsHaveMarker(declaration.Doc) || commentsHaveMarker(specification.Doc)
}

func commentsHaveMarker(group *ast.CommentGroup) bool {
	if group == nil {
		return false
	}
	for _, comment := range group.List {
		text := strings.TrimSpace(comment.Text)
		if strings.HasPrefix(text, "//") {
			text = strings.TrimSpace(strings.TrimPrefix(text, "//"))
		} else if strings.HasPrefix(text, "/*") && strings.HasSuffix(text, "*/") {
			text = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(text, "/*"), "*/"))
		}
		if text == "gor:grain" {
			return true
		}
	}
	return false
}

func locatedError(fileSet *token.FileSet, position token.Pos, format string, args ...any) error {
	positionInfo := fileSet.PositionFor(position, true)
	return fmt.Errorf("%s:%d: %s", positionInfo.Filename, positionInfo.Line, fmt.Sprintf(format, args...))
}
