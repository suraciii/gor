package codegen

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

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
	for _, file := range pkg.Syntax {
		for _, declaration := range file.Decls {
			genDecl, ok := declaration.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.TYPE {
				continue
			}
			for _, specification := range genDecl.Specs {
				typeSpec := specification.(*ast.TypeSpec)
				if !hasEntityMarker(genDecl, typeSpec) {
					continue
				}
				if _, ok := typeSpec.Type.(*ast.InterfaceType); !ok {
					return Loaded{}, locatedError(pkg.Fset, typeSpec.Pos(), "%s.%s is marked gor:entity but is not an interface", pkg.Name, typeSpec.Name.Name)
				}
				entity, err := loadInterface(pkg, typeSpec, imports)
				if err != nil {
					return Loaded{}, err
				}
				model.Interfaces = append(model.Interfaces, entity)
			}
		}
	}
	for _, imported := range imports {
		model.Imports = append(model.Imports, imported)
	}
	sort.Slice(model.Imports, func(i, j int) bool { return model.Imports[i].Path < model.Imports[j].Path })
	if len(model.Interfaces) == 0 {
		return Loaded{}, fmt.Errorf("package %s contains no gor:entity interfaces", pkg.PkgPath)
	}
	files := pkg.CompiledGoFiles
	if len(files) == 0 {
		files = pkg.GoFiles
	}
	return Loaded{Model: model, Dir: filepath.Dir(files[0])}, nil
}

func loadInterface(pkg *packages.Package, specification *ast.TypeSpec, imports map[string]Import) (Interface, error) {
	object := pkg.Types.Scope().Lookup(specification.Name.Name)
	if object == nil {
		return Interface{}, locatedError(pkg.Fset, specification.Pos(), "type %s has no type information", specification.Name.Name)
	}
	typeName, ok := object.(*types.TypeName)
	if !ok {
		return Interface{}, locatedError(pkg.Fset, specification.Pos(), "type %s has no type information", specification.Name.Name)
	}
	entity, ok := typeName.Type().Underlying().(*types.Interface)
	if !ok {
		return Interface{}, locatedError(pkg.Fset, specification.Pos(), "type %s is not an interface", specification.Name.Name)
	}
	model := Interface{Name: specification.Name.Name, Methods: make([]Method, entity.NumMethods())}
	for i := 0; i < entity.NumMethods(); i++ {
		method := entity.Method(i)
		signature, ok := method.Type().(*types.Signature)
		if !ok {
			return Interface{}, locatedError(pkg.Fset, method.Pos(), "%s.%s has no method signature", model.Name, method.Name())
		}
		if signature.Params().Len() == 0 || !isContext(signature.Params().At(0).Type()) {
			return Interface{}, locatedError(pkg.Fset, method.Pos(), "%s.%s must have context.Context as its first parameter", model.Name, method.Name())
		}
		if signature.Results().Len() == 0 || !isError(signature.Results().At(signature.Results().Len()-1).Type()) {
			return Interface{}, locatedError(pkg.Fset, method.Pos(), "%s.%s must have error as its last result", model.Name, method.Name())
		}
		loaded := Method{Name: method.Name()}
		for parameter := 0; parameter < signature.Params().Len(); parameter++ {
			variable := signature.Params().At(parameter)
			name := variable.Name()
			if name == "" {
				name = fmt.Sprintf("arg%d", parameter)
			}
			loaded.Params = append(loaded.Params, Parameter{Name: name, Type: typeString(variable.Type(), imports)})
		}
		for result := 0; result < signature.Results().Len(); result++ {
			loaded.Results = append(loaded.Results, typeString(signature.Results().At(result).Type(), imports))
		}
		model.Methods[i] = loaded
	}
	return model, nil
}

func typeString(value types.Type, imports map[string]Import) string {
	return types.TypeString(value, func(imported *types.Package) string {
		if imported == nil {
			return ""
		}
		imports[imported.Path()] = Import{Name: imported.Name(), Path: imported.Path()}
		return imported.Name()
	})
}

func isContext(value types.Type) bool {
	named, ok := value.(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "context" && named.Obj().Name() == "Context"
}

func isError(value types.Type) bool {
	return types.Identical(value, types.Universe.Lookup("error").Type())
}

func hasEntityMarker(declaration *ast.GenDecl, specification *ast.TypeSpec) bool {
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
		if text == "gor:entity" {
			return true
		}
	}
	return false
}

func locatedError(fileSet *token.FileSet, position token.Pos, format string, args ...any) error {
	positionInfo := fileSet.PositionFor(position, true)
	return fmt.Errorf("%s:%d: %s", positionInfo.Filename, positionInfo.Line, fmt.Sprintf(format, args...))
}
