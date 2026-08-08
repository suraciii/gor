package codegen

import (
	"bytes"
	"fmt"
	"go/format"
	"sort"
	"strings"
	"text/template"
	"unicode"
	"unicode/utf8"
)

type renderModel struct {
	PackageName      string
	SourcePackage    string
	SourceImportPath string
	SourceImportName string
	Imports          []renderImport
	Interfaces       []renderInterface
}

type renderImport struct {
	Name string
	Path string
}

type renderInterface struct {
	Name            string
	ProxyName       string
	DispatchName    string
	ConstructorName string
	Methods         []renderMethod
}

type renderMethod struct {
	Name             string
	Params           string
	Results          string
	ContextName      string
	Args             string
	ArgsName         string
	ReplyName        string
	ReplyPointer     string
	DispatchCall     string
	ResultNames      string
	ReplyFields      []renderResult
	ArgsFields       []renderResult
	ReplyAssignments []renderAssignment
	HasValues        bool
}

type renderResult struct {
	Name string
	Type string
}

type renderAssignment struct {
	Field string
	Value string
}

func Render(model Model) ([]byte, error) {
	prepared := prepare(model)
	var source bytes.Buffer
	_ = generatedTemplate.Execute(&source, prepared)
	formatted, err := format.Source(source.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format generated source: %w", err)
	}
	return formatted, nil
}

func prepare(model Model) renderModel {
	sourcePackage := model.SourcePackageName
	if model.SourceImportName != "" {
		sourcePackage = model.SourceImportName
	}
	prepared := renderModel{
		PackageName:      model.PackageName,
		SourcePackage:    sourcePackage,
		SourceImportPath: model.SourceImportPath,
		SourceImportName: model.SourceImportName,
		Imports:          renderImports(model),
		Interfaces:       make([]renderInterface, len(model.Interfaces)),
	}
	for i, entity := range model.Interfaces {
		prepared.Interfaces[i] = renderInterface{
			Name:            entity.Name,
			ProxyName:       lowerFirst(entity.Name) + "Proxy",
			DispatchName:    "dispatch" + entity.Name,
			ConstructorName: "new" + entity.Name + "Proxy",
			Methods:         make([]renderMethod, len(entity.Methods)),
		}
		for j, method := range entity.Methods {
			prepared.Interfaces[i].Methods[j] = prepareMethod(entity, method)
		}
	}
	return prepared
}

func renderImports(model Model) []renderImport {
	imports := make([]renderImport, 0, len(model.Imports))
	for _, imported := range model.Imports {
		switch imported.Path {
		case "context", "fmt", "github.com/suraciii/gor", model.SourceImportPath:
			continue
		}
		imports = append(imports, renderImport(imported))
	}
	sort.Slice(imports, func(i, j int) bool { return imports[i].Path < imports[j].Path })
	return imports
}

func prepareMethod(entity Interface, method Method) renderMethod {
	values := method.Results[:len(method.Results)-1]
	rendered := renderMethod{
		Name:         method.Name,
		Params:       joinParameters(method.Params),
		Results:      joinResults(method.Results),
		ContextName:  method.Params[0].Name,
		ArgsName:     lowerFirst(entity.Name) + method.Name + "Request",
		ReplyName:    lowerFirst(entity.Name) + method.Name + "Reply",
		HasValues:    len(values) > 0,
		DispatchCall: dispatchCall(method, values),
	}
	rendered.Args = joinArgs(rendered.ArgsName, method.Params[1:])
	rendered.ReplyPointer = "&reply"
	for i, param := range method.Params[1:] {
		rendered.ArgsFields = append(rendered.ArgsFields, renderResult{Name: fmt.Sprintf("A%d", i), Type: param.Type})
	}
	for i, result := range values {
		fieldName := fmt.Sprintf("R%d", i)
		rendered.ReplyFields = append(rendered.ReplyFields, renderResult{Name: fieldName, Type: result})
		rendered.ReplyAssignments = append(rendered.ReplyAssignments, renderAssignment{Field: fieldName, Value: fmt.Sprintf("r%d", i)})
	}
	if rendered.HasValues {
		resultNames := make([]string, len(rendered.ReplyFields))
		for i, field := range rendered.ReplyFields {
			resultNames[i] = "reply." + field.Name
		}
		rendered.ResultNames = strings.Join(resultNames, ", ")
	}
	return rendered
}

func dispatchCall(method Method, values []string) string {
	resultNames := make([]string, len(values)+1)
	for i := range values {
		resultNames[i] = fmt.Sprintf("r%d", i)
	}
	resultNames[len(values)] = "err"
	assignment := " := "
	return strings.Join(resultNames, ", ") + assignment + "instance." + method.Name + "(ctx" + dispatchArguments(method.Params[1:]) + ")"
}

func dispatchArguments(params []Parameter) string {
	if len(params) == 0 {
		return ""
	}
	parts := make([]string, len(params))
	for i := range params {
		parts[i] = fmt.Sprintf("typedArgs.A%d", i)
	}
	return ", " + strings.Join(parts, ", ")
}

func joinParameters(params []Parameter) string {
	parts := make([]string, len(params))
	for i, param := range params {
		parts[i] = param.Name + " " + param.Type
	}
	return strings.Join(parts, ", ")
}

func joinResults(results []string) string {
	if len(results) == 1 {
		return results[0]
	}
	return "(" + strings.Join(results, ", ") + ")"
}

func joinArgs(name string, params []Parameter) string {
	parts := make([]string, len(params))
	for i, param := range params {
		parts[i] = fmt.Sprintf("A%d: %s", i, param.Name)
	}
	return "&" + name + "{" + strings.Join(parts, ", ") + "}"
}

func lowerFirst(value string) string {
	runeValue, size := utf8.DecodeRuneInString(value)
	return string(unicode.ToLower(runeValue)) + value[size:]
}

var generatedTemplate = template.Must(template.New("generated").Parse(`package {{.PackageName}}

import (
	"context"
	"fmt"

	"github.com/suraciii/gor"
	{{if .SourceImportName}}{{.SourceImportName}} {{end}}"{{.SourceImportPath}}"
	{{range .Imports}}{{.Name}} "{{.Path}}"
	{{end}}
)

{{range .Interfaces}}{{ $entity := . }}
type {{.ProxyName}} struct {
	id gor.GrainId
	rt gor.Invoker
}

{{range .Methods}}
{{if .ArgsFields}}type {{.ArgsName}} struct {
{{range .ArgsFields}}	{{.Name}} {{.Type}}
{{end}}
}{{else}}type {{.ArgsName}} struct{}{{end}}
{{if .ReplyFields}}type {{.ReplyName}} struct {
{{range .ReplyFields}}	{{.Name}} {{.Type}}
{{end}}}{{else}}type {{.ReplyName}} struct{}{{end}}
func (p *{{$entity.ProxyName}}) {{.Name}}({{.Params}}) {{.Results}} {
	var reply {{.ReplyName}}
	err := p.rt.Invoke({{.ContextName}}, p.id, "{{.Name}}", {{.Args}}, {{.ReplyPointer}})
{{if .HasValues}}	return {{.ResultNames}}, err
{{else}}	return err
{{end}}}
{{end}}
func {{$entity.DispatchName}}(ctx context.Context, instance {{$.SourcePackage}}.{{$entity.Name}}, method string, args any, reply any) error {
	switch method {
{{range .Methods}}	case "{{.Name}}":
{{if .ArgsFields}}		typedArgs := args.(*{{.ArgsName}})
{{end}}{{if .ReplyFields}}		typedReply := reply.(*{{.ReplyName}})
{{end}}		{{.DispatchCall}}
{{range .ReplyAssignments}}			typedReply.{{.Field}} = {{.Value}}
{{end}}		return err
{{end}}	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func new{{ $entity.Name }}Call(method string) (args any, reply any) {
	switch method {
{{range .Methods}}	case "{{.Name}}":
		return &{{.ArgsName}}{}, &{{.ReplyName}}{}
{{end}}	default:
		return nil, nil
	}
}

func {{$entity.ConstructorName}}(rt gor.Invoker, id gor.GrainId) {{$.SourcePackage}}.{{$entity.Name}} {
	return &{{.ProxyName}}{id: id, rt: rt}
}

{{end}}
// Install installs the generated entity bindings in rt.
// Call it once after creating rt and before registering or referencing any of
// the generated entity types. After it returns nil, gor.Register and gor.Ref
// can use those types with rt.
func Install(rt *gor.Runtime) error {
{{range .Interfaces}}	if err := gor.InstallType[{{$.SourcePackage}}.{{.Name}}](rt, {{.DispatchName}}, {{.ConstructorName}}, new{{.Name}}Call); err != nil {
		return err
	}
{{end}}	return nil
}
`))
