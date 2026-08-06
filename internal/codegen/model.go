package codegen

type Model struct {
	PackageName       string
	SourcePackageName string
	SourceImportName  string // alias for the source package's import line; empty keeps the package name
	SourceImportPath  string
	Imports           []Import
	Interfaces        []Interface
}

type Import struct {
	Name string
	Path string
}

type Interface struct {
	Name    string
	Methods []Method
}

type Method struct {
	Name    string
	Params  []Parameter
	Results []string
}

type Parameter struct {
	Name string
	Type string
}
