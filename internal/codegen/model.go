package codegen

type Model struct {
	PackageName       string
	SourcePackageName string
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
