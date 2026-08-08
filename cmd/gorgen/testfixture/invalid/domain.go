package invalid

//gor:grain
type Broken interface {
	Call(value string) error
}
