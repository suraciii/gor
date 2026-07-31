package invalid

//gor:entity
type Broken interface {
	Call(value string) error
}
