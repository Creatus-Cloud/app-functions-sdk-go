package interfaces

//go:generate go run github.com/vektra/mockery/v2@v2.53.5 --name ApplicationService --dir . --output ./mocks --outpkg mocks --filename ApplicationService.go --disable-version-string

// ConfigValidationRegistrar registers a service's existing validation function
// for preflight. Call it explicitly during app startup or driver Initialize.
// The factory must return a fresh struct pointer for each validation, and the
// validator must not perform I/O or mutate live state. LoadCustomConfig records
// required sections but never discovers or registers validation methods.
type ConfigValidationRegistrar interface {
	RegisterConfigValidator(factory func() any, validate func(any) error) error
}
