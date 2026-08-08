// Package context is a fixture whose package name collides with one of the
// names the generated file always imports itself. It exercises the alias
// allocation of the source package's own import line.
package context

import "context"

//gor:grain
type Account interface {
	Deposit(ctx context.Context, amount int64) (int64, error)
}
