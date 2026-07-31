package domain

import "context"

//gor:entity
type Account interface {
	Deposit(ctx context.Context, amount int64) (int64, error)
	Reset(ctx context.Context) error
}

type Helper interface {
	NotAnEntity(string) error
}
