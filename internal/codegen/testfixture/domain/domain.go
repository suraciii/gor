package domain

import "context"

type Account interface {
	Lookup(ctx context.Context, key string) (int64, string, error)
	Reset(ctx context.Context) error
}

type Ledger interface {
	Balance(ctx context.Context) (int64, error)
}
