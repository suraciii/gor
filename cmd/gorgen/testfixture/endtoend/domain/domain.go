package domain

import (
	"context"

	"github.com/suraciii/gor"
)

//gor:grain
type Account interface {
	Deposit(ctx context.Context, amount int64) (int64, error)
	Snapshot(ctx context.Context) (int64, string, error)
	Reset(ctx context.Context) error
	Tick(ctx context.Context, status gor.TickStatus) error
}

type account struct {
	balance gor.State[int64]
}

func NewAccount(b *gor.Binder) Account {
	return &account{balance: gor.NewState[int64](b, "balance")}
}

func (a *account) Deposit(ctx context.Context, amount int64) (int64, error) {
	value := a.balance.Get() + amount
	if err := a.balance.Set(ctx, value); err != nil {
		return 0, err
	}
	return value, nil
}

func (a *account) Snapshot(context.Context) (int64, string, error) {
	return a.balance.Get(), "account", nil
}

func (a *account) Reset(ctx context.Context) error {
	return a.balance.Set(ctx, 0)
}

func (a *account) Tick(context.Context, gor.TickStatus) error {
	return nil
}
