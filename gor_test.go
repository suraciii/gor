package gor

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
)

type Account interface {
	Deposit(context.Context, int64) (int64, error)
	Balance(context.Context) (int64, error)
}

type account struct {
	value int
}

func (a *account) Deposit(_ context.Context, amount int64) (int64, error) {
	a.value += int(amount)
	return int64(a.value), nil
}

func (a *account) Balance(context.Context) (int64, error) {
	return int64(a.value), nil
}

func TestRegister_InvokesHandWrittenDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()

		if err := Register[Account](rt, func() Account { return &account{} }, func(ctx context.Context, instance Account, method string, args []any, reply any) error {
			switch method {
			case "Deposit":
				value, err := instance.Deposit(ctx, args[0].(int64))
				if err != nil {
					return err
				}
				*(reply.(*int64)) = value
				return nil
			case "Balance":
				value, err := instance.Balance(ctx)
				if err != nil {
					return err
				}
				*(reply.(*int64)) = value
				return nil
			case "Missing":
				return errors.New("missing method")
			default:
				return errors.New("unknown method")
			}
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: TypeName[Account](), Key: "alice"}
		var first int64
		if err := rt.Invoke(context.Background(), id, "Deposit", []any{int64(2)}, &first); err != nil {
			t.Fatalf("first invoke error = %v", err)
		}
		var second int64
		if err := rt.Invoke(context.Background(), id, "Deposit", []any{int64(3)}, &second); err != nil {
			t.Fatalf("second invoke error = %v", err)
		}
		var balance int64
		if err := rt.Invoke(context.Background(), id, "Balance", nil, &balance); err != nil {
			t.Fatalf("balance invoke error = %v", err)
		}
		if first != 2 || second != 5 || balance != 5 {
			t.Fatalf("replies = %d, %d, %d; want 2, 5, 5", first, second, balance)
		}
		if err := rt.Invoke(context.Background(), id, "Missing", nil, nil); err == nil {
			t.Fatal("missing method returned nil error")
		}
	})
}

func TestRegister_RejectsDuplicateType(t *testing.T) {
	rt := New(WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()

	dispatch := func(context.Context, Account, string, []any, any) error { return nil }
	if err := Register[Account](rt, func() Account { return &account{} }, dispatch); err != nil {
		t.Fatal(err)
	}
	if err := Register[Account](rt, func() Account { return &account{} }, dispatch); err == nil {
		t.Fatal("duplicate registration returned nil error")
	}
}
