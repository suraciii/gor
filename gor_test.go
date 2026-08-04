package gor

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/suraciii/gor/store"
)

type Account interface {
	Deposit(context.Context, int64) (int64, error)
	Balance(context.Context) (int64, error)
}

type account struct {
	value State[int64]
}

func mustNew(t *testing.T, options ...Option) *Runtime {
	t.Helper()
	rt, err := New(options...)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func (a *account) Deposit(ctx context.Context, amount int64) (int64, error) {
	value := a.value.Get() + amount
	if err := a.value.Set(ctx, value); err != nil {
		return 0, err
	}
	return value, nil
}

func (a *account) Balance(context.Context) (int64, error) {
	return a.value.Get(), nil
}

func TestRegister_InvokesInstalledDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()

		installAccount(t, rt)
		if err := Register[Account](rt, func(b *Binder) Account {
			return &account{value: NewState[int64](b, "value")}
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
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()

	installAccount(t, rt)
	if err := Register[Account](rt, func(*Binder) Account { return &account{} }); err != nil {
		t.Fatal(err)
	}
	if err := Register[Account](rt, func(*Binder) Account { return &account{} }); err == nil {
		t.Fatal("duplicate registration returned nil error")
	}
}

func TestRegister_RejectsUninstalledType(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()

	if err := Register[Account](rt, func(*Binder) Account { return &account{} }); !errors.Is(err, ErrTypeNotInstalled) {
		t.Fatalf("Register error = %v, want ErrTypeNotInstalled", err)
	}
}

func TestInstallType_IsScopedToRuntime(t *testing.T) {
	first := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer first.Close()
	second := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer second.Close()

	installAccount(t, first)
	if err := Register[Account](second, func(*Binder) Account { return &account{} }); !errors.Is(err, ErrTypeNotInstalled) {
		t.Fatalf("second runtime Register error = %v, want ErrTypeNotInstalled", err)
	}
}

func TestRef_ConstructsTypedProxyFromInstalledType(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	installAccount(t, rt)
	if err := Register[Account](rt, func(b *Binder) Account {
		return &account{value: NewState[int64](b, "value")}
	}); err != nil {
		t.Fatal(err)
	}

	accountRef := Ref[Account](rt, "alice")
	if _, ok := accountRef.(*accountProxy); !ok {
		t.Fatalf("Ref returned %T, want *accountProxy", accountRef)
	}
	value, err := accountRef.Deposit(context.Background(), 2)
	if err != nil || value != 2 {
		t.Fatalf("Ref proxy Deposit = (%d, %v), want (2, nil)", value, err)
	}
}

func TestRef_PanicsForUninstalledType(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("Ref did not panic for uninstalled type")
		}
	}()
	Ref[Account](rt, "alice")
}

func TestRegister_LoadsAndPersistsState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := store.NewMemory()
		id := store.Identity{Type: TypeName[Account](), Key: "alice"}
		if _, err := backend.Write(context.Background(), id, []byte(`{"value":7}`), 0); err != nil {
			t.Fatalf("seed Write: %v", err)
		}

		rt := mustNew(t, WithStore(backend), WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()
		installAccount(t, rt)
		if err := Register[Account](rt, func(b *Binder) Account {
			return &account{value: NewState[int64](b, "value")}
		}); err != nil {
			t.Fatal(err)
		}

		var balance int64
		if err := rt.Invoke(context.Background(), Identity(id), "Balance", nil, &balance); err != nil {
			t.Fatalf("Balance invoke error = %v", err)
		}
		if balance != 7 {
			t.Fatalf("loaded balance = %d, want 7", balance)
		}

		var deposited int64
		if err := rt.Invoke(context.Background(), Identity(id), "Deposit", []any{int64(2)}, &deposited); err != nil {
			t.Fatalf("Deposit invoke error = %v", err)
		}
		if deposited != 9 {
			t.Fatalf("deposited balance = %d, want 9", deposited)
		}

		record, err := backend.Read(context.Background(), id)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(record.Data) != `{"value":9}` || record.ETag != 2 {
			t.Fatalf("stored record = %#v, want value 9 and ETag 2", record)
		}
	})
}

func TestRuntime_RestartRestoresStateFromMemoryStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := store.NewMemory()
		id := Identity{Type: TypeName[Account](), Key: "alice"}

		first := mustNew(t, WithStore(backend), WithIdleTimeout(0), WithEvictionInterval(0))
		registerAccount(t, first)
		var written int64
		if err := first.Invoke(context.Background(), id, "Deposit", []any{int64(42)}, &written); err != nil {
			t.Fatalf("first Deposit invoke error = %v", err)
		}
		first.Close()

		second := mustNew(t, WithStore(backend), WithIdleTimeout(0), WithEvictionInterval(0))
		registerAccount(t, second)
		defer second.Close()
		var restored int64
		if err := second.Invoke(context.Background(), id, "Balance", nil, &restored); err != nil {
			t.Fatalf("restarted Balance invoke error = %v", err)
		}
		if restored != 42 {
			t.Fatalf("restored balance = %d, want 42", restored)
		}
	})
}

func TestRuntime_RestartRestoresStateFromSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gor.db")
	firstStore, err := store.OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite first: %v", err)
	}
	first := mustNew(t, WithStore(firstStore), WithIdleTimeout(0), WithEvictionInterval(0))
	registerAccount(t, first)
	var written int64
	if err := first.Invoke(context.Background(), Identity{Type: TypeName[Account](), Key: "alice"}, "Deposit", []any{int64(42)}, &written); err != nil {
		first.Close()
		firstStore.Close()
		t.Fatalf("first Deposit invoke error = %v", err)
	}
	first.Close()
	if err := firstStore.Close(); err != nil {
		t.Fatalf("Close first store: %v", err)
	}

	secondStore, err := store.OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite second: %v", err)
	}
	second := mustNew(t, WithStore(secondStore), WithIdleTimeout(0), WithEvictionInterval(0))
	registerAccount(t, second)
	defer second.Close()
	defer secondStore.Close()
	var restored int64
	if err := second.Invoke(context.Background(), Identity{Type: TypeName[Account](), Key: "alice"}, "Balance", nil, &restored); err != nil {
		t.Fatalf("restarted Balance invoke error = %v", err)
	}
	if restored != 42 {
		t.Fatalf("restored balance = %d, want 42", restored)
	}
}

func dispatchAccountWithWrappedError(ctx context.Context, instance Account, method string, args []any, reply any) error {
	err := dispatchAccount(ctx, instance, method, args, reply)
	if err != nil {
		return fmt.Errorf("domain update: %w", err)
	}
	return nil
}

func TestRegister_ConflictDiscardsActivationBeforeReactivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := store.NewMemory()
		id := Identity{Type: TypeName[Account](), Key: "alice"}
		rt := mustNew(t, WithStore(backend), WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()

		var factoryCalls atomic.Int32
		installAccountWithDispatch(t, rt, dispatchAccountWithWrappedError)
		if err := Register[Account](rt, func(b *Binder) Account {
			factoryCalls.Add(1)
			return &account{value: NewState[int64](b, "value")}
		}); err != nil {
			t.Fatal(err)
		}

		var first int64
		if err := rt.Invoke(context.Background(), id, "Deposit", []any{int64(10)}, &first); err != nil {
			t.Fatalf("first Deposit invoke error = %v", err)
		}
		if first != 10 {
			t.Fatalf("first balance = %d, want 10", first)
		}

		storeID := store.Identity(id)
		if _, err := backend.Write(context.Background(), storeID, []byte(`{"value":100}`), 1); err != nil {
			t.Fatalf("external Write: %v", err)
		}

		var conflictResult int64
		err := rt.Invoke(context.Background(), id, "Deposit", []any{int64(1)}, &conflictResult)
		if !errors.Is(err, store.ErrConflict) {
			t.Fatalf("conflicting Deposit error = %v, want store.ErrConflict", err)
		}
		if !strings.HasPrefix(err.Error(), "domain update:") {
			t.Fatalf("conflicting Deposit error = %v, want user error wrapper", err)
		}

		var second int64
		if err := rt.Invoke(context.Background(), id, "Deposit", []any{int64(1)}, &second); err != nil {
			t.Fatalf("reactivated Deposit invoke error = %v", err)
		}
		if second != 101 {
			t.Fatalf("reactivated balance = %d, want 101", second)
		}
		if factoryCalls.Load() != 2 {
			t.Fatalf("factory calls = %d, want 2", factoryCalls.Load())
		}

		record, err := backend.Read(context.Background(), storeID)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(record.Data) != `{"value":101}` || record.ETag != 3 {
			t.Fatalf("stored record = %#v, want value 101 and ETag 3", record)
		}
	})
}

func dispatchAccount(ctx context.Context, instance Account, method string, args []any, reply any) error {
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
	default:
		return errors.New("unknown method")
	}
}

func registerAccount(t *testing.T, rt *Runtime) {
	t.Helper()
	installAccount(t, rt)
	if err := Register[Account](rt, func(b *Binder) Account {
		return &account{value: NewState[int64](b, "value")}
	}); err != nil {
		t.Fatal(err)
	}
}

type accountProxy struct {
	invoker Invoker
	id      Identity
}

func (p *accountProxy) Deposit(ctx context.Context, amount int64) (int64, error) {
	var reply int64
	err := p.invoker.Invoke(ctx, p.id, "Deposit", []any{amount}, &reply)
	return reply, err
}

func (p *accountProxy) Balance(ctx context.Context) (int64, error) {
	var reply int64
	err := p.invoker.Invoke(ctx, p.id, "Balance", nil, &reply)
	return reply, err
}

func installAccount(t *testing.T, rt *Runtime) {
	t.Helper()
	installAccountWithDispatch(t, rt, dispatchAccount)
}

func installAccountWithDispatch(t *testing.T, rt *Runtime, dispatch func(context.Context, Account, string, []any, any) error) {
	t.Helper()
	if err := InstallType[Account](rt, dispatch, func(invoker Invoker, id Identity) Account {
		return &accountProxy{invoker: invoker, id: id}
	}); err != nil {
		t.Fatal(err)
	}
}
