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
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

type Account interface {
	Deposit(context.Context, int64) (int64, error)
	Balance(context.Context) (int64, error)
}

type accountDepositRequest struct {
	A0 int64
}

type accountDepositReply struct {
	R0 int64
}

type accountBalanceRequest struct{}

type accountBalanceReply struct {
	R0 int64
}

type account struct {
	value State[int64]
}

type writeIgnoringAccount struct {
	value     State[int64]
	methodErr error
}

func (a *writeIgnoringAccount) Deposit(ctx context.Context, amount int64) (int64, error) {
	value := a.value.Get() + amount
	_ = a.value.Set(ctx, value)
	return value, a.methodErr
}

func (a *writeIgnoringAccount) Balance(context.Context) (int64, error) {
	return a.value.Get(), nil
}

type lifecycleAccount interface {
	Value(context.Context) (int, error)
}

type lifecycleAccountValueRequest struct{}

type lifecycleAccountValueReply struct {
	R0 int
}

type lifecycleAccountEntity struct {
	value           State[int]
	activateErr     error
	deactivateErr   error
	deactivateCalls *atomic.Int32
	events          chan string
}

type lifecycleAccountProxy struct {
	invoker Invoker
	id      Identity
}

func (e *lifecycleAccountEntity) OnActivate(context.Context) error {
	if e.events != nil {
		e.events <- fmt.Sprintf("activate:%d", e.value.Get())
	}
	return e.activateErr
}

func (e *lifecycleAccountEntity) OnDeactivate(context.Context) error {
	if e.deactivateCalls != nil {
		e.deactivateCalls.Add(1)
	}
	if e.events != nil {
		e.events <- "deactivate"
	}
	return e.deactivateErr
}

func (e *lifecycleAccountEntity) Value(context.Context) (int, error) {
	if e.events != nil {
		e.events <- "value"
	}
	return e.value.Get(), nil
}

func (p *lifecycleAccountProxy) Value(ctx context.Context) (int, error) {
	var reply lifecycleAccountValueReply
	err := p.invoker.Invoke(ctx, p.id, "Value", &lifecycleAccountValueRequest{}, &reply)
	return reply.R0, err
}

func dispatchLifecycleAccount(ctx context.Context, instance lifecycleAccount, method string, _ any, reply any) error {
	if method != "Value" {
		return fmt.Errorf("unknown method %q", method)
	}
	typedReply := reply.(*lifecycleAccountValueReply)
	value, err := instance.Value(ctx)
	if err == nil {
		typedReply.R0 = value
	}
	return err
}

func newLifecycleAccountCall(method string) (args any, reply any) {
	if method != "Value" {
		return nil, nil
	}
	return &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}
}

func installLifecycleAccount(t *testing.T, rt *Runtime, factoryCalls *atomic.Int32, configure func(*lifecycleAccountEntity)) {
	t.Helper()
	if err := InstallType[lifecycleAccount](rt, dispatchLifecycleAccount, func(invoker Invoker, id Identity) lifecycleAccount {
		return &lifecycleAccountProxy{invoker: invoker, id: id}
	}, newLifecycleAccountCall); err != nil {
		t.Fatal(err)
	}
	if err := Register[lifecycleAccount](rt, func(b *Binder) lifecycleAccount {
		factoryCalls.Add(1)
		entity := &lifecycleAccountEntity{value: NewState[int](b, "value")}
		configure(entity)
		return entity
	}); err != nil {
		t.Fatal(err)
	}
}

type reportedError struct {
	id     Identity
	method string
	err    error
}

type scopeAccount interface {
	CreatedAt(context.Context) (time.Time, error)
	ForwardDeposit(context.Context, int64) (int64, error)
}

type scopeAccountCreatedAtRequest struct{}

type scopeAccountCreatedAtReply struct {
	R0 time.Time
}

type scopeAccountForwardDepositRequest struct {
	A0 int64
}

type scopeAccountForwardDepositReply struct {
	R0 int64
}

type scopeAccountEntity struct {
	createdAt time.Time
	target    Account
}

func (a *scopeAccountEntity) CreatedAt(context.Context) (time.Time, error) {
	return a.createdAt, nil
}

func (a *scopeAccountEntity) ForwardDeposit(ctx context.Context, amount int64) (int64, error) {
	return a.target.Deposit(ctx, amount)
}

type scopeAccountProxy struct {
	invoker Invoker
	id      Identity
}

func (p *scopeAccountProxy) CreatedAt(ctx context.Context) (time.Time, error) {
	var reply scopeAccountCreatedAtReply
	err := p.invoker.Invoke(ctx, p.id, "CreatedAt", &scopeAccountCreatedAtRequest{}, &reply)
	return reply.R0, err
}

func (p *scopeAccountProxy) ForwardDeposit(ctx context.Context, amount int64) (int64, error) {
	var reply scopeAccountForwardDepositReply
	err := p.invoker.Invoke(ctx, p.id, "ForwardDeposit", &scopeAccountForwardDepositRequest{A0: amount}, &reply)
	return reply.R0, err
}

func newScopeAccountCall(method string) (args any, reply any) {
	switch method {
	case "CreatedAt":
		return &scopeAccountCreatedAtRequest{}, &scopeAccountCreatedAtReply{}
	case "ForwardDeposit":
		return &scopeAccountForwardDepositRequest{}, &scopeAccountForwardDepositReply{}
	default:
		return nil, nil
	}
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

func TestLifecycle_OnActivateRunsAfterLoadBeforeFirstCall(t *testing.T) {
	backend := store.NewMemory()
	id := store.Identity{Type: TypeName[lifecycleAccount](), Key: "alice"}
	if _, err := backend.Write(context.Background(), id, []byte(`{"value":7}`), 0); err != nil {
		t.Fatalf("seed Write: %v", err)
	}
	events := make(chan string, 3)
	rt := mustNew(t, WithStore(backend), WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
		entity.events = events
	})

	value, err := Ref[lifecycleAccount](rt, "alice").Value(context.Background())
	if err != nil || value != 7 {
		t.Fatalf("Value = (%d, %v), want (7, nil)", value, err)
	}
	got := []string{<-events, <-events}
	want := []string{"activate:7", "value"}
	if got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("lifecycle events = %v, want %v", got, want)
	}
}

func TestLifecycle_OnActivateFailureDoesNotEstablishActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		activateErr := errors.New("activate failed")
		factoryCalls := new(atomic.Int32)
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()
		installLifecycleAccount(t, rt, factoryCalls, func(entity *lifecycleAccountEntity) {
			entity.activateErr = activateErr
		})

		id := Identity{Type: TypeName[lifecycleAccount](), Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); !errors.Is(err, activateErr) {
			t.Fatalf("first activation error = %v, want %v", err, activateErr)
		}
		if activations := rt.Activations(); len(activations) != 0 {
			t.Fatalf("Activations after failed activation = %#v, want empty", activations)
		}
		if err := rt.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); !errors.Is(err, activateErr) {
			t.Fatalf("second activation error = %v, want %v", err, activateErr)
		}
		if got := factoryCalls.Load(); got != 2 {
			t.Fatalf("factory calls after failed activations = %d, want 2", got)
		}
	})
}

func TestLifecycle_OnDeactivateFailureReportsAndRemovesActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deactivateErr := errors.New("deactivate failed")
		errorsSeen := make(chan reportedError, 1)
		fakeClock := clock.NewFake(time.Unix(0, 0).UTC())
		rt := mustNew(t,
			WithClock(fakeClock),
			WithIdleTimeout(time.Second),
			WithEvictionInterval(time.Second),
			OnError(func(id Identity, method string, err error) {
				errorsSeen <- reportedError{id: id, method: method, err: err}
			}),
		)
		defer rt.Close()
		deactivateCalls := new(atomic.Int32)
		installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateErr = deactivateErr
			entity.deactivateCalls = deactivateCalls
		})

		id := Identity{Type: TypeName[lifecycleAccount](), Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); err != nil {
			t.Fatalf("initial Value: %v", err)
		}
		fakeClock.Advance(time.Second)
		synctest.Wait()
		if activations := rt.Activations(); len(activations) != 0 {
			t.Fatalf("Activations after failed deactivation = %#v, want empty", activations)
		}
		if got := deactivateCalls.Load(); got != 1 {
			t.Fatalf("OnDeactivate calls = %d, want 1", got)
		}
		select {
		case got := <-errorsSeen:
			if got.id != id || got.method != "OnDeactivate" || !errors.Is(got.err, deactivateErr) {
				t.Fatalf("reported error = %#v, want id %v, method OnDeactivate, error %v", got, id, deactivateErr)
			}
		default:
			t.Fatal("OnDeactivate error was not reported")
		}
	})
}

func TestLifecycle_KillSkipsOnDeactivate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()
		deactivateCalls := new(atomic.Int32)
		installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateCalls = deactivateCalls
		})

		id := Identity{Type: TypeName[lifecycleAccount](), Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); err != nil {
			t.Fatalf("initial Value: %v", err)
		}
		rt.Kill()
		synctest.Wait()
		if got := deactivateCalls.Load(); got != 0 {
			t.Fatalf("OnDeactivate calls after Kill = %d, want 0", got)
		}
	})
}

func dispatchScopeAccount(ctx context.Context, instance scopeAccount, method string, args any, reply any) error {
	switch method {
	case "CreatedAt":
		typedReply := reply.(*scopeAccountCreatedAtReply)
		value, err := instance.CreatedAt(ctx)
		if err == nil {
			typedReply.R0 = value
		}
		return err
	case "ForwardDeposit":
		typedArgs := args.(*scopeAccountForwardDepositRequest)
		typedReply := reply.(*scopeAccountForwardDepositReply)
		value, err := instance.ForwardDeposit(ctx, typedArgs.A0)
		if err == nil {
			typedReply.R0 = value
		}
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
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
		var first accountDepositReply
		if err := rt.Invoke(context.Background(), id, "Deposit", &accountDepositRequest{A0: 2}, &first); err != nil {
			t.Fatalf("first invoke error = %v", err)
		}
		var second accountDepositReply
		if err := rt.Invoke(context.Background(), id, "Deposit", &accountDepositRequest{A0: 3}, &second); err != nil {
			t.Fatalf("second invoke error = %v", err)
		}
		var balance accountBalanceReply
		if err := rt.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &balance); err != nil {
			t.Fatalf("balance invoke error = %v", err)
		}
		if first.R0 != 2 || second.R0 != 5 || balance.R0 != 5 {
			t.Fatalf("replies = %d, %d, %d; want 2, 5, 5", first.R0, second.R0, balance.R0)
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

func TestBinderScope_ProvidesClockAndTypedReferences(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(100, 0).UTC()
		fakeClock := clock.NewFake(start)
		rt := mustNew(t, WithClock(fakeClock), WithIdleTimeout(time.Minute), WithEvictionInterval(time.Minute))
		defer rt.Close()

		installAccount(t, rt)
		if err := Register[Account](rt, func(b *Binder) Account {
			return &account{value: NewState[int64](b, "value")}
		}); err != nil {
			t.Fatal(err)
		}
		if err := InstallType[scopeAccount](rt, dispatchScopeAccount, func(invoker Invoker, id Identity) scopeAccount {
			return &scopeAccountProxy{invoker: invoker, id: id}
		}, newScopeAccountCall); err != nil {
			t.Fatal(err)
		}
		if err := Register[scopeAccount](rt, func(b *Binder) scopeAccount {
			return &scopeAccountEntity{
				createdAt: Now(b),
				target:    Ref[Account](b, "target"),
			}
		}); err != nil {
			t.Fatal(err)
		}

		source := Ref[scopeAccount](rt, "source")
		createdAt, err := source.CreatedAt(context.Background())
		if err != nil {
			t.Fatalf("CreatedAt = %v", err)
		}
		if !createdAt.Equal(start) {
			t.Fatalf("CreatedAt = %s, want %s", createdAt, start)
		}

		value, err := source.ForwardDeposit(context.Background(), 7)
		if err != nil || value != 7 {
			t.Fatalf("ForwardDeposit = (%d, %v), want (7, nil)", value, err)
		}

		fakeClock.Advance(time.Hour)
		synctest.Wait()
		createdAt, err = source.CreatedAt(context.Background())
		if err != nil {
			t.Fatalf("CreatedAt after reactivation = %v", err)
		}
		if !createdAt.Equal(start.Add(time.Hour)) {
			t.Fatalf("CreatedAt after reactivation = %s, want %s", createdAt, start.Add(time.Hour))
		}
	})
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

		var balance accountBalanceReply
		if err := rt.Invoke(context.Background(), Identity(id), "Balance", &accountBalanceRequest{}, &balance); err != nil {
			t.Fatalf("Balance invoke error = %v", err)
		}
		if balance.R0 != 7 {
			t.Fatalf("loaded balance = %d, want 7", balance.R0)
		}

		var deposited accountDepositReply
		if err := rt.Invoke(context.Background(), Identity(id), "Deposit", &accountDepositRequest{A0: 2}, &deposited); err != nil {
			t.Fatalf("Deposit invoke error = %v", err)
		}
		if deposited.R0 != 9 {
			t.Fatalf("deposited balance = %d, want 9", deposited.R0)
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
		var written accountDepositReply
		if err := first.Invoke(context.Background(), id, "Deposit", &accountDepositRequest{A0: 42}, &written); err != nil {
			t.Fatalf("first Deposit invoke error = %v", err)
		}
		first.Close()

		second := mustNew(t, WithStore(backend), WithIdleTimeout(0), WithEvictionInterval(0))
		registerAccount(t, second)
		defer second.Close()
		var restored accountBalanceReply
		if err := second.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &restored); err != nil {
			t.Fatalf("restarted Balance invoke error = %v", err)
		}
		if restored.R0 != 42 {
			t.Fatalf("restored balance = %d, want 42", restored.R0)
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
	var written accountDepositReply
	if err := first.Invoke(context.Background(), Identity{Type: TypeName[Account](), Key: "alice"}, "Deposit", &accountDepositRequest{A0: 42}, &written); err != nil {
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
	var restored accountBalanceReply
	if err := second.Invoke(context.Background(), Identity{Type: TypeName[Account](), Key: "alice"}, "Balance", &accountBalanceRequest{}, &restored); err != nil {
		t.Fatalf("restarted Balance invoke error = %v", err)
	}
	if restored.R0 != 42 {
		t.Fatalf("restored balance = %d, want 42", restored.R0)
	}
}

func dispatchAccountWithWrappedError(ctx context.Context, instance Account, method string, args any, reply any) error {
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

		var first accountDepositReply
		if err := rt.Invoke(context.Background(), id, "Deposit", &accountDepositRequest{A0: 10}, &first); err != nil {
			t.Fatalf("first Deposit invoke error = %v", err)
		}
		if first.R0 != 10 {
			t.Fatalf("first balance = %d, want 10", first.R0)
		}

		storeID := store.Identity(id)
		if _, err := backend.Write(context.Background(), storeID, []byte(`{"value":100}`), 1); err != nil {
			t.Fatalf("external Write: %v", err)
		}

		var conflictResult accountDepositReply
		err := rt.Invoke(context.Background(), id, "Deposit", &accountDepositRequest{A0: 1}, &conflictResult)
		if !errors.Is(err, store.ErrConflict) {
			t.Fatalf("conflicting Deposit error = %v, want store.ErrConflict", err)
		}
		if !strings.HasPrefix(err.Error(), "domain update:") {
			t.Fatalf("conflicting Deposit error = %v, want user error wrapper", err)
		}

		var second accountDepositReply
		if err := rt.Invoke(context.Background(), id, "Deposit", &accountDepositRequest{A0: 1}, &second); err != nil {
			t.Fatalf("reactivated Deposit invoke error = %v", err)
		}
		if second.R0 != 101 {
			t.Fatalf("reactivated balance = %d, want 101", second.R0)
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

func TestRegister_StateWriteFailureReturnsErrorAndDiscardsActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writeErr := errors.New("store unavailable")
		rt := mustNew(t, WithStore(failingWriteStore{err: writeErr}), WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()

		var factoryCalls atomic.Int32
		installAccount(t, rt)
		if err := Register[Account](rt, func(b *Binder) Account {
			factoryCalls.Add(1)
			return &writeIgnoringAccount{value: NewState[int64](b, "value")}
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: TypeName[Account](), Key: "alice"}
		var result accountDepositReply
		err := rt.Invoke(context.Background(), id, "Deposit", &accountDepositRequest{A0: 1}, &result)
		if err == nil {
			t.Fatal("state write failure returned nil error")
		}
		if !errors.Is(err, writeErr) {
			t.Fatalf("state write failure = %v, want %v", err, writeErr)
		}

		var balance accountBalanceReply
		if err := rt.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &balance); err != nil {
			t.Fatalf("reactivated Balance invoke error = %v", err)
		}
		if factoryCalls.Load() != 2 {
			t.Fatalf("factory calls = %d, want 2 after discarded activation", factoryCalls.Load())
		}
	})
}

func TestRegister_StateWriteFailureJoinsMethodError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writeErr := errors.New("store unavailable")
		methodErr := errors.New("method failed")
		rt := mustNew(t, WithStore(failingWriteStore{err: writeErr}), WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()

		installAccount(t, rt)
		if err := Register[Account](rt, func(b *Binder) Account {
			return &writeIgnoringAccount{value: NewState[int64](b, "value"), methodErr: methodErr}
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: TypeName[Account](), Key: "alice"}
		var result accountDepositReply
		err := rt.Invoke(context.Background(), id, "Deposit", &accountDepositRequest{A0: 1}, &result)
		if err == nil {
			t.Fatal("method and state write failures returned nil error")
		}
		if !errors.Is(err, methodErr) {
			t.Fatalf("combined error = %v, want method error %v", err, methodErr)
		}
		if !errors.Is(err, writeErr) {
			t.Fatalf("combined error = %v, want state write error %v", err, writeErr)
		}
	})
}

func dispatchAccount(ctx context.Context, instance Account, method string, args any, reply any) error {
	switch method {
	case "Deposit":
		typedArgs := args.(*accountDepositRequest)
		typedReply := reply.(*accountDepositReply)
		value, err := instance.Deposit(ctx, typedArgs.A0)
		if err != nil {
			return err
		}
		typedReply.R0 = value
		return nil
	case "Balance":
		typedReply := reply.(*accountBalanceReply)
		value, err := instance.Balance(ctx)
		if err != nil {
			return err
		}
		typedReply.R0 = value
		return nil
	default:
		return errors.New("unknown method")
	}
}

func newAccountCall(method string) (args any, reply any) {
	switch method {
	case "Deposit":
		return &accountDepositRequest{}, &accountDepositReply{}
	case "Balance":
		return &accountBalanceRequest{}, &accountBalanceReply{}
	case "Fail":
		return &accountFailRequest{}, &accountFailReply{}
	default:
		return nil, nil
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
	var reply accountDepositReply
	err := p.invoker.Invoke(ctx, p.id, "Deposit", &accountDepositRequest{A0: amount}, &reply)
	return reply.R0, err
}

func (p *accountProxy) Balance(ctx context.Context) (int64, error) {
	var reply accountBalanceReply
	err := p.invoker.Invoke(ctx, p.id, "Balance", &accountBalanceRequest{}, &reply)
	return reply.R0, err
}

func installAccount(t *testing.T, rt *Runtime) {
	t.Helper()
	installAccountWithDispatch(t, rt, dispatchAccount)
}

func installAccountWithDispatch(t *testing.T, rt *Runtime, dispatch func(context.Context, Account, string, any, any) error) {
	t.Helper()
	if err := InstallType[Account](rt, dispatch, func(invoker Invoker, id Identity) Account {
		return &accountProxy{invoker: invoker, id: id}
	}, newAccountCall); err != nil {
		t.Fatal(err)
	}
}
