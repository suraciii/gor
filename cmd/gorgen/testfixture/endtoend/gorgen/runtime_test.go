package gorgen

import (
	"context"
	"testing"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/cmd/gorgen/testfixture/endtoend/domain"
	"github.com/suraciii/gor/store"
)

func TestGeneratedAccountPersistsAcrossRestart(t *testing.T) {
	backend := store.NewMemory()

	first := newRuntime(t, backend)
	account := gor.Ref[domain.Account](first, "alice")
	if value, err := account.Deposit(context.Background(), 7); err != nil || value != 7 {
		t.Fatalf("Deposit = (%d, %v), want (7, nil)", value, err)
	}
	first.Close()

	second := newRuntime(t, backend)
	account = gor.Ref[domain.Account](second, "alice")
	if value, label, err := account.Snapshot(context.Background()); err != nil || value != 7 || label != "account" {
		t.Fatalf("Snapshot = (%d, %q, %v), want (7, account, nil)", value, label, err)
	}
	if err := account.Reset(context.Background()); err != nil {
		t.Fatalf("Reset = %v, want nil", err)
	}
	second.Close()

	third := newRuntime(t, backend)
	account = gor.Ref[domain.Account](third, "alice")
	defer third.Close()
	if value, label, err := account.Snapshot(context.Background()); err != nil || value != 0 || label != "account" {
		t.Fatalf("Snapshot after reset = (%d, %q, %v), want (0, account, nil)", value, label, err)
	}
}

func newRuntime(t *testing.T, backend store.Store) *gor.Runtime {
	t.Helper()
	rt, err := gor.New(gor.WithStore(backend), gor.WithIdleTimeout(0), gor.WithEvictionInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := Install(rt); err != nil {
		t.Fatal(err)
	}
	if err := gor.Register[domain.Account](rt, domain.NewAccount); err != nil {
		t.Fatal(err)
	}
	return rt
}
