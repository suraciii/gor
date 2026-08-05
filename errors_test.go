package gor

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

const testApplicationCode Code = "test.application_failure"

func TestCodeOfUsesOnlySingleValueUnwrap(t *testing.T) {
	wrapped := fmt.Errorf("wrapped: %w", testApplicationCode)
	if got, ok := CodeOf(wrapped); !ok || got != testApplicationCode {
		t.Fatalf("CodeOf(wrapped) = (%q, %v), want (%q, true)", got, ok, testApplicationCode)
	}

	joined := errors.Join(testApplicationCode, errors.New("another error"))
	if got, ok := CodeOf(joined); ok {
		t.Fatalf("CodeOf(joined) = (%q, true), want no code", got)
	}

	if !errors.Is(wrapped, testApplicationCode) {
		t.Fatal("wrapped application code did not match with errors.Is")
	}
}

func TestCodeOfPrefersTheOuterResultAroundJoinedDiagnostics(t *testing.T) {
	outer := withCode(testApplicationCode, errors.Join(errors.New("method failed"), withCode(ErrPersistenceFailed, errors.New("store unavailable"))))
	if got, ok := CodeOf(outer); !ok || got != testApplicationCode {
		t.Fatalf("CodeOf(outer) = (%q, %v), want (%q, true)", got, ok, testApplicationCode)
	}
}

func TestInvokePreservesApplicationCodeAndMapsFrameworkCode(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	installAccountWithDispatch(t, rt, func(ctx context.Context, instance Account, method string, args any, reply any) error {
		if method == "Deposit" {
			return fmt.Errorf("application failure: %w", testApplicationCode)
		}
		return dispatchAccount(ctx, instance, method, args, reply)
	})
	if err := Register[Account](rt, func(b *Binder) Account {
		return &account{value: NewState[int64](b, "value")}
	}); err != nil {
		t.Fatal(err)
	}

	err := rt.Invoke(context.Background(), Identity{Type: TypeName[Account](), Key: "alice"}, "Deposit", &accountDepositRequest{}, &accountDepositReply{})
	if !errors.Is(err, testApplicationCode) {
		t.Fatalf("application error = %v, want application code", err)
	}
	if got, ok := CodeOf(err); !ok || got != testApplicationCode {
		t.Fatalf("CodeOf(application error) = (%q, %v), want (%q, true)", got, ok, testApplicationCode)
	}

	rt.Close()
	err = rt.Invoke(context.Background(), Identity{Type: TypeName[Account](), Key: "alice"}, "Balance", &accountBalanceRequest{}, &accountBalanceReply{})
	if !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("closed runtime error = %v, want ErrRuntimeClosed", err)
	}
}
