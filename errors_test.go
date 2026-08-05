package gor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/suraciii/gor/store"
)

const testApplicationCode Code = "test.application_failure"

func TestCodeOfReturnsSoleReachableCode(t *testing.T) {
	wrapped := fmt.Errorf("wrapped: %w", testApplicationCode)
	if got, ok := CodeOf(wrapped); !ok || got != testApplicationCode {
		t.Fatalf("CodeOf(wrapped) = (%q, %v), want (%q, true)", got, ok, testApplicationCode)
	}

	// A code inside a multi-value unwrap is reachable too; with no other code
	// present it is the sole, determined code.
	joined := errors.Join(testApplicationCode, errors.New("another error"))
	if got, ok := CodeOf(joined); !ok || got != testApplicationCode {
		t.Fatalf("CodeOf(joined) = (%q, %v), want (%q, true)", got, ok, testApplicationCode)
	}

	if !errors.Is(wrapped, testApplicationCode) {
		t.Fatal("wrapped application code did not match with errors.Is")
	}
}

func TestCodeOfReportsNoCodeWhenTreeHasMultipleCodes(t *testing.T) {
	// Two distinct codes in one tree is ambiguous: there is no determined code,
	// so CodeOf reports none. This holds whether the codes sit at different
	// depths or are direct siblings of a join.
	nested := withCode(testApplicationCode, errors.Join(errors.New("method failed"), withCode(ErrPersistenceFailed, errors.New("store unavailable"))))
	if got, ok := CodeOf(nested); ok {
		t.Fatalf("CodeOf(nested) = (%q, true), want no code", got)
	}
	siblings := errors.Join(testApplicationCode, ErrPersistenceFailed)
	if got, ok := CodeOf(siblings); ok {
		t.Fatalf("CodeOf(siblings) = (%q, true), want no code", got)
	}
}

// An incomparable value-type error (slice/map/func field) would panic if CodeOf
// keyed a visited set by the error interface: hashing an unhashable dynamic
// type is a runtime panic on every public call path. CodeOf must not do that.
type unhashableCoded struct{ fields []string }

func (unhashableCoded) Error() string { return "validation failed" }
func (unhashableCoded) Code() Code    { return testApplicationCode }

func TestCodeOfHandlesIncomparableErrorType(t *testing.T) {
	err := unhashableCoded{fields: []string{"name", "email"}}
	if got, ok := CodeOf(err); !ok || got != testApplicationCode {
		t.Fatalf("CodeOf(unhashableCoded) = (%q, %v), want (%q, true)", got, ok, testApplicationCode)
	}
}

// TestJoinedErrorWithSoleCodeRoundTripsAcrossNodes pins the spec's equivalence
// promise at the wire boundary: a method result joining a declared code with
// diagnostic text must match that code after the server projects it and the
// client rebuilds it. Reverting CodeOf to a single-value unwrap walk makes
// this fail: the envelope would carry no code and the rebuild would be opaque.
func TestJoinedErrorWithSoleCodeRoundTripsAcrossNodes(t *testing.T) {
	err := errors.Join(testApplicationCode, errors.New("diagnostic"))
	rebuilt := errorFromEnvelope(errorEnvelopeFor(publicError(err)))
	if !errors.Is(rebuilt, testApplicationCode) {
		t.Fatalf("errors.Is(rebuilt, %q) = false, want true", testApplicationCode)
	}
	if got, ok := CodeOf(rebuilt); !ok || got != testApplicationCode {
		t.Fatalf("CodeOf(rebuilt) = (%q, %v), want (%q, true)", got, ok, testApplicationCode)
	}
}

// TestJoinedErrorWithMultipleCodesIsOpaqueAcrossNodes pins the uniqueness
// rule across the wire: two distinct codes in one tree have no determined
// code, so only text survives. Changing CodeOf to return the first code found
// instead of requiring a single one makes this fail: the envelope would carry
// a code and the rebuild would match it.
func TestJoinedErrorWithMultipleCodesIsOpaqueAcrossNodes(t *testing.T) {
	err := errors.Join(testApplicationCode, ErrPersistenceFailed)
	rebuilt := errorFromEnvelope(errorEnvelopeFor(publicError(err)))
	if got, ok := CodeOf(rebuilt); ok {
		t.Fatalf("CodeOf(rebuilt) = (%q, true), want no code", got)
	}
	if errors.Is(rebuilt, testApplicationCode) {
		t.Fatalf("errors.Is(rebuilt, %q) = true, want false", testApplicationCode)
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

func TestInvokeMapsMethodPanicToErrPanic(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	installAccountWithDispatch(t, rt, func(ctx context.Context, instance Account, method string, args any, reply any) error {
		if method == "Deposit" {
			panic("method exploded")
		}
		return dispatchAccount(ctx, instance, method, args, reply)
	})
	if err := Register[Account](rt, func(b *Binder) Account {
		return &account{value: NewState[int64](b, "value")}
	}); err != nil {
		t.Fatal(err)
	}

	err := rt.Invoke(context.Background(), Identity{Type: TypeName[Account](), Key: "alice"}, "Deposit", &accountDepositRequest{}, &accountDepositReply{})
	if !errors.Is(err, ErrPanic) {
		t.Fatalf("panic error = %v, want ErrPanic", err)
	}
	if got, ok := CodeOf(err); !ok || got != ErrPanic {
		t.Fatalf("CodeOf(panic error) = (%q, %v), want (%q, true)", got, ok, ErrPanic)
	}
}

func TestPublicErrorMapsPersistenceConflict(t *testing.T) {
	err := publicError(store.ErrConflict)
	if !errors.Is(err, ErrPersistenceConflict) {
		t.Fatalf("publicError(store.ErrConflict) = %v, want ErrPersistenceConflict", err)
	}
	if got, ok := CodeOf(err); !ok || got != ErrPersistenceConflict {
		t.Fatalf("CodeOf(publicError(store.ErrConflict)) = (%q, %v), want (%q, true)", got, ok, ErrPersistenceConflict)
	}
}

func TestInvokePreservesContextDeadlineExceeded(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	installAccountWithDispatch(t, rt, func(ctx context.Context, instance Account, method string, args any, reply any) error {
		if method == "Deposit" {
			return context.DeadlineExceeded
		}
		return dispatchAccount(ctx, instance, method, args, reply)
	})
	if err := Register[Account](rt, func(b *Binder) Account {
		return &account{value: NewState[int64](b, "value")}
	}); err != nil {
		t.Fatal(err)
	}

	err := rt.Invoke(context.Background(), Identity{Type: TypeName[Account](), Key: "alice"}, "Deposit", &accountDepositRequest{}, &accountDepositReply{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error = %v, want context.DeadlineExceeded", err)
	}
	if got, ok := CodeOf(err); ok {
		t.Fatalf("CodeOf(deadline error) = (%q, true), want no code", got)
	}
}
