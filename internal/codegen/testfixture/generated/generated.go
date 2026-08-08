package generated

import (
	"context"
	"fmt"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/internal/codegen/testfixture/domain"
)

type accountProxy struct {
	id gor.GrainId
	rt gor.Invoker
}

type accountLookupRequest struct {
	A0 string
}
type accountLookupReply struct {
	R0 int64
	R1 string
}

func (p *accountProxy) Lookup(ctx context.Context, key string) (int64, string, error) {
	var reply accountLookupReply
	err := p.rt.Invoke(ctx, p.id, "Lookup", &accountLookupRequest{A0: key}, &reply)
	return reply.R0, reply.R1, err
}

type accountResetRequest struct{}
type accountResetReply struct{}

func (p *accountProxy) Reset(ctx context.Context) error {
	var reply accountResetReply
	err := p.rt.Invoke(ctx, p.id, "Reset", &accountResetRequest{}, &reply)
	return err
}

func dispatchAccount(ctx context.Context, instance domain.Account, method string, args any, reply any) error {
	switch method {
	case "Lookup":
		typedArgs := args.(*accountLookupRequest)
		typedReply := reply.(*accountLookupReply)
		r0, r1, err := instance.Lookup(ctx, typedArgs.A0)
		typedReply.R0 = r0
		typedReply.R1 = r1
		return err
	case "Reset":
		err := instance.Reset(ctx)
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newAccountCall(method string) (args any, reply any) {
	switch method {
	case "Lookup":
		return &accountLookupRequest{}, &accountLookupReply{}
	case "Reset":
		return &accountResetRequest{}, &accountResetReply{}
	default:
		return nil, nil
	}
}

func newAccountProxy(rt gor.Invoker, id gor.GrainId) domain.Account {
	return &accountProxy{id: id, rt: rt}
}

type ledgerProxy struct {
	id gor.GrainId
	rt gor.Invoker
}

type ledgerBalanceRequest struct{}
type ledgerBalanceReply struct {
	R0 int64
}

func (p *ledgerProxy) Balance(ctx context.Context) (int64, error) {
	var reply ledgerBalanceReply
	err := p.rt.Invoke(ctx, p.id, "Balance", &ledgerBalanceRequest{}, &reply)
	return reply.R0, err
}

func dispatchLedger(ctx context.Context, instance domain.Ledger, method string, args any, reply any) error {
	switch method {
	case "Balance":
		typedReply := reply.(*ledgerBalanceReply)
		r0, err := instance.Balance(ctx)
		typedReply.R0 = r0
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newLedgerCall(method string) (args any, reply any) {
	switch method {
	case "Balance":
		return &ledgerBalanceRequest{}, &ledgerBalanceReply{}
	default:
		return nil, nil
	}
}

func newLedgerProxy(rt gor.Invoker, id gor.GrainId) domain.Ledger {
	return &ledgerProxy{id: id, rt: rt}
}

// Install installs the generated entity bindings in rt.
// Call it once after creating rt and before registering or referencing any of
// the generated entity types. After it returns nil, gor.Register and gor.Ref
// can use those types with rt.
func Install(rt *gor.Runtime) error {
	if err := gor.InstallType[domain.Account](rt, dispatchAccount, newAccountProxy, newAccountCall); err != nil {
		return err
	}
	if err := gor.InstallType[domain.Ledger](rt, dispatchLedger, newLedgerProxy, newLedgerCall); err != nil {
		return err
	}
	return nil
}
