package generated

import (
	"context"
	"fmt"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/internal/codegen/testfixture/domain"
)

type accountProxy struct {
	id gor.Identity
	rt gor.Invoker
}

type accountLookupReply struct {
	R0 int64
	R1 string
}

func (p *accountProxy) Lookup(ctx context.Context, key string) (int64, string, error) {
	var reply accountLookupReply
	err := p.rt.Invoke(ctx, p.id, "Lookup", []any{key}, &reply)
	return reply.R0, reply.R1, err
}

func (p *accountProxy) Reset(ctx context.Context) error {
	err := p.rt.Invoke(ctx, p.id, "Reset", nil, nil)
	return err
}

func dispatchAccount(ctx context.Context, instance domain.Account, method string, args []any, reply any) error {
	switch method {
	case "Lookup":
		r0, r1, err := instance.Lookup(ctx, args[0].(string))
		typedReply := reply.(*accountLookupReply)
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

func newAccountProxy(rt gor.Invoker, id gor.Identity) domain.Account {
	return &accountProxy{id: id, rt: rt}
}

type ledgerProxy struct {
	id gor.Identity
	rt gor.Invoker
}

type ledgerBalanceReply struct {
	R0 int64
}

func (p *ledgerProxy) Balance(ctx context.Context) (int64, error) {
	var reply ledgerBalanceReply
	err := p.rt.Invoke(ctx, p.id, "Balance", nil, &reply)
	return reply.R0, err
}

func dispatchLedger(ctx context.Context, instance domain.Ledger, method string, args []any, reply any) error {
	switch method {
	case "Balance":
		r0, err := instance.Balance(ctx)
		typedReply := reply.(*ledgerBalanceReply)
		typedReply.R0 = r0
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newLedgerProxy(rt gor.Invoker, id gor.Identity) domain.Ledger {
	return &ledgerProxy{id: id, rt: rt}
}

func Install(rt *gor.Runtime) error {
	if err := gor.InstallType[domain.Account](rt, dispatchAccount, newAccountProxy); err != nil {
		return err
	}
	if err := gor.InstallType[domain.Ledger](rt, dispatchLedger, newLedgerProxy); err != nil {
		return err
	}
	return nil
}
