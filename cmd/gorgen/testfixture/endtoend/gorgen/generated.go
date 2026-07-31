package gorgen

import (
	"context"
	"fmt"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/cmd/gorgen/testfixture/endtoend/domain"
)

type accountProxy struct {
	id gor.Identity
	rt gor.Invoker
}

type accountDepositReply struct {
	R0 int64
}

func (p *accountProxy) Deposit(ctx context.Context, amount int64) (int64, error) {
	var reply accountDepositReply
	err := p.rt.Invoke(ctx, p.id, "Deposit", []any{amount}, &reply)
	return reply.R0, err
}

func (p *accountProxy) Reset(ctx context.Context) error {
	err := p.rt.Invoke(ctx, p.id, "Reset", nil, nil)
	return err
}

type accountSnapshotReply struct {
	R0 int64
	R1 string
}

func (p *accountProxy) Snapshot(ctx context.Context) (int64, string, error) {
	var reply accountSnapshotReply
	err := p.rt.Invoke(ctx, p.id, "Snapshot", nil, &reply)
	return reply.R0, reply.R1, err
}

func dispatchAccount(ctx context.Context, instance domain.Account, method string, args []any, reply any) error {
	switch method {
	case "Deposit":
		r0, err := instance.Deposit(ctx, args[0].(int64))
		typedReply := reply.(*accountDepositReply)
		typedReply.R0 = r0
		return err
	case "Reset":
		err := instance.Reset(ctx)
		return err
	case "Snapshot":
		r0, r1, err := instance.Snapshot(ctx)
		typedReply := reply.(*accountSnapshotReply)
		typedReply.R0 = r0
		typedReply.R1 = r1
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newAccountProxy(rt gor.Invoker, id gor.Identity) domain.Account {
	return &accountProxy{id: id, rt: rt}
}

func Install(rt *gor.Runtime) error {
	if err := gor.InstallType[domain.Account](rt, dispatchAccount, newAccountProxy); err != nil {
		return err
	}
	return nil
}
