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

type accountDepositRequest struct {
	A0 int64
}
type accountDepositReply struct {
	R0 int64
}

func (p *accountProxy) Deposit(ctx context.Context, amount int64) (int64, error) {
	var reply accountDepositReply
	err := p.rt.Invoke(ctx, p.id, "Deposit", &accountDepositRequest{A0: amount}, &reply)
	return reply.R0, err
}

type accountResetRequest struct{}
type accountResetReply struct{}

func (p *accountProxy) Reset(ctx context.Context) error {
	var reply accountResetReply
	err := p.rt.Invoke(ctx, p.id, "Reset", &accountResetRequest{}, &reply)
	return err
}

type accountSnapshotRequest struct{}
type accountSnapshotReply struct {
	R0 int64
	R1 string
}

func (p *accountProxy) Snapshot(ctx context.Context) (int64, string, error) {
	var reply accountSnapshotReply
	err := p.rt.Invoke(ctx, p.id, "Snapshot", &accountSnapshotRequest{}, &reply)
	return reply.R0, reply.R1, err
}

func dispatchAccount(ctx context.Context, instance domain.Account, method string, args any, reply any) error {
	switch method {
	case "Deposit":
		typedArgs := args.(*accountDepositRequest)
		typedReply := reply.(*accountDepositReply)
		r0, err := instance.Deposit(ctx, typedArgs.A0)
		typedReply.R0 = r0
		return err
	case "Reset":
		err := instance.Reset(ctx)
		return err
	case "Snapshot":
		typedReply := reply.(*accountSnapshotReply)
		r0, r1, err := instance.Snapshot(ctx)
		typedReply.R0 = r0
		typedReply.R1 = r1
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newAccountCall(method string) (args any, reply any) {
	switch method {
	case "Deposit":
		return &accountDepositRequest{}, &accountDepositReply{}
	case "Reset":
		return &accountResetRequest{}, &accountResetReply{}
	case "Snapshot":
		return &accountSnapshotRequest{}, &accountSnapshotReply{}
	default:
		return nil, nil
	}
}

func newAccountProxy(rt gor.Invoker, id gor.Identity) domain.Account {
	return &accountProxy{id: id, rt: rt}
}

// Install installs the generated entity bindings in rt.
// Call it once after creating rt and before registering or referencing any of
// the generated entity types. After it returns nil, gor.Register and gor.Ref
// can use those types with rt.
func Install(rt *gor.Runtime) error {
	if err := gor.InstallType[domain.Account](rt, dispatchAccount, newAccountProxy, newAccountCall); err != nil {
		return err
	}
	return nil
}
