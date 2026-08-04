package gorgen

import (
	"context"
	"fmt"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/examples/shadow/domain"
)

type deviceProxy struct {
	id gor.Identity
	rt gor.Invoker
}

func (p *deviceProxy) Configure(ctx context.Context, configuration string) error {
	err := p.rt.Invoke(ctx, p.id, "Configure", []any{configuration}, nil)
	return err
}

func (p *deviceProxy) MarkOffline(ctx context.Context) error {
	err := p.rt.Invoke(ctx, p.id, "MarkOffline", nil, nil)
	return err
}

func (p *deviceProxy) Report(ctx context.Context, workshopID string, state string) error {
	err := p.rt.Invoke(ctx, p.id, "Report", []any{workshopID, state}, nil)
	return err
}

type deviceShadowReply struct {
	R0 domain.Shadow
}

func (p *deviceProxy) Shadow(ctx context.Context) (domain.Shadow, error) {
	var reply deviceShadowReply
	err := p.rt.Invoke(ctx, p.id, "Shadow", nil, &reply)
	return reply.R0, err
}

func dispatchDevice(ctx context.Context, instance domain.Device, method string, args []any, reply any) error {
	switch method {
	case "Configure":
		err := instance.Configure(ctx, args[0].(string))
		return err
	case "MarkOffline":
		err := instance.MarkOffline(ctx)
		return err
	case "Report":
		err := instance.Report(ctx, args[0].(string), args[1].(string))
		return err
	case "Shadow":
		r0, err := instance.Shadow(ctx)
		typedReply := reply.(*deviceShadowReply)
		typedReply.R0 = r0
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newDeviceProxy(rt gor.Invoker, id gor.Identity) domain.Device {
	return &deviceProxy{id: id, rt: rt}
}

type workshopProxy struct {
	id gor.Identity
	rt gor.Invoker
}

func (p *workshopProxy) DeviceOffline(ctx context.Context, deviceID string) error {
	err := p.rt.Invoke(ctx, p.id, "DeviceOffline", []any{deviceID}, nil)
	return err
}

func (p *workshopProxy) DeviceOnline(ctx context.Context, deviceID string) error {
	err := p.rt.Invoke(ctx, p.id, "DeviceOnline", []any{deviceID}, nil)
	return err
}

type workshopOnlineCountReply struct {
	R0 int
}

func (p *workshopProxy) OnlineCount(ctx context.Context) (int, error) {
	var reply workshopOnlineCountReply
	err := p.rt.Invoke(ctx, p.id, "OnlineCount", nil, &reply)
	return reply.R0, err
}

func dispatchWorkshop(ctx context.Context, instance domain.Workshop, method string, args []any, reply any) error {
	switch method {
	case "DeviceOffline":
		err := instance.DeviceOffline(ctx, args[0].(string))
		return err
	case "DeviceOnline":
		err := instance.DeviceOnline(ctx, args[0].(string))
		return err
	case "OnlineCount":
		r0, err := instance.OnlineCount(ctx)
		typedReply := reply.(*workshopOnlineCountReply)
		typedReply.R0 = r0
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newWorkshopProxy(rt gor.Invoker, id gor.Identity) domain.Workshop {
	return &workshopProxy{id: id, rt: rt}
}

func Install(rt *gor.Runtime) error {
	if err := gor.InstallType[domain.Device](rt, dispatchDevice, newDeviceProxy); err != nil {
		return err
	}
	if err := gor.InstallType[domain.Workshop](rt, dispatchWorkshop, newWorkshopProxy); err != nil {
		return err
	}
	return nil
}
