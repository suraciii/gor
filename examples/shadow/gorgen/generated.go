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

type deviceConfigureRequest struct {
	A0 string
}
type deviceConfigureReply struct{}

func (p *deviceProxy) Configure(ctx context.Context, configuration string) error {
	var reply deviceConfigureReply
	err := p.rt.Invoke(ctx, p.id, "Configure", &deviceConfigureRequest{A0: configuration}, &reply)
	return err
}

type deviceMarkOfflineRequest struct{}
type deviceMarkOfflineReply struct{}

func (p *deviceProxy) MarkOffline(ctx context.Context) error {
	var reply deviceMarkOfflineReply
	err := p.rt.Invoke(ctx, p.id, "MarkOffline", &deviceMarkOfflineRequest{}, &reply)
	return err
}

type deviceReportRequest struct {
	A0 string
	A1 string
}
type deviceReportReply struct{}

func (p *deviceProxy) Report(ctx context.Context, workshopID string, state string) error {
	var reply deviceReportReply
	err := p.rt.Invoke(ctx, p.id, "Report", &deviceReportRequest{A0: workshopID, A1: state}, &reply)
	return err
}

type deviceShadowRequest struct{}
type deviceShadowReply struct {
	R0 domain.Shadow
}

func (p *deviceProxy) Shadow(ctx context.Context) (domain.Shadow, error) {
	var reply deviceShadowReply
	err := p.rt.Invoke(ctx, p.id, "Shadow", &deviceShadowRequest{}, &reply)
	return reply.R0, err
}

func dispatchDevice(ctx context.Context, instance domain.Device, method string, args any, reply any) error {
	switch method {
	case "Configure":
		typedArgs := args.(*deviceConfigureRequest)
		err := instance.Configure(ctx, typedArgs.A0)
		return err
	case "MarkOffline":
		err := instance.MarkOffline(ctx)
		return err
	case "Report":
		typedArgs := args.(*deviceReportRequest)
		err := instance.Report(ctx, typedArgs.A0, typedArgs.A1)
		return err
	case "Shadow":
		typedReply := reply.(*deviceShadowReply)
		r0, err := instance.Shadow(ctx)
		typedReply.R0 = r0
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newDeviceCall(method string) (args any, reply any) {
	switch method {
	case "Configure":
		return &deviceConfigureRequest{}, &deviceConfigureReply{}
	case "MarkOffline":
		return &deviceMarkOfflineRequest{}, &deviceMarkOfflineReply{}
	case "Report":
		return &deviceReportRequest{}, &deviceReportReply{}
	case "Shadow":
		return &deviceShadowRequest{}, &deviceShadowReply{}
	default:
		return nil, nil
	}
}

func newDeviceProxy(rt gor.Invoker, id gor.Identity) domain.Device {
	return &deviceProxy{id: id, rt: rt}
}

type workshopProxy struct {
	id gor.Identity
	rt gor.Invoker
}

type workshopDeviceOfflineRequest struct {
	A0 string
}
type workshopDeviceOfflineReply struct{}

func (p *workshopProxy) DeviceOffline(ctx context.Context, deviceID string) error {
	var reply workshopDeviceOfflineReply
	err := p.rt.Invoke(ctx, p.id, "DeviceOffline", &workshopDeviceOfflineRequest{A0: deviceID}, &reply)
	return err
}

type workshopDeviceOnlineRequest struct {
	A0 string
}
type workshopDeviceOnlineReply struct{}

func (p *workshopProxy) DeviceOnline(ctx context.Context, deviceID string) error {
	var reply workshopDeviceOnlineReply
	err := p.rt.Invoke(ctx, p.id, "DeviceOnline", &workshopDeviceOnlineRequest{A0: deviceID}, &reply)
	return err
}

type workshopOnlineCountRequest struct{}
type workshopOnlineCountReply struct {
	R0 int
}

func (p *workshopProxy) OnlineCount(ctx context.Context) (int, error) {
	var reply workshopOnlineCountReply
	err := p.rt.Invoke(ctx, p.id, "OnlineCount", &workshopOnlineCountRequest{}, &reply)
	return reply.R0, err
}

func dispatchWorkshop(ctx context.Context, instance domain.Workshop, method string, args any, reply any) error {
	switch method {
	case "DeviceOffline":
		typedArgs := args.(*workshopDeviceOfflineRequest)
		err := instance.DeviceOffline(ctx, typedArgs.A0)
		return err
	case "DeviceOnline":
		typedArgs := args.(*workshopDeviceOnlineRequest)
		err := instance.DeviceOnline(ctx, typedArgs.A0)
		return err
	case "OnlineCount":
		typedReply := reply.(*workshopOnlineCountReply)
		r0, err := instance.OnlineCount(ctx)
		typedReply.R0 = r0
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newWorkshopCall(method string) (args any, reply any) {
	switch method {
	case "DeviceOffline":
		return &workshopDeviceOfflineRequest{}, &workshopDeviceOfflineReply{}
	case "DeviceOnline":
		return &workshopDeviceOnlineRequest{}, &workshopDeviceOnlineReply{}
	case "OnlineCount":
		return &workshopOnlineCountRequest{}, &workshopOnlineCountReply{}
	default:
		return nil, nil
	}
}

func newWorkshopProxy(rt gor.Invoker, id gor.Identity) domain.Workshop {
	return &workshopProxy{id: id, rt: rt}
}

// Install installs the generated entity bindings in rt.
// Call it once after creating rt and before registering or referencing any of
// the generated entity types. After it returns nil, gor.Register and gor.Ref
// can use those types with rt.
func Install(rt *gor.Runtime) error {
	if err := gor.InstallType[domain.Device](rt, dispatchDevice, newDeviceProxy, newDeviceCall); err != nil {
		return err
	}
	if err := gor.InstallType[domain.Workshop](rt, dispatchWorkshop, newWorkshopProxy, newWorkshopCall); err != nil {
		return err
	}
	return nil
}
