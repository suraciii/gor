package gorgen

import (
	"context"
	"fmt"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/examples/shadow/domain"
)

type deviceProxy struct {
	id gor.GrainId
	rt gor.Invoker
}

type deviceApplyPendingRequest struct {
	A0 string
}
type deviceApplyPendingReply struct{}

func (p *deviceProxy) ApplyPending(ctx context.Context, actionID string) error {
	var reply deviceApplyPendingReply
	err := p.rt.Invoke(ctx, p.id, "ApplyPending", &deviceApplyPendingRequest{A0: actionID}, &reply)
	return err
}

type deviceClearShadowRequest struct{}
type deviceClearShadowReply struct{}

func (p *deviceProxy) ClearShadow(ctx context.Context) error {
	var reply deviceClearShadowReply
	err := p.rt.Invoke(ctx, p.id, "ClearShadow", &deviceClearShadowRequest{}, &reply)
	return err
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

type deviceMarkOfflineRequest struct {
	A0 gor.TickStatus
}
type deviceMarkOfflineReply struct{}

func (p *deviceProxy) MarkOffline(ctx context.Context, tick gor.TickStatus) error {
	var reply deviceMarkOfflineReply
	err := p.rt.Invoke(ctx, p.id, "MarkOffline", &deviceMarkOfflineRequest{A0: tick}, &reply)
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

type deviceReportActionRequest struct {
	A0 string
	A1 string
}
type deviceReportActionReply struct{}

func (p *deviceProxy) ReportAction(ctx context.Context, actionID string, state string) error {
	var reply deviceReportActionReply
	err := p.rt.Invoke(ctx, p.id, "ReportAction", &deviceReportActionRequest{A0: actionID, A1: state}, &reply)
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

type deviceShadowExistsRequest struct{}
type deviceShadowExistsReply struct {
	R0 bool
}

func (p *deviceProxy) ShadowExists(ctx context.Context) (bool, error) {
	var reply deviceShadowExistsReply
	err := p.rt.Invoke(ctx, p.id, "ShadowExists", &deviceShadowExistsRequest{}, &reply)
	return reply.R0, err
}

func dispatchDevice(ctx context.Context, instance domain.Device, method string, args any, reply any) error {
	switch method {
	case "ApplyPending":
		typedArgs := args.(*deviceApplyPendingRequest)
		err := instance.ApplyPending(ctx, typedArgs.A0)
		return err
	case "ClearShadow":
		err := instance.ClearShadow(ctx)
		return err
	case "Configure":
		typedArgs := args.(*deviceConfigureRequest)
		err := instance.Configure(ctx, typedArgs.A0)
		return err
	case "MarkOffline":
		typedArgs := args.(*deviceMarkOfflineRequest)
		err := instance.MarkOffline(ctx, typedArgs.A0)
		return err
	case "Report":
		typedArgs := args.(*deviceReportRequest)
		err := instance.Report(ctx, typedArgs.A0, typedArgs.A1)
		return err
	case "ReportAction":
		typedArgs := args.(*deviceReportActionRequest)
		err := instance.ReportAction(ctx, typedArgs.A0, typedArgs.A1)
		return err
	case "Shadow":
		typedReply := reply.(*deviceShadowReply)
		r0, err := instance.Shadow(ctx)
		typedReply.R0 = r0
		return err
	case "ShadowExists":
		typedReply := reply.(*deviceShadowExistsReply)
		r0, err := instance.ShadowExists(ctx)
		typedReply.R0 = r0
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newDeviceCall(method string) (args any, reply any) {
	switch method {
	case "ApplyPending":
		return &deviceApplyPendingRequest{}, &deviceApplyPendingReply{}
	case "ClearShadow":
		return &deviceClearShadowRequest{}, &deviceClearShadowReply{}
	case "Configure":
		return &deviceConfigureRequest{}, &deviceConfigureReply{}
	case "MarkOffline":
		return &deviceMarkOfflineRequest{}, &deviceMarkOfflineReply{}
	case "Report":
		return &deviceReportRequest{}, &deviceReportReply{}
	case "ReportAction":
		return &deviceReportActionRequest{}, &deviceReportActionReply{}
	case "Shadow":
		return &deviceShadowRequest{}, &deviceShadowReply{}
	case "ShadowExists":
		return &deviceShadowExistsRequest{}, &deviceShadowExistsReply{}
	default:
		return nil, nil
	}
}

func newDeviceReminderCall(method string, status gor.TickStatus) (args any, reply any) {
	switch method {
	case "MarkOffline":
		return &deviceMarkOfflineRequest{A0: status}, &deviceMarkOfflineReply{}
	default:
		return nil, nil
	}
}

func newDeviceProxy(rt gor.Invoker, id gor.GrainId) domain.Device {
	return &deviceProxy{id: id, rt: rt}
}

type recoveryCoordinatorProxy struct {
	id gor.GrainId
	rt gor.Invoker
}

type recoveryCoordinatorRecoverRequest struct {
	A0 gor.TickStatus
}
type recoveryCoordinatorRecoverReply struct{}

func (p *recoveryCoordinatorProxy) Recover(ctx context.Context, tick gor.TickStatus) error {
	var reply recoveryCoordinatorRecoverReply
	err := p.rt.Invoke(ctx, p.id, "Recover", &recoveryCoordinatorRecoverRequest{A0: tick}, &reply)
	return err
}

type recoveryCoordinatorStartRequest struct{}
type recoveryCoordinatorStartReply struct{}

func (p *recoveryCoordinatorProxy) Start(ctx context.Context) error {
	var reply recoveryCoordinatorStartReply
	err := p.rt.Invoke(ctx, p.id, "Start", &recoveryCoordinatorStartRequest{}, &reply)
	return err
}

type recoveryCoordinatorStopRequest struct{}
type recoveryCoordinatorStopReply struct{}

func (p *recoveryCoordinatorProxy) Stop(ctx context.Context) error {
	var reply recoveryCoordinatorStopReply
	err := p.rt.Invoke(ctx, p.id, "Stop", &recoveryCoordinatorStopRequest{}, &reply)
	return err
}

func dispatchRecoveryCoordinator(ctx context.Context, instance domain.RecoveryCoordinator, method string, args any, reply any) error {
	switch method {
	case "Recover":
		typedArgs := args.(*recoveryCoordinatorRecoverRequest)
		err := instance.Recover(ctx, typedArgs.A0)
		return err
	case "Start":
		err := instance.Start(ctx)
		return err
	case "Stop":
		err := instance.Stop(ctx)
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newRecoveryCoordinatorCall(method string) (args any, reply any) {
	switch method {
	case "Recover":
		return &recoveryCoordinatorRecoverRequest{}, &recoveryCoordinatorRecoverReply{}
	case "Start":
		return &recoveryCoordinatorStartRequest{}, &recoveryCoordinatorStartReply{}
	case "Stop":
		return &recoveryCoordinatorStopRequest{}, &recoveryCoordinatorStopReply{}
	default:
		return nil, nil
	}
}

func newRecoveryCoordinatorReminderCall(method string, status gor.TickStatus) (args any, reply any) {
	switch method {
	case "Recover":
		return &recoveryCoordinatorRecoverRequest{A0: status}, &recoveryCoordinatorRecoverReply{}
	default:
		return nil, nil
	}
}

func newRecoveryCoordinatorProxy(rt gor.Invoker, id gor.GrainId) domain.RecoveryCoordinator {
	return &recoveryCoordinatorProxy{id: id, rt: rt}
}

type workshopProxy struct {
	id gor.GrainId
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

func newWorkshopReminderCall(method string, status gor.TickStatus) (args any, reply any) {
	switch method {
	default:
		return nil, nil
	}
}

func newWorkshopProxy(rt gor.Invoker, id gor.GrainId) domain.Workshop {
	return &workshopProxy{id: id, rt: rt}
}

// Install installs the generated Grain bindings in rt.
// Call it once after creating rt and before registering or referencing any of
// the generated Grain types. After it returns nil, gor.Register and gor.Ref
// can use those types with rt.
func Install(rt *gor.Runtime) error {
	if err := gor.InstallType[domain.Device](rt, dispatchDevice, newDeviceProxy, newDeviceCall, newDeviceReminderCall); err != nil {
		return err
	}
	if err := gor.InstallType[domain.RecoveryCoordinator](rt, dispatchRecoveryCoordinator, newRecoveryCoordinatorProxy, newRecoveryCoordinatorCall, newRecoveryCoordinatorReminderCall); err != nil {
		return err
	}
	if err := gor.InstallType[domain.Workshop](rt, dispatchWorkshop, newWorkshopProxy, newWorkshopCall, newWorkshopReminderCall); err != nil {
		return err
	}
	return nil
}
