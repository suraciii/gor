package gor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/suraciii/gor/cluster"
	runtimepkg "github.com/suraciii/gor/runtime"
)

type callRequest struct {
	Kind     string             `json:"kind"`
	Type     string             `json:"type"`
	Key      string             `json:"key"`
	Method   string             `json:"method"`
	Args     json.RawMessage    `json:"args"`
	Occupied []occupiedIdentity `json:"occupied,omitempty"`
}

// occupiedIdentity is the wire form of one entity on a forwarded call's
// occupied chain, shaped like the request's own type/key fields.
type occupiedIdentity struct {
	Type string `json:"type"`
	Key  string `json:"key"`
}

type callResponse struct {
	Reply json.RawMessage `json:"reply,omitempty"`
	Error *errorEnvelope  `json:"error,omitempty"`
}

type errorEnvelope struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
}

type remoteCodedError struct {
	code    Code
	message string
}

func (e remoteCodedError) Error() string {
	return e.message
}

func (e remoteCodedError) Code() Code {
	return e.code
}

func (e remoteCodedError) Is(target error) bool {
	code, ok := target.(Code)
	return ok && e.code == code
}

func errorEnvelopeFor(err error) *errorEnvelope {
	if err == nil {
		return nil
	}
	envelope := &errorEnvelope{Message: err.Error()}
	if code, ok := CodeOf(err); ok {
		envelope.Code = string(code)
	}
	return envelope
}

func errorFromEnvelope(envelope *errorEnvelope) error {
	if envelope == nil {
		return nil
	}
	if envelope.Code == "" {
		return errors.New(envelope.Message)
	}
	return remoteCodedError{code: Code(envelope.Code), message: envelope.Message}
}

func errorResponse(err error) ([]byte, error) {
	return encodeCallResponse(callResponse{Error: errorEnvelopeFor(publicError(err))})
}

func (rt *Runtime) forward(ctx context.Context, owner string, id Identity, method string, args any, reply any) error {
	encodedArgs, err := json.Marshal(args)
	if err != nil {
		return withCode(ErrRequestEncodeFailed, fmt.Errorf("encode %s arguments: %w", method, err))
	}
	payload, err := json.Marshal(callRequest{
		Kind:     requestKindInvoke,
		Type:     id.Type,
		Key:      id.Key,
		Method:   method,
		Args:     encodedArgs,
		Occupied: occupiedToWire(runtimepkg.OccupiedFrom(ctx)),
	})
	if err != nil {
		return withCode(ErrRequestEncodeFailed, fmt.Errorf("encode invocation request: %w", err))
	}
	encodedResponse, err := rt.transport.Send(ctx, owner, payload)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return withCode(ErrTransportFailed, err)
	}
	var response callResponse
	if err := json.Unmarshal(encodedResponse, &response); err != nil {
		return withCode(ErrTransportFailed, fmt.Errorf("decode invocation response: %w", err))
	}
	if response.Error != nil {
		return errorFromEnvelope(response.Error)
	}
	if reply == nil {
		return nil
	}
	if err := json.Unmarshal(response.Reply, reply); err != nil {
		return withCode(ErrTransportFailed, fmt.Errorf("decode %s reply: %w", method, err))
	}
	return nil
}

func (rt *Runtime) handle(ctx context.Context, payload []byte) ([]byte, error) {
	var request callRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return errorResponse(withCode(ErrInvalidRequest, fmt.Errorf("decode invocation request: %w", err)))
	}

	switch request.Kind {
	case requestKindInvoke:
		return rt.handleInvoke(ctx, request)
	case requestKindProbe:
		return rt.handleProbe()
	default:
		return errorResponse(withCode(ErrInvalidRequest, fmt.Errorf("unknown request kind %q", request.Kind)))
	}
}

const (
	requestKindInvoke = "invoke"
	requestKindProbe  = "probe"
)

func (rt *Runtime) handleInvoke(ctx context.Context, request callRequest) ([]byte, error) {
	// Admission is the boundary: a runtime that has left running rejects
	// before any request-specific work, so a stopped node reports its stop
	// status rather than a type or method error.
	release, err := rt.admit()
	if err != nil {
		return errorResponse(err)
	}
	defer release()

	registration, ok := rt.typeRegistration(request.Type)
	if !ok {
		return errorResponse(fmt.Errorf("%w: %s", ErrTypeNotInstalled, request.Type))
	}
	args, reply := registration.newCall(request.Method)
	if args == nil && reply == nil {
		return errorResponse(withCode(ErrUnknownMethod, fmt.Errorf("unknown method %q", request.Method)))
	}
	if args != nil {
		if err := json.Unmarshal(request.Args, args); err != nil {
			return errorResponse(withCode(ErrInvalidRequest, fmt.Errorf("decode %s arguments: %w", request.Method, err)))
		}
	}

	// Forwarded requests already crossed the ownership decision; execute them
	// locally without re-routing. The occupied chain crossed the wire with
	// the request; restore it so the receiving engine checks and extends it.
	if occupied := occupiedFromWire(request.Occupied); len(occupied) > 0 {
		ctx = runtimepkg.WithOccupied(ctx, occupied)
	}
	invokeErr := publicError(rt.engine.Invoke(ctx, runtimepkg.Identity{Type: request.Type, Key: request.Key}, request.Method, args, reply))
	if invokeErr != nil {
		// The handler context belongs to the serving runtime, so cancellation
		// after a stop transition is a runtime outcome rather than caller
		// cancellation.
		select {
		case <-rt.done:
			if errors.Is(invokeErr, context.Canceled) {
				invokeErr = stopRejection(rt.stopCodeSnapshot())
			}
		default:
		}
		return errorResponse(invokeErr)
	}
	encodedReply, err := json.Marshal(reply)
	if err != nil {
		return errorResponse(withCode(ErrReplyEncodeFailed, fmt.Errorf("encode %s reply: %w", request.Method, err)))
	}
	return encodeCallResponse(callResponse{Reply: encodedReply})
}

func (rt *Runtime) handleProbe() ([]byte, error) {
	if rt.clusterNode == nil {
		return errorResponse(withCode(ErrInvalidRequest, errors.New("cluster node is not configured")))
	}
	// A probe is not an entity call and does not register against the admission
	// count, but it reads the same root state and refuses to reply once the
	// runtime has left running.
	rt.lifecycleMu.Lock()
	running := rt.state == rootRunning
	code := rt.stopCode
	rt.lifecycleMu.Unlock()
	if !running {
		return errorResponse(stopRejection(code))
	}
	id, ok := rt.clusterNode.Probe()
	if !ok {
		return errorResponse(cluster.ErrNodeDead)
	}
	reply, err := json.Marshal(id)
	if err != nil {
		return errorResponse(withCode(ErrReplyEncodeFailed, fmt.Errorf("encode probe reply: %w", err)))
	}
	return encodeCallResponse(callResponse{Reply: reply})
}

func encodeCallResponse(response callResponse) ([]byte, error) {
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("encode invocation response: %w", err)
	}
	return encoded, nil
}

func occupiedToWire(chain []runtimepkg.Identity) []occupiedIdentity {
	if len(chain) == 0 {
		return nil
	}
	wire := make([]occupiedIdentity, 0, len(chain))
	for _, id := range chain {
		wire = append(wire, occupiedIdentity{Type: id.Type, Key: id.Key})
	}
	return wire
}

func occupiedFromWire(wire []occupiedIdentity) []runtimepkg.Identity {
	if len(wire) == 0 {
		return nil
	}
	chain := make([]runtimepkg.Identity, 0, len(wire))
	for _, id := range wire {
		chain = append(chain, runtimepkg.Identity{Type: id.Type, Key: id.Key})
	}
	return chain
}
