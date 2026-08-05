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
	Kind   string          `json:"kind"`
	Type   string          `json:"type"`
	Key    string          `json:"key"`
	Method string          `json:"method"`
	Args   json.RawMessage `json:"args"`
}

type callResponse struct {
	Reply json.RawMessage `json:"reply"`
	Error string          `json:"error"`
}

func (rt *Runtime) forward(ctx context.Context, owner string, id Identity, method string, args any, reply any) error {
	encodedArgs, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("encode %s arguments: %w", method, err)
	}
	payload, err := json.Marshal(callRequest{
		Kind:   requestKindInvoke,
		Type:   id.Type,
		Key:    id.Key,
		Method: method,
		Args:   encodedArgs,
	})
	if err != nil {
		return fmt.Errorf("encode invocation request: %w", err)
	}
	encodedResponse, err := rt.transport.Send(ctx, owner, payload)
	if err != nil {
		return err
	}
	var response callResponse
	if err := json.Unmarshal(encodedResponse, &response); err != nil {
		return fmt.Errorf("decode invocation response: %w", err)
	}
	if response.Error != "" {
		return errors.New(response.Error)
	}
	if reply == nil {
		return nil
	}
	if err := json.Unmarshal(response.Reply, reply); err != nil {
		return fmt.Errorf("decode %s reply: %w", method, err)
	}
	return nil
}

func (rt *Runtime) handle(ctx context.Context, payload []byte) ([]byte, error) {
	select {
	case <-rt.done:
		return encodeCallResponse(callResponse{Error: runtimepkg.ErrRuntimeClosed.Error()})
	default:
	}

	var request callRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return encodeCallResponse(callResponse{Error: fmt.Errorf("decode invocation request: %w", err).Error()})
	}

	switch request.Kind {
	case requestKindInvoke:
		return rt.handleInvoke(ctx, request)
	case requestKindProbe:
		return rt.handleProbe()
	default:
		return encodeCallResponse(callResponse{Error: fmt.Sprintf("unknown request kind %q", request.Kind)})
	}
}

const (
	requestKindInvoke = "invoke"
	requestKindProbe  = "probe"
)

func (rt *Runtime) handleInvoke(ctx context.Context, request callRequest) ([]byte, error) {
	registration, ok := rt.typeRegistration(request.Type)
	if !ok {
		return encodeCallResponse(callResponse{Error: fmt.Errorf("%w: %s", ErrTypeNotInstalled, request.Type).Error()})
	}
	args, reply := registration.newCall(request.Method)
	if args != nil {
		if err := json.Unmarshal(request.Args, args); err != nil {
			return encodeCallResponse(callResponse{Error: fmt.Errorf("decode %s arguments: %w", request.Method, err).Error()})
		}
	}

	// Forwarded requests already crossed the ownership decision; execute them locally.
	invokeErr := rt.Runtime.Invoke(ctx, runtimepkg.Identity{Type: request.Type, Key: request.Key}, request.Method, args, reply)
	response := callResponse{Error: ""}
	if invokeErr != nil {
		response.Error = invokeErr.Error()
	}
	var replyErr error
	response.Reply, replyErr = json.Marshal(reply)
	if replyErr != nil {
		response.Reply = nil
		response.Error = fmt.Errorf("encode %s reply: %w", request.Method, replyErr).Error()
	}
	return encodeCallResponse(response)
}

func (rt *Runtime) handleProbe() ([]byte, error) {
	if rt.clusterNode == nil {
		return encodeCallResponse(callResponse{Error: "cluster node is not configured"})
	}
	id, ok := rt.clusterNode.Probe()
	if !ok {
		return encodeCallResponse(callResponse{Error: cluster.ErrNodeDead.Error()})
	}
	reply, err := json.Marshal(id)
	if err != nil {
		return encodeCallResponse(callResponse{Error: fmt.Errorf("encode probe reply: %w", err).Error()})
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
