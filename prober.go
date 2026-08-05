package gor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/suraciii/gor/cluster"
	"github.com/suraciii/gor/transport"
)

type probeResponseError struct {
	message string
}

func (e probeResponseError) Error() string {
	return "probe response error: " + e.message
}

type transportProber struct {
	transport transport.Transport
}

type probeRequest struct {
	Kind string `json:"kind"`
}

var _ cluster.Prober = transportProber{}

func (p transportProber) Probe(ctx context.Context, target cluster.MemberID) <-chan cluster.ProbeResult {
	results := make(chan cluster.ProbeResult, 1)
	go func() {
		defer close(results)

		payload, err := json.Marshal(probeRequest{Kind: requestKindProbe})
		if err != nil {
			results <- cluster.ProbeResult{Err: fmt.Errorf("encode probe request: %w", err)}
			return
		}
		encodedResponse, err := p.transport.Send(ctx, target.NodeAddr, payload)
		if err != nil {
			results <- cluster.ProbeResult{Err: fmt.Errorf("send probe to %q: %w", target.NodeAddr, err)}
			return
		}

		var response callResponse
		if err := json.Unmarshal(encodedResponse, &response); err != nil {
			results <- cluster.ProbeResult{Err: fmt.Errorf("decode probe response: %w", err)}
			return
		}
		if response.Error != nil {
			results <- cluster.ProbeResult{Err: probeResponseError{message: response.Error.Message}}
			return
		}

		var id cluster.MemberID
		if err := json.Unmarshal(response.Reply, &id); err != nil {
			results <- cluster.ProbeResult{Err: fmt.Errorf("decode probe reply: %w", err)}
			return
		}
		results <- cluster.ProbeResult{ID: id}
	}()
	return results
}
