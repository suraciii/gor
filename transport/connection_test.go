package transport

import (
	"context"
	"errors"
	"net"
	"testing"
)

type connectionResult struct {
	request string
	payload []byte
	err     error
}

func TestConnectionMatchesOutOfOrderResponses(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(client)
	serverDone := make(chan error, 1)
	serverFinished := false
	t.Cleanup(func() {
		_ = server.Close()
		_ = conn.Close()
		if !serverFinished {
			<-serverDone
		}
	})

	go func() {
		frames := make([]Frame, 2)
		for i := range frames {
			frame, err := ReadFrame(server)
			if err != nil {
				serverDone <- err
				return
			}
			frames[i] = frame
		}
		for i := len(frames) - 1; i >= 0; i-- {
			frame := frames[i]
			frame.Type = FrameResponse
			frame.Payload = append([]byte("response:"), frame.Payload...)
			if err := WriteFrame(server, frame); err != nil {
				serverDone <- err
				return
			}
		}
		serverDone <- nil
	}()

	results := make(chan connectionResult, 2)
	for _, request := range []string{"one", "two"} {
		request := request
		go func() {
			payload, err := conn.Send(context.Background(), []byte(request))
			results <- connectionResult{request: request, payload: payload, err: err}
		}()
	}

	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("Send(%q) error = %v", result.request, result.err)
		}
		want := []byte("response:" + result.request)
		if string(result.payload) != string(want) {
			t.Fatalf("Send(%q) payload = %q, want %q", result.request, result.payload, want)
		}
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	serverFinished = true
}

func TestConnectionDropsResponseAfterCancellation(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(client)
	serverDone := make(chan error, 1)
	serverFinished := false
	t.Cleanup(func() {
		_ = server.Close()
		_ = conn.Close()
		if !serverFinished {
			<-serverDone
		}
	})

	firstFrame := make(chan Frame, 1)
	go func() {
		first, err := ReadFrame(server)
		if err != nil {
			serverDone <- err
			return
		}
		firstFrame <- first
		second, err := ReadFrame(server)
		if err != nil {
			serverDone <- err
			return
		}
		late := Frame{ID: first.ID, Type: FrameResponse, Payload: []byte("late")}
		if err := WriteFrame(server, late); err != nil {
			serverDone <- err
			return
		}
		if err := WriteFrame(server, Frame{ID: second.ID, Type: FrameResponse, Payload: []byte("second")}); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan connectionResult, 1)
	go func() {
		_, err := conn.Send(ctx, []byte("first"))
		firstDone <- connectionResult{request: "first", err: err}
	}()
	first := <-firstFrame
	cancel()
	if result := <-firstDone; !errors.Is(result.err, context.Canceled) {
		t.Fatalf("timed-out Send error = %v, want context.Canceled", result.err)
	}

	secondDone := make(chan connectionResult, 1)
	go func() {
		payload, err := conn.Send(context.Background(), []byte("second"))
		secondDone <- connectionResult{request: "second", payload: payload, err: err}
	}()
	result := <-secondDone
	if result.err != nil {
		t.Fatalf("second Send error = %v", result.err)
	}
	if string(result.payload) != "second" {
		t.Fatalf("second Send payload = %q, want second", result.payload)
	}
	if first.ID == 0 {
		t.Fatal("first request did not receive a correlation id")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	serverFinished = true
}

func TestConnectionFailsAllPendingRequestsWhenConnectionDies(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(client)
	serverDone := make(chan error, 1)
	serverFinished := false
	t.Cleanup(func() {
		_ = server.Close()
		_ = conn.Close()
		if !serverFinished {
			<-serverDone
		}
	})

	ready := make(chan struct{})
	go func() {
		for range 2 {
			if _, err := ReadFrame(server); err != nil {
				serverDone <- err
				return
			}
		}
		close(ready)
		_ = server.Close()
		serverDone <- nil
	}()

	results := make(chan error, 2)
	for _, payload := range []string{"one", "two"} {
		go func(payload string) {
			_, err := conn.Send(context.Background(), []byte(payload))
			results <- err
		}(payload)
	}
	<-ready
	for range 2 {
		if err := <-results; err == nil {
			t.Fatal("pending Send returned nil after connection death")
		}
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	serverFinished = true
}

func TestConnectionDropsUnknownResponseID(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(client)
	serverDone := make(chan error, 1)
	serverFinished := false
	t.Cleanup(func() {
		_ = server.Close()
		_ = conn.Close()
		if !serverFinished {
			<-serverDone
		}
	})

	go func() {
		request, err := ReadFrame(server)
		if err != nil {
			serverDone <- err
			return
		}
		if err := WriteFrame(server, Frame{ID: request.ID + 100, Type: FrameResponse, Payload: []byte("unknown")}); err != nil {
			serverDone <- err
			return
		}
		if err := WriteFrame(server, Frame{ID: request.ID, Type: FrameResponse, Payload: []byte("valid")}); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	payload, err := conn.Send(context.Background(), []byte("request"))
	if err != nil {
		t.Fatalf("Send error = %v", err)
	}
	if string(payload) != "valid" {
		t.Fatalf("Send payload = %q, want valid", payload)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	serverFinished = true
}

func TestConnectionCloseWaitsForAllLoops(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(client)

	if err := conn.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	_ = server.Close()
	for name, done := range map[string]<-chan struct{}{
		"reader": conn.readerDone,
		"writer": conn.writerDone,
		"owner":  conn.done,
	} {
		select {
		case <-done:
		default:
			t.Errorf("%s loop is still running", name)
		}
	}
}
