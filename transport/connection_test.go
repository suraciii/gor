package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"testing/synctest"
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

func TestConnectionDiesOnMalformedFrame(t *testing.T) {
	for _, test := range []struct {
		name   string
		length uint32
		typ    byte
		want   error
	}{
		{name: "payload too large", length: ^uint32(0), typ: byte(FrameResponse), want: errPayloadTooLarge},
		{name: "invalid type", length: 0, typ: 99, want: errInvalidFrameType},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, server := net.Pipe()
			conn := newConnection(client)
			t.Cleanup(func() {
				_ = server.Close()
				_ = conn.Close()
			})

			result := make(chan error, 1)
			go func() {
				_, err := conn.Send(context.Background(), []byte("request"))
				result <- err
			}()
			request, err := ReadFrame(server)
			if err != nil {
				t.Fatal(err)
			}

			header := make([]byte, frameHeaderSize)
			binary.BigEndian.PutUint32(header[:4], test.length)
			binary.BigEndian.PutUint64(header[4:12], request.ID)
			header[12] = test.typ
			if _, err := server.Write(header); err != nil {
				t.Fatal(err)
			}
			if err := <-result; !errors.Is(err, test.want) {
				t.Fatalf("Send error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestConnectionHandlersDoNotBlockEachOther(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		conn := newConnection(client)
		release := make(chan struct{})
		released := false
		slowStarted := make(chan struct{})
		fastStarted := make(chan struct{})
		srv := newConnectionState(server, context.Background(), func(_ context.Context, payload []byte) ([]byte, error) {
			if string(payload) == "slow" {
				close(slowStarted)
				<-release
			} else {
				close(fastStarted)
			}
			return payload, nil
		}, nil)
		srv.start()
		t.Cleanup(func() {
			if !released {
				close(release)
			}
			_ = conn.Close()
			_ = srv.Close()
		})

		slowDone := make(chan error, 1)
		go func() {
			_, err := conn.Send(context.Background(), []byte("slow"))
			slowDone <- err
		}()
		synctest.Wait()
		<-slowStarted

		fastDone := make(chan connectionResult, 1)
		go func() {
			payload, err := conn.Send(context.Background(), []byte("fast"))
			fastDone <- connectionResult{payload: payload, err: err}
		}()
		synctest.Wait()
		select {
		case <-fastStarted:
		default:
			t.Fatal("fast handler was blocked behind slow handler")
		}
		synctest.Wait()
		if result := <-fastDone; result.err != nil || string(result.payload) != "fast" {
			t.Fatalf("fast result = %#v, want payload fast", result)
		}

		close(release)
		released = true
		synctest.Wait()
		if err := <-slowDone; err != nil {
			t.Fatalf("slow Send error = %v", err)
		}
	})
}

func TestConnectionCloseCancelsAndWaitsForHandlers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		handlerStarted := make(chan struct{})
		handlerCanceled := make(chan struct{})
		releaseHandler := make(chan struct{})
		handlerExited := make(chan struct{})
		released := false
		srv := newConnectionState(server, context.Background(), func(ctx context.Context, _ []byte) ([]byte, error) {
			close(handlerStarted)
			<-ctx.Done()
			close(handlerCanceled)
			<-releaseHandler
			close(handlerExited)
			return nil, ctx.Err()
		}, nil)
		srv.start()
		t.Cleanup(func() {
			if !released {
				close(releaseHandler)
			}
			srv.handlerCancel()
			_ = srv.Close()
			_ = client.Close()
		})

		writeDone := make(chan error, 1)
		go func() {
			writeDone <- WriteFrame(client, Frame{ID: 1, Type: FrameRequest, Payload: []byte("request")})
		}()
		synctest.Wait()
		if err := <-writeDone; err != nil {
			t.Fatalf("request write error = %v", err)
		}
		<-handlerStarted

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- srv.Close()
		}()
		synctest.Wait()
		select {
		case <-handlerCanceled:
		default:
			t.Fatal("Close did not cancel the handler context")
		}
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before handler exited: %v", err)
		default:
		}

		close(releaseHandler)
		released = true
		synctest.Wait()
		if err := <-closeDone; err != nil {
			t.Fatalf("Close error = %v", err)
		}
		select {
		case <-handlerExited:
		default:
			t.Fatal("handler was still running after Close returned")
		}
	})
}

func TestConnectionReturnsHandlerErrorAsText(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(client)
	srv := newConnectionState(server, context.Background(), func(context.Context, []byte) ([]byte, error) {
		return nil, errors.New("handler failed")
	}, nil)
	srv.start()
	t.Cleanup(func() {
		_ = conn.Close()
		_ = srv.Close()
	})

	_, err := conn.Send(context.Background(), []byte("request"))
	if err == nil || err.Error() != "handler failed" {
		t.Fatalf("Send error = %v, want handler failed", err)
	}
}
