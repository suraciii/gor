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

func TestConnectionGracefulCloseFlushesInFlightReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		conn := newConnection(client)
		handlerStarted := make(chan struct{})
		release := make(chan struct{})
		released := false
		srv := newConnectionState(server, context.Background(), func(_ context.Context, payload []byte) ([]byte, error) {
			close(handlerStarted)
			<-release
			return append([]byte("reply:"), payload...), nil
		}, nil)
		srv.start()
		t.Cleanup(func() {
			if !released {
				close(release)
			}
			_ = conn.Close()
			_ = srv.Close()
		})

		sendDone := make(chan connectionResult, 1)
		go func() {
			payload, err := conn.Send(context.Background(), []byte("request"))
			sendDone <- connectionResult{payload: payload, err: err}
		}()
		synctest.Wait()
		<-handlerStarted

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- srv.Close()
		}()
		synctest.Wait()
		// The handler still holds the reply: a graceful Close must not
		// complete while it owes the peer a reply. After synctest.Wait every
		// goroutine in the bubble is durably blocked, so a Close that would
		// have returned has already returned.
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before the in-flight reply was flushed: %v", err)
		default:
		}
		select {
		case result := <-sendDone:
			t.Fatalf("Send returned before the handler was released: %#v", result)
		default:
		}

		close(release)
		released = true
		synctest.Wait()
		result := <-sendDone
		if result.err != nil || string(result.payload) != "reply:request" {
			t.Fatalf("Send after graceful close = %#v, want reply:request", result)
		}
		// The reply reached the caller; the bubble is quiescent, so a Close
		// that would have returned has already returned.
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Close error = %v", err)
			}
		default:
			t.Fatal("Close did not return after the reply was flushed")
		}
	})
}

func TestConnectionGracefulCloseWaitsForQueuedReplies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		conn := newConnection(client)
		started := make(chan struct{}, 2)
		release := make(chan struct{})
		released := false
		srv := newConnectionState(server, context.Background(), func(_ context.Context, payload []byte) ([]byte, error) {
			started <- struct{}{}
			<-release
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

		replies := make(chan connectionResult, 2)
		for _, request := range []string{"one", "two"} {
			request := request
			go func() {
				payload, err := conn.Send(context.Background(), []byte(request))
				replies <- connectionResult{request: request, payload: payload, err: err}
			}()
		}
		synctest.Wait()
		<-started
		<-started

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- srv.Close()
		}()
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned while replies were still pending: %v", err)
		default:
		}

		close(release)
		released = true
		synctest.Wait()
		for range 2 {
			result := <-replies
			if result.err != nil || string(result.payload) != result.request {
				t.Fatalf("Send(%q) after graceful close = %#v", result.request, result)
			}
		}
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Close error = %v", err)
			}
		default:
			t.Fatal("Close did not return after all replies were flushed")
		}
	})
}

func TestConnectionKillCancelsHandlersAndDoesNotWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		conn := newConnection(client)
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
			_ = srv.Kill()
			_ = conn.Close()
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

		killDone := make(chan error, 1)
		go func() {
			killDone <- srv.Kill()
		}()
		synctest.Wait()
		// Kill is the escape hatch for a handler that will not finish: it must
		// return without waiting for the blocked handler.
		select {
		case err := <-killDone:
			if err != nil {
				t.Fatalf("Kill error = %v", err)
			}
		default:
			t.Fatal("Kill did not return while the handler was still blocked")
		}
		select {
		case <-handlerCanceled:
		default:
			t.Fatal("Kill did not cancel the handler context")
		}

		close(releaseHandler)
		released = true
		synctest.Wait()
		select {
		case <-handlerExited:
		default:
			t.Fatal("handler was still running after Kill")
		}
	})
}

func TestConnectionKillEscalatesGracefulClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		conn := newConnection(client)
		handlerStarted := make(chan struct{})
		release := make(chan struct{})
		released := false
		srv := newConnectionState(server, context.Background(), func(_ context.Context, payload []byte) ([]byte, error) {
			close(handlerStarted)
			<-release
			return payload, nil
		}, nil)
		srv.start()
		t.Cleanup(func() {
			if !released {
				close(release)
			}
			_ = conn.Close()
			_ = srv.Kill()
		})

		sendDone := make(chan error, 1)
		go func() {
			_, err := conn.Send(context.Background(), []byte("request"))
			sendDone <- err
		}()
		synctest.Wait()
		<-handlerStarted

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- srv.Close()
		}()
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned while the reply was still pending: %v", err)
		default:
		}

		// Kill during Close escalates: the blocked handler is canceled, the
		// pending reply is dropped, and both stops conclude.
		killDone := make(chan error, 1)
		go func() {
			killDone <- srv.Kill()
		}()
		synctest.Wait()
		select {
		case err := <-killDone:
			if err != nil {
				t.Fatalf("Kill error = %v", err)
			}
		default:
			t.Fatal("Kill did not return during the graceful close")
		}
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Close error after escalation = %v", err)
			}
		default:
			t.Fatal("Close did not conclude after Kill escalated it")
		}
		select {
		case err := <-sendDone:
			if err == nil {
				t.Fatal("pending Send returned nil after Kill escalated the close")
			}
		default:
			t.Fatal("pending Send did not fail after Kill escalated the close")
		}

		close(release)
		released = true
		synctest.Wait()
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
