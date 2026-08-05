//go:build net

package transport

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
)

func TestTCPTransportBindsAddrAndRoundTrips(t *testing.T) {
	server, err := New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := New("127.0.0.1:0")
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	host, portText, err := net.SplitHostPort(server.Addr())
	if err != nil {
		t.Fatalf("Addr() = %q: %v", server.Addr(), err)
	}
	if host != "127.0.0.1" {
		t.Fatalf("Addr host = %q, want 127.0.0.1", host)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port == 0 {
		t.Fatalf("Addr port = %q, want an actual port", portText)
	}
	client.mu.Lock()
	if len(client.connections) != 0 {
		client.mu.Unlock()
		t.Fatal("client dialed before Send")
	}
	client.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(ctx, func(_ context.Context, payload []byte) ([]byte, error) {
			return append([]byte("reply:"), payload...), nil
		})
	}()

	payload, err := client.Send(ctx, server.Addr(), []byte("ping"))
	if err != nil {
		t.Fatalf("Send error = %v", err)
	}
	if string(payload) != "reply:ping" {
		t.Fatalf("Send payload = %q, want reply:ping", payload)
	}
	client.mu.Lock()
	connections := len(client.connections)
	client.mu.Unlock()
	if connections != 1 {
		t.Fatalf("client connections after Send = %d, want 1", connections)
	}

	payload, err = client.Send(ctx, server.Addr(), []byte("again"))
	if err != nil {
		t.Fatalf("second Send error = %v", err)
	}
	if string(payload) != "reply:again" {
		t.Fatalf("second Send payload = %q, want reply:again", payload)
	}
	server.mu.Lock()
	serverConnections := len(server.connections)
	server.mu.Unlock()
	if serverConnections != 1 {
		t.Fatalf("server connections after second Send = %d, want 1", serverConnections)
	}
	client.mu.Lock()
	clientConnections := len(client.connections)
	client.mu.Unlock()
	if clientConnections != 1 {
		t.Fatalf("client connections after second Send = %d, want 1", clientConnections)
	}

	cancel()
	if err := <-serveDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve error = %v, want context.Canceled", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("Close after context cancellation = %v, want nil", err)
	}
}

func TestTCPTransportCloseStopsServe(t *testing.T) {
	transport, err := New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- transport.Serve(context.Background(), func(context.Context, []byte) ([]byte, error) {
			return nil, nil
		})
	}()
	conn, err := net.Dial("tcp", transport.Addr())
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := transport.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	if err := <-serveDone; err != nil {
		t.Fatalf("Serve error after Close = %v", err)
	}
}

func TestTCPTransportRejectsSecondServe(t *testing.T) {
	server, err := New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := New("127.0.0.1:0")
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(context.Background(), func(_ context.Context, payload []byte) ([]byte, error) {
			return payload, nil
		})
	}()
	if _, err := client.Send(context.Background(), server.Addr(), []byte("start")); err != nil {
		t.Fatalf("initial Send error = %v", err)
	}

	if err := server.Serve(context.Background(), nil); !errors.Is(err, errServeAlreadyRunning) {
		t.Fatalf("second Serve error = %v, want errServeAlreadyRunning", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	if err := <-serveDone; err != nil {
		t.Fatalf("first Serve error after Close = %v", err)
	}
}

// TestTCPTransportGracefulCloseFlushesInFlightReply covers the flush
// contract on real TCP: the in-flight Send must receive the business reply,
// not a connection error, when the serving side closes gracefully while the
// reply is still pending.
func TestTCPTransportGracefulCloseFlushesInFlightReply(t *testing.T) {
	server, err := New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := New("127.0.0.1:0")
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Kill()
		_ = server.Kill()
	})

	handlerStarted := make(chan struct{})
	release := make(chan struct{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(context.Background(), func(_ context.Context, payload []byte) ([]byte, error) {
			close(handlerStarted)
			<-release
			return append([]byte("reply:"), payload...), nil
		})
	}()

	sendDone := make(chan connectionResult, 1)
	go func() {
		payload, err := client.Send(context.Background(), server.Addr(), []byte("ping"))
		sendDone <- connectionResult{payload: payload, err: err}
	}()
	<-handlerStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- server.Close()
	}()

	close(release)
	result := <-sendDone
	if result.err != nil || string(result.payload) != "reply:ping" {
		t.Fatalf("Send after graceful close = %#v, want reply:ping", result)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close error = %v", err)
	}
	if err := <-serveDone; err != nil {
		t.Fatalf("Serve error after Close = %v", err)
	}
}

// TestTCPTransportKillCancelsInFlightReply covers the abrupt counterpart on
// real TCP: a Kill on the serving side may leave the in-flight Send with a
// connection error instead of the business reply.
func TestTCPTransportKillCancelsInFlightReply(t *testing.T) {
	server, err := New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := New("127.0.0.1:0")
	if err != nil {
		_ = server.Kill()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Kill()
		_ = server.Kill()
	})

	handlerStarted := make(chan struct{})
	canceled := make(chan struct{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(context.Background(), func(ctx context.Context, _ []byte) ([]byte, error) {
			close(handlerStarted)
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		})
	}()

	sendDone := make(chan error, 1)
	go func() {
		_, err := client.Send(context.Background(), server.Addr(), []byte("ping"))
		sendDone <- err
	}()
	<-handlerStarted

	if err := server.Kill(); err != nil {
		t.Fatalf("Kill error = %v", err)
	}
	select {
	case <-canceled:
	default:
		t.Fatal("Kill did not cancel the handler context")
	}
	if err := <-sendDone; err == nil {
		t.Fatal("in-flight Send returned nil after Kill, want a connection error")
	}
	if err := <-serveDone; err != nil {
		t.Fatalf("Serve error after Kill = %v", err)
	}
}
