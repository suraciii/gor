package transport

import (
	"context"
	"net"
	"testing"
	"testing/synctest"
)

// pipeListener accepts connections handed to it by the test and returns
// net.ErrClosed after Close, so a TCP transport can be exercised in a
// synctest bubble over net.Pipe without real networking.
type pipeListener struct {
	conns chan net.Conn
	done  chan struct{}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	close(l.done)
	return nil
}

func (l *pipeListener) Addr() net.Addr {
	return pipeAddr{}
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// newPipeTCP builds a TCP transport whose accept loop serves net.Pipe
// connections handed to it by the test.
func newPipeTCP() (*TCP, net.Conn) {
	server, peer := net.Pipe()
	listener := &pipeListener{conns: make(chan net.Conn, 1), done: make(chan struct{})}
	listener.conns <- server
	tcp := &TCP{
		listener:    listener,
		addr:        "pipe",
		closeDone:   make(chan struct{}),
		serveDone:   make(chan struct{}),
		outgoing:    make(map[string]*connection),
		connections: make(map[*connection]struct{}),
	}
	return tcp, peer
}

// TestTCPGracefulCloseFlushesReplyAndDoesNotCancelHandler pins the TCP-level
// routing of a graceful stop: TCP.Close must route to the graceful connection
// close, which flushes the in-flight reply before closing the socket and
// never cancels the handler context. A misrouted Close that kills the
// connections cancels the handler while it is still blocked, so the reply is
// lost and the caller never sees it — red under any schedule.
func TestTCPGracefulCloseFlushesReplyAndDoesNotCancelHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tcp, peer := newPipeTCP()
		handlerStarted := make(chan struct{})
		release := make(chan struct{})
		canceled := make(chan struct{})
		serveDone := make(chan error, 1)
		go func() {
			serveDone <- tcp.Serve(context.Background(), func(ctx context.Context, payload []byte) ([]byte, error) {
				close(handlerStarted)
				select {
				case <-release:
				case <-ctx.Done():
					close(canceled)
					return nil, ctx.Err()
				}
				return append([]byte("reply:"), payload...), nil
			})
		}()
		synctest.Wait()

		writeDone := make(chan error, 1)
		go func() {
			writeDone <- WriteFrame(peer, Frame{ID: 1, Type: FrameRequest, Payload: []byte("request")})
		}()
		synctest.Wait()
		if err := <-writeDone; err != nil {
			t.Fatalf("request write error = %v", err)
		}
		<-handlerStarted

		replyDone := make(chan Frame, 1)
		go func() {
			frame, err := ReadFrame(peer)
			if err != nil {
				replyDone <- Frame{ID: 1, Type: FrameResponse, Payload: []byte(err.Error())}
				return
			}
			replyDone <- frame
		}()

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- tcp.Close()
		}()
		synctest.Wait()
		// The reply is still pending: a graceful close must not complete, and
		// it must not cancel the handler.
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before the reply was flushed: %v", err)
		default:
		}
		select {
		case <-canceled:
			t.Fatal("graceful Close canceled the handler context")
		default:
		}

		close(release)
		synctest.Wait()
		frame := <-replyDone
		if frame.Type != FrameResponse || string(frame.Payload) != "reply:request" {
			t.Fatalf("flushed reply = %#v, want reply:request", frame)
		}
		synctest.Wait()
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Close error = %v", err)
			}
		default:
			t.Fatal("Close did not return after the reply was flushed")
		}
		select {
		case err := <-serveDone:
			if err != nil {
				t.Fatalf("Serve error = %v", err)
			}
		default:
			t.Fatal("Serve did not return after Close")
		}
		_ = peer.Close()
	})
}

// TestTCPKillCancelsHandlerAndReturnsWithoutWaiting pins the TCP-level
// routing of an abrupt stop: TCP.Kill must route to the abrupt connection
// close, which cancels the blocked handler and returns without waiting for
// it. A misrouted Kill that gracefully closes the connections waits for the
// handler and never returns.
func TestTCPKillCancelsHandlerAndReturnsWithoutWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tcp, peer := newPipeTCP()
		handlerStarted := make(chan struct{})
		release := make(chan struct{})
		released := false
		canceled := make(chan struct{})
		serveDone := make(chan error, 1)
		go func() {
			serveDone <- tcp.Serve(context.Background(), func(ctx context.Context, _ []byte) ([]byte, error) {
				close(handlerStarted)
				<-ctx.Done()
				close(canceled)
				<-release
				return nil, ctx.Err()
			})
		}()
		synctest.Wait()
		t.Cleanup(func() {
			if !released {
				close(release)
			}
			_ = peer.Close()
			_ = tcp.Kill()
			_ = tcp.Close()
		})

		writeDone := make(chan error, 1)
		go func() {
			writeDone <- WriteFrame(peer, Frame{ID: 1, Type: FrameRequest, Payload: []byte("request")})
		}()
		synctest.Wait()
		if err := <-writeDone; err != nil {
			t.Fatalf("request write error = %v", err)
		}
		<-handlerStarted

		killDone := make(chan error, 1)
		go func() {
			killDone <- tcp.Kill()
		}()
		synctest.Wait()
		// Kill is the escape hatch: it must return while the handler is still
		// blocked, and it must cancel the handler context.
		select {
		case err := <-killDone:
			if err != nil {
				t.Fatalf("Kill error = %v", err)
			}
		default:
			t.Fatal("Kill did not return while the handler was blocked")
		}
		select {
		case <-canceled:
		default:
			t.Fatal("Kill did not cancel the handler context")
		}
		select {
		case err := <-serveDone:
			if err != nil {
				t.Fatalf("Serve error = %v", err)
			}
		default:
			t.Fatal("Serve did not return after Kill")
		}

		close(release)
		released = true
		synctest.Wait()
	})
}
