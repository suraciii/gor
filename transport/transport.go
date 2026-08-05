package transport

import (
	"context"
	"errors"
	"net"
	"sync"
)

// Handler processes one incoming request payload.
//
// The context is canceled when the serving transport or the request's
// connection is closed. How a returned error crosses the transport is defined
// by the transport implementation: TCP sends it as text, so the peer receives
// a new error with the same message and errors.Is and errors.As do not preserve
// the handler's original error. Handlers should stop promptly after ctx is
// canceled because Close waits for active handlers to return.
type Handler func(context.Context, []byte) ([]byte, error)

// Transport moves opaque request payloads to an address and serves incoming
// requests.
//
// Implementations must honor Send contexts, support concurrent calls, invoke
// handlers with cancelable contexts, and ensure Close stops serving and waits
// for active handlers and transport resources to finish. Handlers may be
// invoked concurrently for different requests and must be safe for that use.
// A canceled Send does not prove that the peer did not execute the request.
// Send error representation is transport-specific. MaxPayloadSize limits
// Frame-based wire protocols; a custom Transport implementation is not
// required to enforce it.
type Transport interface {
	// Send sends payload to addr and returns the peer's response payload.
	Send(context.Context, string, []byte) ([]byte, error)
	// Serve accepts requests until ctx is canceled, Close is called, or the
	// serving transport encounters an unrecoverable error. It may invoke handler
	// concurrently for different requests.
	Serve(context.Context, Handler) error
	// Addr returns the address on which the transport is bound.
	Addr() string
	// Close stops the transport and waits for its active work to finish.
	Close() error
}

var (
	errTransportClosed     = errors.New("transport is closed")
	errServeAlreadyRunning = errors.New("transport serve already running")
)

// TCP is a TCP implementation of Transport.
//
// It binds its listening address when created, dials destinations lazily on
// the first Send, and reuses one outgoing connection per destination until
// that connection fails.
type TCP struct {
	listener net.Listener
	addr     string

	mu           sync.Mutex
	closed       bool
	closeDone    chan struct{}
	serveStarted bool
	serveDone    chan struct{}
	outgoing     map[string]*connection
	connections  map[*connection]struct{}
}

// New binds a TCP transport to addr and returns it ready for Serve or Send.
// Use a port of zero to let the operating system choose one, then obtain the
// selected address from Addr. New returns an error if binding or initialization
// fails.
func New(addr string) (*TCP, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	serveDone := make(chan struct{})
	close(serveDone)
	return &TCP{
		listener:    listener,
		addr:        listener.Addr().String(),
		closeDone:   make(chan struct{}),
		serveDone:   serveDone,
		outgoing:    make(map[string]*connection),
		connections: make(map[*connection]struct{}),
	}, nil
}

// Addr returns the address selected when the TCP transport was created.
func (t *TCP) Addr() string {
	return t.addr
}

// Send sends payload to addr, dialing lazily and reusing an existing
// connection to that address. It honors ctx while dialing and waiting for the
// response. If ctx is canceled, the peer may still have processed the request.
// A handler error received from the peer is reconstructed from its text, so
// errors.Is and errors.As do not match the original remote error.
// Payloads larger than MaxPayloadSize fail before a request frame is written.
func (t *TCP) Send(ctx context.Context, addr string, payload []byte) ([]byte, error) {
	conn, err := t.connectionFor(ctx, addr)
	if err != nil {
		return nil, err
	}
	return conn.Send(ctx, payload)
}

// Serve accepts incoming TCP requests and invokes handler for each request.
// Handlers may run concurrently. Serve returns ctx.Err when ctx is canceled,
// returns nil when Close stops the listener, and returns an error for another
// serving failure. Only one Serve call may be active at a time.
func (t *TCP) Serve(ctx context.Context, handler Handler) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return errTransportClosed
	}
	if t.serveStarted {
		t.mu.Unlock()
		return errServeAlreadyRunning
	}
	t.serveStarted = true
	t.serveDone = make(chan struct{})
	listener := t.listener
	serveDone := t.serveDone
	t.mu.Unlock()

	defer close(serveDone)
	defer t.closeConnections()
	wakeDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-wakeDone:
		}
	}()
	defer close(wakeDone)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			t.mu.Lock()
			closed := t.closed
			t.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		t.addAccepted(conn, ctx, handler)
	}
}

// Close stops accepting connections, cancels active handlers, and waits for
// handlers and connection loops to finish. It is safe to call more than once.
func (t *TCP) Close() error {
	t.mu.Lock()
	if t.closed {
		closeDone := t.closeDone
		t.mu.Unlock()
		<-closeDone
		return nil
	}
	t.closed = true
	listener := t.listener
	serveDone := t.serveDone
	serveStarted := t.serveStarted
	t.mu.Unlock()

	err := listener.Close()
	t.closeConnections()
	if serveStarted {
		<-serveDone
	}
	close(t.closeDone)
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (t *TCP) connectionFor(ctx context.Context, addr string) (*connection, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, errTransportClosed
	}
	if conn, ok := t.outgoing[addr]; ok {
		select {
		case <-conn.done:
			delete(t.outgoing, addr)
		default:
			t.mu.Unlock()
			return conn, nil
		}
	}
	t.mu.Unlock()

	dialer := net.Dialer{}
	raw, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = raw.Close()
		return nil, errTransportClosed
	}
	if conn, ok := t.outgoing[addr]; ok {
		select {
		case <-conn.done:
			delete(t.outgoing, addr)
		default:
			t.mu.Unlock()
			_ = raw.Close()
			return conn, nil
		}
	}
	conn := newConnectionState(raw, context.Background(), nil, t.removeConnection)
	t.outgoing[addr] = conn
	t.connections[conn] = struct{}{}
	conn.start()
	t.mu.Unlock()
	return conn, nil
}

func (t *TCP) addAccepted(raw net.Conn, parent context.Context, handler Handler) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = raw.Close()
		return
	}
	conn := newConnectionState(raw, parent, handler, t.removeConnection)
	t.connections[conn] = struct{}{}
	conn.start()
	t.mu.Unlock()
}

func (t *TCP) removeConnection(conn *connection) {
	t.mu.Lock()
	delete(t.connections, conn)
	for addr, current := range t.outgoing {
		if current == conn {
			delete(t.outgoing, addr)
		}
	}
	t.mu.Unlock()
}

func (t *TCP) closeConnections() {
	t.mu.Lock()
	connections := make([]*connection, 0, len(t.connections))
	for conn := range t.connections {
		connections = append(connections, conn)
	}
	t.mu.Unlock()

	for _, conn := range connections {
		_ = conn.conn.Close()
	}
	for _, conn := range connections {
		<-conn.done
	}
}
