package transport

import (
	"context"
	"errors"
	"net"
	"sync"
)

type Handler func(context.Context, []byte) ([]byte, error)

type Transport interface {
	Send(context.Context, string, []byte) ([]byte, error)
	Serve(context.Context, Handler) error
	Addr() string
	Close() error
}

var (
	errTransportClosed     = errors.New("transport is closed")
	errServeAlreadyRunning = errors.New("transport serve already running")
)

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

func (t *TCP) Addr() string {
	return t.addr
}

func (t *TCP) Send(ctx context.Context, addr string, payload []byte) ([]byte, error) {
	conn, err := t.connectionFor(ctx, addr)
	if err != nil {
		return nil, err
	}
	return conn.Send(ctx, payload)
}

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
