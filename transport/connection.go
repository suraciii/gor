package transport

import (
	"context"
	"errors"
	"net"
	"sync"
)

type connection struct {
	conn          net.Conn
	handler       Handler
	handlerCtx    context.Context
	handlerCancel context.CancelFunc
	onDone        func(*connection)

	requests       chan *sendRequest
	responses      chan Frame
	handlerResults chan Frame
	cancels        chan *sendRequest
	writes         chan Frame
	dead           chan error
	stop           chan struct{}
	done           chan struct{}
	readerDone     chan struct{}
	writerDone     chan struct{}
	handlers       sync.WaitGroup
	err            error
}

type sendRequest struct {
	payload []byte
	id      uint64
	result  chan sendResult
}

type sendResult struct {
	payload []byte
	err     error
}

func newConnection(conn net.Conn) *connection {
	c := newConnectionState(conn, context.Background(), nil, nil)
	c.start()
	return c
}

func newConnectionState(conn net.Conn, parent context.Context, handler Handler, onDone func(*connection)) *connection {
	handlerCtx, handlerCancel := context.WithCancel(parent)
	c := &connection{
		conn:           conn,
		handler:        handler,
		handlerCtx:     handlerCtx,
		handlerCancel:  handlerCancel,
		onDone:         onDone,
		requests:       make(chan *sendRequest),
		responses:      make(chan Frame),
		handlerResults: make(chan Frame),
		cancels:        make(chan *sendRequest),
		writes:         make(chan Frame),
		dead:           make(chan error, 1),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
		readerDone:     make(chan struct{}),
		writerDone:     make(chan struct{}),
	}
	return c
}

func (c *connection) start() {
	go c.readLoop()
	go c.writeLoop()
	go c.ownerLoop()
}

func (c *connection) Send(ctx context.Context, payload []byte) ([]byte, error) {
	request := &sendRequest{
		payload: payload,
		result:  make(chan sendResult, 1),
	}
	select {
	case c.requests <- request:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.err
	}

	select {
	case result := <-request.result:
		return result.payload, result.err
	case <-ctx.Done():
		c.cancel(request)
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.err
	}
}

func (c *connection) Close() error {
	err := c.conn.Close()
	<-c.done
	return err
}

func (c *connection) cancel(request *sendRequest) {
	select {
	case c.cancels <- request:
	case <-c.stop:
	case <-c.done:
	}
}

func (c *connection) readLoop() {
	defer close(c.readerDone)
	for {
		frame, err := ReadFrame(c.conn)
		if err != nil {
			c.reportDead(err)
			return
		}
		select {
		case c.responses <- frame:
		case <-c.stop:
			return
		}
	}
}

func (c *connection) writeLoop() {
	defer close(c.writerDone)
	for {
		select {
		case frame := <-c.writes:
			if err := WriteFrame(c.conn, frame); err != nil {
				c.reportDead(err)
				return
			}
		case <-c.stop:
			return
		}
	}
}

func (c *connection) ownerLoop() {
	pending := make(map[uint64]*sendRequest)
	queue := make([]Frame, 0)
	nextID := uint64(1)
	for {
		var writeCh chan Frame
		var head Frame
		if len(queue) > 0 {
			writeCh = c.writes
			head = queue[0]
		}
		select {
		case request := <-c.requests:
			request.id = nextID
			nextID++
			pending[request.id] = request
			queue = append(queue, Frame{ID: request.id, Type: FrameRequest, Payload: request.payload})
		case writeCh <- head:
			queue = queue[1:]
		case frame := <-c.responses:
			switch frame.Type {
			case FrameRequest:
				if c.handler != nil {
					c.startHandler(frame)
				}
			case FrameResponse, FrameError:
				complete(pending, frame)
			}
		case frame := <-c.handlerResults:
			queue = append(queue, frame)
		case request := <-c.cancels:
			// Drop timed-out requests so a silent peer cannot grow pending forever.
			delete(pending, request.id)
		case err := <-c.dead:
			c.stopWith(err)
			return
		}
	}
}

func complete(pending map[uint64]*sendRequest, frame Frame) {
	request, ok := pending[frame.ID]
	if !ok {
		return
	}
	delete(pending, frame.ID)
	if frame.Type == FrameError {
		request.result <- sendResult{err: errors.New(string(frame.Payload))}
		return
	}
	request.result <- sendResult{payload: frame.Payload}
}

func (c *connection) startHandler(frame Frame) {
	c.handlers.Add(1)
	go func() {
		defer c.handlers.Done()
		payload, err := c.handler(c.handlerCtx, frame.Payload)
		response := Frame{ID: frame.ID, Type: FrameResponse, Payload: payload}
		if err != nil {
			response.Type = FrameError
			response.Payload = []byte(err.Error())
		}
		select {
		case c.handlerResults <- response:
		case <-c.stop:
		}
	}()
}

func (c *connection) reportDead(err error) {
	select {
	case c.dead <- err:
	case <-c.stop:
	default:
	}
}

func (c *connection) stopWith(err error) {
	c.err = err
	c.handlerCancel()
	close(c.stop)
	_ = c.conn.Close()
	c.handlers.Wait()
	<-c.readerDone
	<-c.writerDone
	close(c.done)
	if c.onDone != nil {
		c.onDone(c)
	}
}
