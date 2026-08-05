package transport

import (
	"context"
	"errors"
	"net"
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
	closeReq       chan struct{}
	killReq        chan struct{}
	done           chan struct{}
	readerDone     chan struct{}
	writerDone     chan struct{}
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
		closeReq:       make(chan struct{}),
		killReq:        make(chan struct{}),
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

// Close stops accepting new requests, lets in-flight handlers finish, flushes
// their replies to the wire, and only then closes the socket. It may not
// complete while the connection still owes the peer a reply; a dead wire
// fails the flush fast and the close proceeds. A concurrent Kill interrupts
// the flush.
func (c *connection) Close() error {
	select {
	case c.closeReq <- struct{}{}:
	case <-c.done:
		return nil
	}
	<-c.done
	return nil
}

// Kill cancels in-flight handlers, drops replies not yet written, and closes
// the socket. It does not wait for handlers that ignore their canceled
// contexts, and it escalates a Close that is still in progress.
func (c *connection) Kill() error {
	select {
	case c.killReq <- struct{}{}:
	case <-c.done:
		return nil
	}
	<-c.done
	return nil
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
	inflight := 0
	closing := false
	for {
		var requestsCh chan *sendRequest
		if !closing {
			requestsCh = c.requests
		}
		var writeCh chan Frame
		var head Frame
		if len(queue) > 0 {
			writeCh = c.writes
			head = queue[0]
		}
		select {
		case request := <-requestsCh:
			request.id = nextID
			nextID++
			pending[request.id] = request
			queue = append(queue, Frame{ID: request.id, Type: FrameRequest, Payload: request.payload})
		case writeCh <- head:
			queue = queue[1:]
		case frame := <-c.responses:
			switch frame.Type {
			case FrameRequest:
				// A graceful close stops accepting new requests: frames that
				// arrive after it are discarded and the peer learns about the
				// close when the socket goes away.
				if c.handler != nil && !closing {
					inflight++
					c.startHandler(frame)
				}
			case FrameResponse, FrameError:
				complete(pending, frame)
			}
		case frame := <-c.handlerResults:
			// The reply frame is the handler's completion signal; the graceful
			// close joins on it through the inflight count.
			inflight--
			queue = append(queue, frame)
		case request := <-c.cancels:
			// Drop timed-out requests so a silent peer cannot grow pending forever.
			delete(pending, request.id)
		case <-c.closeReq:
			closing = true
		case <-c.killReq:
			c.stopWith(errTransportClosed)
			return
		case err := <-c.dead:
			c.stopWith(err)
			return
		}
		if closing && inflight == 0 && len(queue) == 0 {
			c.finishGraceful()
			return
		}
	}
}

func (c *connection) finishGraceful() {
	c.err = errTransportClosed
	// No reply is owed anymore: every handler delivered its reply and the
	// owner handed every queued frame to the writer, which finishes the
	// current write before observing stop. Closing the socket then unblocks
	// the reader. A Kill that arrives while the writer is stuck on a dead
	// wire takes the select below and aborts the flush.
	close(c.stop)
	select {
	case <-c.writerDone:
	case <-c.killReq:
	}
	_ = c.conn.Close()
	<-c.writerDone
	<-c.readerDone
	close(c.done)
	if c.onDone != nil {
		c.onDone(c)
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
	go func() {
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
	<-c.readerDone
	<-c.writerDone
	close(c.done)
	if c.onDone != nil {
		c.onDone(c)
	}
}
