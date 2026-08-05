package mail

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrOverloaded = errors.New("mailbox overloaded")
	ErrClosed     = errors.New("mailbox closed")
)

type Call func(context.Context) (any, error)

type Result struct {
	Value any
	Err   error
}

type Box struct {
	in   chan *call
	done chan struct{}

	mu     sync.Mutex
	closed bool
}

type call struct {
	fn    Call
	reply chan Result
	ctx   context.Context
}

func New(capacity int) *Box {
	b := &Box{
		in:   make(chan *call, capacity),
		done: make(chan struct{}),
	}
	go b.run()
	return b
}

func (b *Box) Call(ctx context.Context, fn Call) (any, error) {
	c := &call{fn: fn, reply: make(chan Result, 1), ctx: ctx}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	select {
	case b.in <- c:
		b.mu.Unlock()
	default:
		b.mu.Unlock()
		return nil, ErrOverloaded
	}

	select {
	case result := <-c.reply:
		return result.Value, result.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *Box) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	close(b.in)
}

func (b *Box) Done() <-chan struct{} {
	return b.done
}

func (b *Box) Len() int {
	return len(b.in)
}

func (b *Box) run() {
	defer close(b.done)

	for c := range b.in {
		if b.isClosed() {
			c.reply <- Result{Err: ErrClosed}
			b.rejectQueued()
			return
		}
		value, err := c.fn(c.ctx)
		c.reply <- Result{Value: value, Err: err}
		if b.isClosed() {
			b.rejectQueued()
			return
		}
	}

	b.rejectQueued()
}

func (b *Box) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

func (b *Box) rejectQueued() {
	for {
		select {
		case c, ok := <-b.in:
			if !ok {
				return
			}
			c.reply <- Result{Err: ErrClosed}
		default:
			return
		}
	}
}
