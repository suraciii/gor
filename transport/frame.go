// Package transport moves opaque request and response payloads between gor
// runtimes.
//
// It defines the extension boundary for custom transports and provides TCP
// and frame helpers. Transport does not interpret entity identities, methods,
// or application payloads.
package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// MaxPayloadSize is the largest payload accepted in one frame, in bytes.
	// A payload larger than this value is rejected before it is written or
	// allocated; an incoming connection using the frame protocol is terminated
	// after such a frame is detected.
	MaxPayloadSize  = 1 << 20
	frameHeaderSize = 4 + 8 + 1
)

var (
	errPayloadTooLarge  = errors.New("transport payload is too large")
	errInvalidFrameType = errors.New("transport frame type is invalid")
)

// FrameType identifies the role of a transport frame.
type FrameType uint8

const (
	// FrameRequest carries a request from a sender to a handler.
	FrameRequest FrameType = iota + 1
	// FrameResponse carries a successful response to a request.
	FrameResponse
	// FrameError carries an error response. TCP encodes handler errors as text
	// in this payload rather than serializing their error values.
	FrameError
)

// Frame is one request, response, or error message with a correlation ID.
// Its payload must not exceed MaxPayloadSize.
type Frame struct {
	ID      uint64
	Type    FrameType
	Payload []byte
}

// ReadFrame reads one complete frame from r.
//
// It rejects unknown frame types and payloads larger than MaxPayloadSize
// before reading an oversized payload. Short input and reader failures are
// returned unchanged or as the corresponding io error.
func ReadFrame(r io.Reader) (Frame, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}

	payloadSize := binary.BigEndian.Uint32(header[:4])
	if uint64(payloadSize) > MaxPayloadSize {
		return Frame{}, fmt.Errorf("%w: %d bytes", errPayloadTooLarge, payloadSize)
	}

	frame := Frame{
		ID:   binary.BigEndian.Uint64(header[4:12]),
		Type: FrameType(header[12]),
	}
	if !frame.Type.valid() {
		return Frame{}, fmt.Errorf("%w: %d", errInvalidFrameType, frame.Type)
	}

	frame.Payload = make([]byte, int(payloadSize))
	if _, err := io.ReadFull(r, frame.Payload); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

// WriteFrame validates and writes one complete frame to w.
//
// Invalid frame types and payloads larger than MaxPayloadSize are rejected
// before any bytes are written. Errors from w are returned to the caller.
func WriteFrame(w io.Writer, frame Frame) error {
	if !frame.Type.valid() {
		return fmt.Errorf("%w: %d", errInvalidFrameType, frame.Type)
	}
	if uint64(len(frame.Payload)) > MaxPayloadSize {
		return fmt.Errorf("%w: %d bytes", errPayloadTooLarge, len(frame.Payload))
	}

	var header [frameHeaderSize]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(frame.Payload)))
	binary.BigEndian.PutUint64(header[4:12], frame.ID)
	header[12] = byte(frame.Type)
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(frame.Payload)
	return err
}

func (t FrameType) valid() bool {
	return t >= FrameRequest && t <= FrameError
}
