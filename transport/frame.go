package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	MaxPayloadSize  = 1 << 20
	frameHeaderSize = 4 + 8 + 1
)

var (
	ErrPayloadTooLarge  = errors.New("transport payload is too large")
	ErrInvalidFrameType = errors.New("transport frame type is invalid")
)

type FrameType uint8

const (
	FrameRequest FrameType = iota + 1
	FrameResponse
	FrameError
)

type Frame struct {
	ID      uint64
	Type    FrameType
	Payload []byte
}

func ReadFrame(r io.Reader) (Frame, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}

	payloadSize := binary.BigEndian.Uint32(header[:4])
	if uint64(payloadSize) > MaxPayloadSize {
		return Frame{}, fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, payloadSize)
	}

	frame := Frame{
		ID:   binary.BigEndian.Uint64(header[4:12]),
		Type: FrameType(header[12]),
	}
	if !frame.Type.valid() {
		return Frame{}, fmt.Errorf("%w: %d", ErrInvalidFrameType, frame.Type)
	}

	frame.Payload = make([]byte, int(payloadSize))
	if _, err := io.ReadFull(r, frame.Payload); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func WriteFrame(w io.Writer, frame Frame) error {
	if !frame.Type.valid() {
		return fmt.Errorf("%w: %d", ErrInvalidFrameType, frame.Type)
	}
	if uint64(len(frame.Payload)) > MaxPayloadSize {
		return fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, len(frame.Payload))
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
