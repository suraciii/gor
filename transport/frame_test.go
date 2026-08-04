package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		typ     FrameType
		payload []byte
	}{
		{name: "request empty", typ: FrameRequest},
		{name: "response", typ: FrameResponse, payload: []byte("reply")},
		{name: "error", typ: FrameError, payload: []byte("failed")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := Frame{ID: 0x0102030405060708, Type: test.typ, Payload: test.payload}
			var wire bytes.Buffer
			if err := WriteFrame(&wire, want); err != nil {
				t.Fatal(err)
			}

			got, err := ReadFrame(&wire)
			if err != nil {
				t.Fatal(err)
			}
			if got.ID != want.ID || got.Type != want.Type || !bytes.Equal(got.Payload, want.Payload) {
				t.Fatalf("frame = %#v, want %#v", got, want)
			}
		})
	}
}

func TestFrameWireFormatIsBigEndian(t *testing.T) {
	frame := Frame{ID: 0x0102030405060708, Type: FrameResponse, Payload: []byte{0xaa, 0xbb}}
	var wire bytes.Buffer
	if err := WriteFrame(&wire, frame); err != nil {
		t.Fatal(err)
	}

	want := []byte{
		0, 0, 0, 2,
		1, 2, 3, 4, 5, 6, 7, 8,
		byte(FrameResponse),
		0xaa, 0xbb,
	}
	if !bytes.Equal(wire.Bytes(), want) {
		t.Fatalf("wire bytes = %x, want %x", wire.Bytes(), want)
	}
}

func TestFramePayloadLimit(t *testing.T) {
	want := Frame{ID: 9, Type: FrameRequest, Payload: bytes.Repeat([]byte{0x5a}, MaxPayloadSize)}
	var wire bytes.Buffer
	if err := WriteFrame(&wire, want); err != nil {
		t.Fatal(err)
	}

	got, err := ReadFrame(&wire)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Payload) != MaxPayloadSize || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatal("payload at the limit did not round trip")
	}
}

func TestReadFrameRejectsOversizedPayloadBeforeReadingPayload(t *testing.T) {
	header := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint32(header[:4], ^uint32(0))
	binary.BigEndian.PutUint64(header[4:12], 1)
	header[12] = byte(FrameRequest)
	reader := &headerOnlyReader{header: header}

	_, err := ReadFrame(reader)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("error = %v, want ErrPayloadTooLarge", err)
	}
	if reader.payloadRead {
		t.Fatal("ReadFrame attempted to read or allocate an oversized payload")
	}
}

func TestReadFrameRejectsInvalidType(t *testing.T) {
	header := make([]byte, frameHeaderSize)
	header[12] = 99

	_, err := ReadFrame(bytes.NewReader(header))
	if !errors.Is(err, ErrInvalidFrameType) {
		t.Fatalf("error = %v, want ErrInvalidFrameType", err)
	}
}

func TestReadFrameReturnsUnexpectedEOFForPartialFrame(t *testing.T) {
	frame := Frame{ID: 12, Type: FrameRequest, Payload: []byte("payload")}
	var wire bytes.Buffer
	if err := WriteFrame(&wire, frame); err != nil {
		t.Fatal(err)
	}
	full := wire.Bytes()

	for _, test := range []struct {
		name string
		wire []byte
	}{
		{name: "partial header", wire: full[:frameHeaderSize-1]},
		{name: "partial payload", wire: full[:len(full)-1]},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(test.wire))
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("error = %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}

func TestWriteFrameReturnsConnectionErrorAfterPartialWrite(t *testing.T) {
	wantErr := errors.New("connection closed")
	writer := &partialErrorWriter{remaining: frameHeaderSize / 2, err: wantErr}

	err := WriteFrame(writer, Frame{ID: 1, Type: FrameRequest, Payload: []byte("payload")})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want connection error", err)
	}
	if writer.written != frameHeaderSize/2 {
		t.Fatalf("bytes written = %d, want %d", writer.written, frameHeaderSize/2)
	}
}

func TestWriteFrameRejectsInvalidTypeAndOversizedPayload(t *testing.T) {
	for _, test := range []struct {
		name  string
		frame Frame
		want  error
	}{
		{name: "invalid type", frame: Frame{Type: 99}, want: ErrInvalidFrameType},
		{name: "oversized payload", frame: Frame{Type: FrameRequest, Payload: make([]byte, MaxPayloadSize+1)}, want: ErrPayloadTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			var wire bytes.Buffer
			err := WriteFrame(&wire, test.frame)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if wire.Len() != 0 {
				t.Fatalf("wrote %d bytes for invalid frame", wire.Len())
			}
		})
	}
}

type headerOnlyReader struct {
	header      []byte
	read        bool
	payloadRead bool
}

func (r *headerOnlyReader) Read(p []byte) (int, error) {
	if r.read {
		r.payloadRead = true
		return 0, errors.New("payload was read")
	}
	r.read = true
	copy(p, r.header)
	return len(r.header), nil
}

type partialErrorWriter struct {
	remaining int
	written   int
	err       error
}

func (w *partialErrorWriter) Write(p []byte) (int, error) {
	n := min(len(p), w.remaining)
	w.remaining -= n
	w.written += n
	return n, w.err
}
