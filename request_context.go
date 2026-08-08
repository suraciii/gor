package gor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"unicode/utf8"
)

const (
	maxRequestContextEntries = 32
	maxRequestContextBytes   = 4096
	maxRequestContextKeySize = 128
)

type requestContextKey struct{}

type requestContextSnapshot struct {
	values map[string]any
}

type requestContextWireEntry struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value,omitempty"`
}

// WithRequestContext returns a context with one immutable Request Context
// entry. Values are normalized to the canonical types used by local and
// forwarded Calls. On validation failure it returns the original context and
// an error matching ErrRequestEncodeFailed.
func WithRequestContext(ctx context.Context, key string, value any) (context.Context, error) {
	if ctx == nil {
		return nil, requestContextEncodeError(fmt.Errorf("context is nil"))
	}
	if err := validateRequestContextKey(key); err != nil {
		return ctx, requestContextEncodeError(err)
	}
	canonical, err := normalizeRequestContextValue(value)
	if err != nil {
		return ctx, requestContextEncodeError(err)
	}

	values := cloneRequestContextValues(requestContextSnapshotFrom(ctx).values)
	values[key] = canonical
	snapshot := requestContextSnapshot{values: values}
	if _, err := marshalRequestContext(snapshot); err != nil {
		return ctx, requestContextEncodeError(err)
	}
	return context.WithValue(ctx, requestContextKey{}, snapshot), nil
}

// RequestContextValue returns the canonical Request Context value for key. A
// stored nil value is reported as present; a missing key returns (nil, false).
func RequestContextValue(ctx context.Context, key string) (value any, ok bool) {
	if ctx == nil {
		return nil, false
	}
	value, ok = requestContextSnapshotFrom(ctx).values[key]
	return value, ok
}

func requestContextEncodeError(err error) error {
	return withCode(ErrRequestEncodeFailed, fmt.Errorf("request context: %w", err))
}

func requestContextSnapshotFrom(ctx context.Context) requestContextSnapshot {
	if ctx == nil {
		return requestContextSnapshot{}
	}
	snapshot, _ := ctx.Value(requestContextKey{}).(requestContextSnapshot)
	return snapshot
}

func withRequestContextSnapshot(ctx context.Context, snapshot requestContextSnapshot) context.Context {
	return context.WithValue(ctx, requestContextKey{}, snapshot)
}

func cloneRequestContextValues(values map[string]any) map[string]any {
	if len(values) == 0 {
		return make(map[string]any)
	}
	clone := make(map[string]any, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func validateRequestContextKey(key string) error {
	if key == "" {
		return fmt.Errorf("key is empty")
	}
	if !utf8.ValidString(key) {
		return fmt.Errorf("key is not valid UTF-8")
	}
	if len(key) > maxRequestContextKeySize {
		return fmt.Errorf("key is %d bytes, maximum is %d", len(key), maxRequestContextKeySize)
	}
	return nil
}

func normalizeRequestContextValue(value any) (any, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return value, nil
	case string:
		if !utf8.ValidString(value) {
			return nil, fmt.Errorf("value is not valid UTF-8")
		}
		return value, nil
	case int:
		return int64(value), nil
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case uint:
		return uint64(value), nil
	case uint8:
		return uint64(value), nil
	case uint16:
		return uint64(value), nil
	case uint32:
		return uint64(value), nil
	case uint64:
		return value, nil
	case float32:
		value64 := float64(value)
		if math.IsNaN(value64) || math.IsInf(value64, 0) {
			return nil, fmt.Errorf("value is not finite")
		}
		return value64, nil
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("value is not finite")
		}
		return value, nil
	default:
		return nil, fmt.Errorf("value type %T is not a supported scalar", value)
	}
}

func marshalRequestContext(snapshot requestContextSnapshot) ([]byte, error) {
	if len(snapshot.values) > maxRequestContextEntries {
		return nil, fmt.Errorf("has %d entries, maximum is %d", len(snapshot.values), maxRequestContextEntries)
	}
	wire := make(map[string]requestContextWireEntry, len(snapshot.values))
	for key, value := range snapshot.values {
		if err := validateRequestContextKey(key); err != nil {
			return nil, err
		}
		entry, err := marshalRequestContextEntry(value)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", key, err)
		}
		wire[key] = entry
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("encode entries: %w", err)
	}
	if len(encoded) > maxRequestContextBytes {
		return nil, fmt.Errorf("is %d bytes, maximum is %d", len(encoded), maxRequestContextBytes)
	}
	return encoded, nil
}

func marshalRequestContextEntry(value any) (requestContextWireEntry, error) {
	canonical, err := normalizeRequestContextValue(value)
	if err != nil {
		return requestContextWireEntry{}, err
	}
	if canonical == nil {
		return requestContextWireEntry{Type: "null"}, nil
	}

	var typeName string
	switch canonical.(type) {
	case bool:
		typeName = "bool"
	case string:
		typeName = "string"
	case int64:
		typeName = "int64"
	case uint64:
		typeName = "uint64"
	case float64:
		typeName = "float64"
	default:
		return requestContextWireEntry{}, fmt.Errorf("value type %T is not a canonical scalar", canonical)
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return requestContextWireEntry{}, fmt.Errorf("encode value: %w", err)
	}
	return requestContextWireEntry{Type: typeName, Value: encoded}, nil
}

func requestContextPayload(ctx context.Context) ([]byte, error) {
	snapshot := requestContextSnapshotFrom(ctx)
	if len(snapshot.values) == 0 {
		return nil, nil
	}
	return marshalRequestContext(snapshot)
}

func decodeRequestContext(raw json.RawMessage) (requestContextSnapshot, error) {
	if len(raw) == 0 {
		return requestContextSnapshot{}, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if !utf8.Valid(trimmed) {
		return requestContextSnapshot{}, fmt.Errorf("request_context is not valid UTF-8")
	}
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return requestContextSnapshot{}, fmt.Errorf("request_context must be a JSON object")
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &wire); err != nil {
		return requestContextSnapshot{}, fmt.Errorf("decode request_context: %w", err)
	}
	if wire == nil {
		return requestContextSnapshot{}, fmt.Errorf("request_context must be a JSON object")
	}
	if len(wire) > maxRequestContextEntries {
		return requestContextSnapshot{}, fmt.Errorf("request_context has %d entries, maximum is %d", len(wire), maxRequestContextEntries)
	}

	values := make(map[string]any, len(wire))
	for key, rawEntry := range wire {
		if err := validateRequestContextKey(key); err != nil {
			return requestContextSnapshot{}, fmt.Errorf("request_context key %q: %w", key, err)
		}
		value, err := decodeRequestContextEntry(rawEntry)
		if err != nil {
			return requestContextSnapshot{}, fmt.Errorf("request_context key %q: %w", key, err)
		}
		values[key] = value
	}
	snapshot := requestContextSnapshot{values: values}
	if _, err := marshalRequestContext(snapshot); err != nil {
		return requestContextSnapshot{}, fmt.Errorf("validate request_context: %w", err)
	}
	return snapshot, nil
}

func decodeRequestContextEntry(raw json.RawMessage) (any, error) {
	trimmed := bytes.TrimSpace(raw)
	if !utf8.Valid(trimmed) {
		return nil, fmt.Errorf("entry is not valid UTF-8")
	}
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, fmt.Errorf("entry must be a JSON object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return nil, fmt.Errorf("decode entry: %w", err)
	}
	if fields == nil {
		return nil, fmt.Errorf("entry must be a JSON object")
	}

	rawType, ok := fields["type"]
	if !ok || isJSONNull(rawType) {
		return nil, fmt.Errorf("entry type is missing")
	}
	var typeName string
	if err := json.Unmarshal(rawType, &typeName); err != nil || typeName == "" {
		return nil, fmt.Errorf("entry type is not a string")
	}
	if typeName == "null" {
		if _, ok := fields["value"]; ok {
			return nil, fmt.Errorf("null entry must not have a value")
		}
		return nil, nil
	}

	rawValue, ok := fields["value"]
	if !ok || isJSONNull(rawValue) {
		return nil, fmt.Errorf("%s entry value is missing or null", typeName)
	}
	switch typeName {
	case "bool":
		var value bool
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return nil, fmt.Errorf("decode bool value: %w", err)
		}
		return value, nil
	case "string":
		var value string
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return nil, fmt.Errorf("decode string value: %w", err)
		}
		return value, nil
	case "int64":
		var value int64
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return nil, fmt.Errorf("decode int64 value: %w", err)
		}
		return value, nil
	case "uint64":
		var value uint64
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return nil, fmt.Errorf("decode uint64 value: %w", err)
		}
		return value, nil
	case "float64":
		var value float64
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return nil, fmt.Errorf("decode float64 value: %w", err)
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("float64 value is not finite")
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown entry type %q", typeName)
	}
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
