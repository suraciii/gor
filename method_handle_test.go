package gor

import (
	"context"
	"testing"
)

// handleProbe is the entity interface the extraction tripwire locks its map
// on. Both methods must have the schedule shape func(handleProbe,
// context.Context) error, which is what Handle admits.
type handleProbe interface {
	Wake(context.Context) error
	Arm(context.Context) error
}

// TestHandle_ExtractsTrailingMethodName locks the map from an interface method
// expression to the trailing-segment method name. Handle reads that name with
// reflect and runtime.FuncForPC, and FuncForPC's name format is not a
// Go-documented contract — it is empirically stable, but Go is free to change
// it. This test exists so that a Go upgrade which changes the encoding breaks
// the build instead of silently mis-naming schedules: a mis-read name would be
// stored into the schedule table, match no dispatch case, and fail every
// delivery with "unknown method". It is a tripwire for an external contract,
// not a check of business logic — keep it as long as Handle reads names from
// FuncForPC, and delete it only when Handle stops doing that.
func TestHandle_ExtractsTrailingMethodName(t *testing.T) {
	handles := []struct {
		name   string
		method func(handleProbe, context.Context) error
		want   string
	}{
		{name: "Wake", method: handleProbe.Wake, want: "Wake"},
		{name: "Arm", method: handleProbe.Arm, want: "Arm"},
	}
	for _, h := range handles {
		if got := Handle(h.method).method; got != h.want {
			t.Errorf("Handle(%s) extracted %q, want %q", h.name, got, h.want)
		}
	}
}
