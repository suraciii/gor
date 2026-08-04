package transport

import (
	"fmt"
	"os"
	"runtime"
	"testing"
)

func TestMain(m *testing.M) {
	before := runtime.NumGoroutine()
	code := m.Run()
	after := runtime.NumGoroutine()
	if after != before {
		fmt.Fprintf(os.Stderr, "goroutine leak: before=%d after=%d\n", before, after)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}
