package cluster

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

func TestNewZeroProbeConfigUsesDefaults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(100, 0).UTC()
		backend := &recordingMemberStore{backend: store.NewMemory()}
		config := testNodeConfig(backend, clock.NewFake(start), "node-a", "generation-a")
		config.ProbeInterval, config.ProbeTimeout, config.ProbeFailures, config.VoteTTL, config.MaxTickGap, config.MaxTableLatency = 0, 0, 0, 0, 0, 0
		node, err := New(config)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer node.Close()

		want := []any{time.Second, 500 * time.Millisecond, 3, 6 * time.Second, 2 * time.Second, 500 * time.Millisecond}
		got := []any{node.probeInterval, node.probeTimeout, node.probeFailureLimit, node.voteTTL, node.maxTickGap, node.maxTableLatency}
		names := []string{"probeInterval", "probeTimeout", "probeFailureLimit", "voteTTL", "maxTickGap", "maxTableLatency"}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s = %v, want %v", names[i], got[i], want[i])
			}
		}
	})
}

func TestNewNegativeProbeConfigRejected(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	backend := &recordingMemberStore{backend: store.NewMemory()}
	base := testNodeConfig(backend, clock.NewFake(start), "node-a", "generation-a")
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"ProbeInterval", func(c *Config) { c.ProbeInterval = -time.Second }},
		{"ProbeTimeout", func(c *Config) { c.ProbeTimeout = -time.Second }},
		{"ProbeFailures", func(c *Config) { c.ProbeFailures = -1 }},
		{"VoteTTL", func(c *Config) { c.VoteTTL = -time.Second }},
		{"MaxTickGap", func(c *Config) { c.MaxTickGap = -time.Second }},
		{"MaxTableLatency", func(c *Config) { c.MaxTableLatency = -time.Second }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := base
			tc.mutate(&config)
			if _, err := New(config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New = %v, want ErrInvalidConfig", err)
			}
		})
	}
}
