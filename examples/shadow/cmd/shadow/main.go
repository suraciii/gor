package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/suraciii/gor"
	shadow "github.com/suraciii/gor/examples/shadow"
	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/transport"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string) (runErr error) {
	flags := flag.NewFlagSet("shadow", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	address := flags.String("addr", ":8080", "HTTP listen address")
	databasePath := flags.String("db", "data/gor.db", "SQLite database path (shared by every node in cluster mode)")
	clusterEnabled := flags.Bool("cluster", false, "run as one node of a cluster; all nodes share -db")
	nodeAddr := flags.String("node-addr", "", "cluster transport bind address; defaults to 127.0.0.1:0 when -cluster")
	generation := flags.String("generation", "", "cluster membership generation; defaults to a fresh value when -cluster")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}

	if err := os.MkdirAll(filepath.Dir(*databasePath), 0o755); err != nil {
		return fmt.Errorf("create database directory: %w", err)
	}
	database, err := store.OpenSQLite(*databasePath)
	if err != nil {
		return fmt.Errorf("open SQLite database: %w", err)
	}
	defer func() {
		if err := database.Close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("close SQLite database: %w", err)
		}
	}()

	clusterOptions, nodeTransport, err := configureCluster(*clusterEnabled, database, *nodeAddr, *generation)
	if err != nil {
		return err
	}
	rt, err := newRuntime(database, clusterOptions...)
	if err != nil {
		if nodeTransport != nil {
			nodeTransport.Close()
		}
		return fmt.Errorf("create runtime: %w", err)
	}
	defer rt.Close()
	if err := shadow.Register(rt); err != nil {
		return fmt.Errorf("register shadow entities: %w", err)
	}

	server := &http.Server{
		Addr:    *address,
		Handler: shadow.NewHandler(rt),
	}
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()
	log.Printf("device shadow listening on %s with database %s", *address, *databasePath)

	select {
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		return nil
	}
}

// configureCluster builds the cluster options for this node when clustering is
// enabled, or returns nil when running single-node. The same database backs
// both entity state and the shared membership table: every node opens the same
// SQLite file. nodeAddr defaults to an OS-chosen loopback port; a fresh
// generation is taken on every start, so a node that rejoins at the same
// address never reuses its previous incarnation's row. The returned transport
// is owned by the runtime once New succeeds; the caller closes it only on the
// early-error path.
func configureCluster(enabled bool, members store.MemberStore, nodeAddr, generation string) ([]gor.Option, *transport.TCP, error) {
	if !enabled {
		return nil, nil, nil
	}
	if nodeAddr == "" {
		nodeAddr = "127.0.0.1:0"
	}
	nodeTransport, err := transport.New(nodeAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("bind cluster transport: %w", err)
	}
	if generation == "" {
		generation, err = newGeneration()
		if err != nil {
			nodeTransport.Close()
			return nil, nil, fmt.Errorf("generate membership generation: %w", err)
		}
	}
	log.Printf("cluster node %s generation %s", nodeTransport.Addr(), generation)
	options := []gor.Option{
		gor.WithMemberStore(members),
		gor.WithNodeAddr(nodeTransport.Addr()),
		gor.WithGeneration(generation),
		gor.WithTransport(nodeTransport),
	}
	return options, nodeTransport, nil
}

// newGeneration returns a fresh membership generation. A generation must be
// new on every rejoin at the same address, so a restarted node does not claim
// the row of its previous incarnation while others may still be voting on it.
func newGeneration() (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func newRuntime(database store.Store, options ...gor.Option) (*gor.Runtime, error) {
	options = append([]gor.Option{gor.WithStore(database), gor.OnError(shadow.LogBackgroundError)}, options...)
	return gor.New(options...)
}
