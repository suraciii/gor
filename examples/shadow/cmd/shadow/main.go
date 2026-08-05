package main

import (
	"context"
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
	"github.com/suraciii/gor/clock"
	shadow "github.com/suraciii/gor/examples/shadow"
	"github.com/suraciii/gor/store"
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
	databasePath := flags.String("db", "data/gor.db", "SQLite database path")
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

	rt, err := gor.New(gor.WithStore(database))
	if err != nil {
		return fmt.Errorf("create runtime: %w", err)
	}
	defer rt.Close()
	if err := shadow.Register(rt, clock.Real{}); err != nil {
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
