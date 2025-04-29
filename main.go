package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/andrew-a-hale/mdf/internal/parser"
	"github.com/andrew-a-hale/mdf/internal/scheduler"
	"github.com/andrew-a-hale/mdf/internal/triggerer"
)

func main() {
	// Setup structured logging
	logHandler := slog.NewJSONHandler(os.Stdout, nil)
	slog.SetDefault(slog.New(logHandler))

	configDir := flag.String("config-dir", "configs", "Path to the directory containing configuration files")
	flag.Parse()

	if *configDir == "" {
		slog.Error("No config directory provided", "flag", "-config-dir")
		os.Exit(1)
	}

	// Parse configuration directory
	slog.Info("Using config directory", "dir", *configDir)
	config, err := parser.ParseConfigDirectory(*configDir)
	if err != nil {
		slog.Error("Failed to parse config directory", "error", err, "dir", *configDir)
		os.Exit(1)
	}

	// Initialize and start the Triggerer
	triggerer := triggerer.New(config)
	err = triggerer.Start()
	if err != nil {
		slog.Error("Failed to start triggerer", "error", err)
		os.Exit(1)
	}

	slog.Info("Triggerer is running in background...")

	// Initialize and start the Scheduler
	scheduler := scheduler.New()
	err = scheduler.Start()
	if err != nil {
		slog.Error("Failed to start scheduler", "error", err)
		os.Exit(1)
	}

	slog.Info("Scheduler is running in background, waiting for jobs")

	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Keepalive ticker for logging that process is still running
	keepaliveTicker := time.NewTicker(30 * time.Minute)
	defer keepaliveTicker.Stop()

	// Keep process alive until interrupted
	for {
		select {
		case sig := <-sigCh:
			slog.Info("Received signal, shutting down", "signal", sig.String())
			triggerer.Stop()
			// scheduler.Stop()
			slog.Info("Triggerer stopped, exiting")
			return
		case <-keepaliveTicker.C:
			slog.Info("Triggerer is still running")
		}
	}
}
