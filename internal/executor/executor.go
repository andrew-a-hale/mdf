package executor

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/andrew-a-hale/mdf/internal/connectors"
	"github.com/andrew-a-hale/mdf/internal/connectors/filesystem"
	"github.com/andrew-a-hale/mdf/internal/parser"
	"github.com/andrew-a-hale/mdf/internal/validator"
)

// Executor handles the execution of data ingestion jobs
type Executor struct {
	Config parser.Config
}

// New creates a new executor instance
func New(config parser.Config) *Executor {
	return &Executor{
		Config: config,
	}
}

// Execute runs the data ingestion job
func (e *Executor) Execute() error {
	// Log the execution start with timestamp
	start := time.Now()
	jobID := fmt.Sprintf("%s-%s-%d", e.Config.DataSource.Domain, e.Config.DataSource.Name, start.Unix())
	slog.Info("Job started",
		"domain", e.Config.DataSource.Domain,
		"name", e.Config.DataSource.Name,
		"time", start.Format(time.RFC3339),
		"job_id", jobID)

	// Get source connector
	var err error
	var sourceConnecter connectors.Connector
	switch e.Config.Connectors["source"].(map[string]any)["type"] {
	case connectors.FILESYSTEM:
		sourceConnecter, err = filesystem.New(
			filepath.Join("raw", e.Config.DataSource.Domain),
			e.Config.Connectors["source"].(map[string]any)["partition"].(string),
			e.Config.DataSource.Fields,
		)
		if err != nil {
			slog.Error("failed to initialise source connector", "error", err)
			return fmt.Errorf("failed to initialise source connector: %v", err)
		}
	default:
		slog.Error("failed to initialise source connector", "error", err)
		return fmt.Errorf("failed to initialise source connector: %v", err)
	}
	defer sourceConnecter.Close()

	// Get destination connector
	var destConnecter connectors.Connector
	switch e.Config.Connectors["destination"].(map[string]any)["type"] {
	case connectors.FILESYSTEM:
		destConnecter, err = filesystem.New(
			filepath.Join("ingested", e.Config.DataSource.Domain),
			e.Config.Connectors["destination"].(map[string]any)["partition"].(string),
			e.Config.DataSource.Fields,
		)
		if err != nil {
			slog.Error("failed to initialise destination connector", "error", err)
			return fmt.Errorf("failed to initialise destination connector: %v", err)
		}
	default:
		slog.Error("failed to initialise destination connector", "error", err)
		return fmt.Errorf("failed to initialise destination connector: %v", err)
	}
	defer destConnecter.Close()

	// Extract data from source
	data, err := sourceConnecter.Read()
	if err != nil {
		slog.Error("Failed to extract data",
			"error", err,
			"source", e.Config.DataSource.Source.FQNResource)
		return err
	}

	// Validate the data
	validator := validator.New(e.Config.DataSource.Validate)
	err = validator.Validate(data)
	if err != nil {
		slog.Error("Validation failed", "error", err)
		return err
	}

	// Load data to destination
	err = destConnecter.Write(data)
	if err != nil {
		slog.Error("Failed to load data", "error", err)
		return err
	}

	// TODO: Add eventlog and notifier
	// Log successful execution with duration
	end := time.Now()
	duration := end.Sub(start)
	slog.Info("Job completed",
		"domain", e.Config.DataSource.Domain,
		"name", e.Config.DataSource.Name,
		"records", len(data),
		"time", end.Format(time.RFC3339),
		"duration_ms", duration.Milliseconds(),
		"job_id", jobID)

	return nil
}
