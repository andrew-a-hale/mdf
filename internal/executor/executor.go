package executor

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/andy/mdf/internal/connectors"
	"github.com/andy/mdf/internal/parser"
	"github.com/andy/mdf/internal/validator"
)

// Executor handles the execution of data ingestion jobs
type Executor struct {
	dataSource parser.DataSource
	connectors map[string]connectors.Connector
}

// New creates a new executor instance
func New(dataSource parser.DataSource, connectors map[string]connectors.Connector) *Executor {
	return &Executor{
		dataSource: dataSource,
		connectors: connectors,
	}
}

// Execute runs the data ingestion job
func (e *Executor) Execute() error {
	// Log the execution start with timestamp
	start := time.Now()
	slog.Info("Job started",
		"domain", e.dataSource.Domain,
		"name", e.dataSource.Name,
		"time", start.Format(time.RFC3339),
		"job_id", fmt.Sprintf("%s-%s-%d", e.dataSource.Domain, e.dataSource.Name, start.Unix()))

	// Get source connector
	sourceConnector, err := e.getConnector(e.dataSource.Source.Connector)
	if err != nil {
		slog.Error("Failed to get source connector",
			"error", err,
			"connector", e.dataSource.Source.Connector)
		return err
	}

	// Get destination connector
	destConnector, err := e.getConnector(e.dataSource.Destination.Connector)
	if err != nil {
		slog.Error("Failed to get destination connector",
			"error", err,
			"connector", e.dataSource.Destination.Connector)
		return err
	}

	// Extract data from source
	data, err := e.extractData(sourceConnector)
	if err != nil {
		slog.Error("Failed to extract data",
			"error", err,
			"source", e.dataSource.Source.FQNResource)
		return err
	}

	// Validate the data
	validator := validator.New(e.dataSource.Validate)
	err = validator.Validate(data)
	if err != nil {
		slog.Error("Validation failed", "error", err)
		return err
	}

	// Load data to destination
	err = e.loadData(destConnector, data)
	if err != nil {
		slog.Error("Failed to load data", "error", err)
		return err
	}

	// Log successful execution with duration
	end := time.Now()
	duration := end.Sub(start)
	slog.Info("Job completed",
		"domain", e.dataSource.Domain,
		"name", e.dataSource.Name,
		"records", len(data),
		"time", end.Format(time.RFC3339),
		"duration_ms", duration.Milliseconds(),
		"job_id", fmt.Sprintf("%s-%s-%d", e.dataSource.Domain, e.dataSource.Name, start.Unix()))
	return nil
}

// getConnector retrieves a connector by name
func (e *Executor) getConnector(name string) (connectors.Connector, error) {
	connector, ok := e.connectors[name]
	if !ok {
		return nil, fmt.Errorf("connector not found: %s", name)
	}
	return connector, nil
}

// extractData extracts data from the source
func (e *Executor) extractData(connector connectors.Connector) ([]map[string]any, error) {
	// Read data from the source
	return connector.Read(e.dataSource.Source.FQNResource)
}

// loadData loads data to the destination
func (e *Executor) loadData(connector connectors.Connector, data []map[string]any) error {
	// Generate destination path (appending timestamp for uniqueness if needed)
	destResource := fmt.Sprintf("%s_%s.csv",
		e.dataSource.Name,
		time.Now().Format("20060102150405"))

	// Write data to the destination
	return connector.Write(destResource, data)
}

