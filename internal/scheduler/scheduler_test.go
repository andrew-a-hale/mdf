package scheduler

import (
	"testing"
	"time"

	"github.com/andy/mdf/internal/parser"
)

func TestNew(t *testing.T) {
	// Create a simple config
	config := &parser.Config{
		Connectors:  make(map[string]any),
		DataSources: []parser.DataSource{},
	}

	// Test scheduler creation
	s := New(config)
	if s == nil {
		t.Fatal("New() returned nil")
	}

	if s.config != config {
		t.Errorf("Scheduler has wrong config reference")
	}

	if s.cron == nil {
		t.Errorf("Cron instance not initialized")
	}
}

func TestStartStop(t *testing.T) {
	// Create test data source
	ds := parser.DataSource{
		Domain: "test",
		Name:   "test_source",
		Schedule: parser.ScheduleConfig{
			Cron:         "* * * * *", // Every minute
			RandomOffset: false,
		},
		Source: parser.SourceConfig{
			Connector: "test_connector",
		},
		Destination: parser.DestinationConfig{
			Connector: "test_connector",
		},
	}

	// Create config with the test data source
	config := &parser.Config{
		Connectors: map[string]any{
			"test_connector": "test",
		},
		DataSources: []parser.DataSource{ds},
	}

	// Create scheduler
	s := New(config)

	// Test Start
	err := s.Start()
	if err != nil {
		t.Errorf("Start() error = %v, want nil", err)
	}

	// Verify job was scheduled
	entries := s.cron.Entries()
	if len(entries) != 1 {
		t.Errorf("Expected 1 scheduled job, got %d", len(entries))
	}

	// Test Stop
	s.Stop()

	// Let the cron engine process the stop
	time.Sleep(10 * time.Millisecond)
}

func TestScheduleDataSourceWithInvalidCron(t *testing.T) {
	// Create test data source with invalid cron expression
	ds := parser.DataSource{
		Domain: "test",
		Name:   "test_source",
		Schedule: parser.ScheduleConfig{
			Cron:         "invalid cron",
			RandomOffset: false,
		},
		Source: parser.SourceConfig{
			Connector: "test_connector",
		},
		Destination: parser.DestinationConfig{
			Connector: "test_connector",
		},
	}

	// Create config with the test data source
	config := &parser.Config{
		Connectors: map[string]any{
			"test_connector": "test",
		},
		DataSources: []parser.DataSource{ds},
	}

	// Create scheduler
	s := New(config)

	// Test Start with invalid cron
	err := s.Start()
	if err == nil {
		t.Error("Start() with invalid cron should return error")
	}
}
