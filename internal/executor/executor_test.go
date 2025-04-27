package executor

import (
	"testing"

	"github.com/andrew-a-hale/mdf/internal/parser"
)

// MockConnector is a mock implementation of the Connector interface for testing
type MockConnector struct {
	readFunc  func() ([]map[string]any, error)
	writeFunc func(data []map[string]any) error
}

// NewMockConnector creates a new mock connector
func NewMockConnector() *MockConnector {
	return &MockConnector{
		readFunc: func() ([]map[string]any, error) {
			return []map[string]any{}, nil
		},
		writeFunc: func(data []map[string]any) error {
			return nil
		},
	}
}

// Read implements the Connector interface
func (m *MockConnector) Read() ([]map[string]any, error) {
	if m.readFunc != nil {
		return m.readFunc()
	}
	return []map[string]any{}, nil
}

// Write implements the Connector interface
func (m *MockConnector) Write(data []map[string]any) error {
	if m.writeFunc != nil {
		return m.writeFunc(data)
	}
	return nil
}

func TestNew(t *testing.T) {
	// Create test data source
	ds := parser.DataSource{
		Domain: "test",
		Name:   "test_source",
		Source: parser.SourceConfig{
			Connector: "test_connector",
		},
		Destination: parser.DestinationConfig{
			Connector: "test_connector",
		},
	}

	// Test executor creation
	exec := New(parser.Config{Connectors: nil, DataSource: ds})
	if exec == nil {
		t.Fatal("New() returned nil")
	}

	if exec.Config.DataSource.Source.Connector != ds.Source.Connector {
		t.Errorf("Executor has wrong data source reference")
	}
}
