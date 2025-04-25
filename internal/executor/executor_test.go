package executor

import (
	"testing"

	"github.com/andy/mdf/internal/connectors"
	"github.com/andy/mdf/internal/parser"
)

// MockConnector is a mock implementation of the Connector interface for testing
type MockConnector struct {
	readFunc  func(resource string) ([]map[string]any, error)
	writeFunc func(resource string, data []map[string]any) error
}

// NewMockConnector creates a new mock connector
func NewMockConnector() *MockConnector {
	return &MockConnector{
		readFunc: func(resource string) ([]map[string]any, error) {
			return []map[string]any{}, nil
		},
		writeFunc: func(resource string, data []map[string]any) error {
			return nil
		},
	}
}

// Read implements the Connector interface
func (m *MockConnector) Read(resource string) ([]map[string]any, error) {
	if m.readFunc != nil {
		return m.readFunc(resource)
	}
	return []map[string]any{}, nil
}

// Write implements the Connector interface
func (m *MockConnector) Write(resource string, data []map[string]any) error {
	if m.writeFunc != nil {
		return m.writeFunc(resource, data)
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

	// Create connectors map
	connectors := map[string]connectors.Connector{
		"test_connector": NewMockConnector(),
	}

	// Test executor creation
	exec := New(ds, connectors)
	if exec == nil {
		t.Fatal("New() returned nil")
	}

	if exec.dataSource.Source.Connector != ds.Source.Connector {
		t.Errorf("Executor has wrong data source reference")
	}
}

func TestGetConnector(t *testing.T) {
	// Create test data source
	ds := parser.DataSource{
		Domain: "test",
		Name:   "test_source",
	}

	// Create a mock connector
	mockConnector := NewMockConnector()

	// Create connectors map
	connectorMap := map[string]connectors.Connector{
		"existing_connector": mockConnector,
	}

	// Create executor
	exec := New(ds, connectorMap)

	// Test getting existing connector
	conn, err := exec.getConnector("existing_connector")
	if err != nil {
		t.Errorf("getConnector() error = %v, want nil", err)
	}

	if conn != mockConnector {
		t.Errorf("getConnector() didn't return the expected connector")
	}

	// Test getting non-existent connector
	_, err = exec.getConnector("non_existent_connector")
	if err == nil {
		t.Error("getConnector() with non-existent connector should return error")
	}
}

func TestExtractData(t *testing.T) {
	// Create test data source
	ds := parser.DataSource{
		Domain: "test",
		Name:   "test_source",
		Source: parser.SourceConfig{
			Connector:   "test_connector",
			FQNResource: "test.csv",
		},
	}

	// Expected test data
	expectedData := []map[string]any{
		{"id": "1", "name": "Test1"},
		{"id": "2", "name": "Test2"},
	}

	// Create a mock connector with a custom Read function
	mockConnector := NewMockConnector()
	mockConnector.readFunc = func(resource string) ([]map[string]any, error) {
		if resource == "test.csv" {
			return expectedData, nil
		}
		return nil, nil
	}

	// Create executor with empty connectors (we'll pass the mock directly to extractData)
	exec := New(ds, map[string]connectors.Connector{})

	// Test extract data
	data, err := exec.extractData(mockConnector)
	if err != nil {
		t.Errorf("extractData() error = %v, want nil", err)
	}

	// Verify the returned data
	if len(data) != len(expectedData) {
		t.Errorf("extractData() returned %d records, want %d", len(data), len(expectedData))
	}

	// Check content of the returned data
	for i, record := range data {
		for key, expectedValue := range expectedData[i] {
			if val, ok := record[key]; !ok || val != expectedValue {
				t.Errorf("extractData() record[%d][%s] = %v, want %v", i, key, val, expectedValue)
			}
		}
	}
}

func TestLoadData(t *testing.T) {
	// Create test data source
	ds := parser.DataSource{
		Domain: "test",
		Name:   "test_source",
	}

	// Test data to be loaded
	testData := []map[string]any{
		{"id": "1", "name": "Test1"},
		{"id": "2", "name": "Test2"},
	}

	// Counter to verify the write was called
	writeCounter := 0

	// Create a mock connector with a custom Write function
	mockConnector := NewMockConnector()
	mockConnector.writeFunc = func(resource string, data []map[string]any) error {
		writeCounter++

		// Verify the data being written
		if len(data) != len(testData) {
			t.Errorf("Write() called with %d records, want %d", len(data), len(testData))
		}

		return nil
	}

	// Create executor with empty connectors (we'll pass the mock directly to loadData)
	exec := New(ds, map[string]connectors.Connector{})

	// Test load data
	err := exec.loadData(mockConnector, testData)
	if err != nil {
		t.Errorf("loadData() error = %v, want nil", err)
	}

	// Verify that write was called
	if writeCounter != 1 {
		t.Errorf("Write() was called %d times, want 1", writeCounter)
	}
}
