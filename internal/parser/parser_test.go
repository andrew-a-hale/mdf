package parser

import (
	"os"
	"path/filepath"
	"testing"
)

func init() {
	path, _ := os.Getwd()
	os.MkdirAll(filepath.Join(path, "../../raw"), 0755)
	os.MkdirAll(filepath.Join(path, "../../ingested"), 0755)
}

func TestParseConfigFile(t *testing.T) {
	// Create temporary config file
	tempFile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Write test configuration
	testConfig := `
id: config1
connectors:
  source: 
    type: filesystem
    base_path: ../../raw
    partition: daily
  destination: 
    type: filesystem
    base_path: ../../ingested
    partition: daily
data_source:
  domain: test
  name: users
  source: 
    connector: source
    fqn_resource: users.csv
    is_cdc: false
    primary_key: [id]
    timestamp_field: updated_at
  destination:
    connector: destination
    ordering: [id asc]
  trigger:
    cron: "0 0 * * *"
    random_offset: false
  validate:
    not_null: [id, name]
    unique: [id]
  fields:
    - label: id
      data_type: string
    - label: name
      data_type: string
`
	if _, err := tempFile.Write([]byte(testConfig)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	// Test valid config
	config, err := ParseConfigFile(tempFile.Name())
	if err != nil {
		t.Fatalf("ParseConfigFile() error = %v", err)
	}

	// Verify parsed values
	if _, ok := config.Connectors[config.DataSource.Source.Connector]; !ok {
		t.Errorf("Expected source connector in connectors, %s not found", config.DataSource.Source.Connector)
	}

	// Verify parsed values
	if _, ok := config.Connectors[config.DataSource.Destination.Connector]; !ok {
		t.Errorf("Expected destination connector in connectors, %s not found", config.DataSource.Destination.Connector)
	}

	ds := config.DataSource
	if ds.Domain != "test" {
		t.Errorf("Expected domain 'test', got '%s'", ds.Domain)
	}
	if ds.Name != "users" {
		t.Errorf("Expected name 'users', got '%s'", ds.Name)
	}
	if ds.Source.Connector != "source" {
		t.Errorf("Expected connector 'source', got '%s'", ds.Source.Connector)
	}
	if ds.Destination.Connector != "destination" {
		t.Errorf("Expected connector 'destination', got '%s'", ds.Destination.Connector)
	}
	if ds.Trigger.Cron != "0 0 * * *" {
		t.Errorf("Expected cron '0 0 * * *', got '%s'", ds.Trigger.Cron)
	}
	if len(ds.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(ds.Fields))
	}

	// Test with non-existent file
	_, err = ParseConfigFile("/non/existent/file.yaml")
	if err == nil {
		t.Error("ParseConfigFile() with non-existent file should return error")
	}
}

func TestParseConfigDirectory(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "config-dir-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create first config file
	config1 := `
id: config1
connectors:
  source: 
    type: filesystem
    base_path: ../../raw
    partition: daily
  destination: 
    type: filesystem
    base_path: ../../ingested
    partition: daily
data_sources:
  domain: test
  name: users
  source: 
    connector: source
    fqn_resource: users.csv
    is_cdc: false
    primary_key: [id]
    timestamp_field: updated_at
  destination:
    connector: destination
    ordering: [id asc]
  trigger:
    cron: "0 0 * * *"
    random_offset: false
  validate:
    not_null: [id, name]
    unique: [id]
  fields:
    - label: id
      data_type: string
    - label: name
      data_type: string
`
	config1Path := filepath.Join(tempDir, "config1.yaml")
	if err := os.WriteFile(config1Path, []byte(config1), 0644); err != nil {
		t.Fatalf("Failed to write config1: %v", err)
	}

	// Create second config file
	config2 := `
id: config2
connectors:
  source: 
    type: filesystem
    base_path: ../../raw
    partition: daily
  destination: 
    type: filesystem
    base_path: ../../ingested
    partition: daily
data_sources:
  domain: test
  name: products
  source: 
    connector: source
    fqn_resource: products.csv
    is_cdc: false
    primary_key: [id]
    timestamp_field: updated_at
  destination:
    connector: destination
    ordering: [id asc]
  trigger:
    cron: "0 0 * * *"
    random_offset: false
  validate:
    not_null: [id, name]
    unique: [id]
  fields:
    - label: id
      data_type: string
    - label: name
      data_type: string
    - label: price
      data_type: float
`

	config2Path := filepath.Join(tempDir, "nested", "config2.yaml")
	os.Mkdir(filepath.Join(tempDir, "nested"), 0755)
	if err := os.WriteFile(config2Path, []byte(config2), 0644); err != nil {
		t.Fatalf("Failed to write config2: %v", err)
	}

	// Test parsing directory
	configs, err := ParseConfigDirectory(tempDir)
	if err != nil {
		t.Fatalf("ParseConfigDirectory() error = %v", err)
	}

	// Verify configs parsed
	if len(*configs) != 2 {
		t.Errorf("Expected 2 configs, got %d", len(*configs))
	}

	config3Path := filepath.Join(tempDir, "nested", "config3.yaml")
	if err := os.WriteFile(config3Path, []byte(config2), 0644); err != nil {
		t.Fatalf("Failed to write config3: %v", err)
	}

	// Test duplicate config error
	_, err = ParseConfigDirectory(tempDir)
	if err == nil {
		t.Error("ParseConfigDirectory() with duplicate config ids")
	}

	// Test with non-existent directory
	_, err = ParseConfigDirectory("/non/existent/directory")
	if err == nil {
		t.Error("ParseConfigDirectory() with non-existent directory should return error")
	}

	// Test with directory containing no YAML files
	emptyDir, err := os.MkdirTemp("", "empty-config-dir-*")
	if err != nil {
		t.Fatalf("Failed to create empty directory: %v", err)
	}
	defer os.RemoveAll(emptyDir)

	_, err = ParseConfigDirectory(emptyDir)
	if err == nil {
		t.Error("ParseConfigDirectory() with empty directory should return error")
	}
}
