package filesystem

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	// Create a temp directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fc := FilesystemConnector{tempDir}

	// Verify struct fields
	if fc.BasePath != tempDir {
		t.Errorf("BasePath = %v, want %v", fc.BasePath, tempDir)
	}

	// Test with non-existent directory
	_, err = New("/non/existent/directory")
	if err == nil {
		t.Error("New() with non-existent directory should return error")
	}
}

func TestReadWriteCSV(t *testing.T) {
	// Create a temp directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create connector
	fc, err := New(tempDir)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	// Test data
	testData := []map[string]any{
		{"id": "1", "name": "Alice", "age": "30"},
		{"id": "2", "name": "Bob", "age": "25"},
	}

	// Test writing CSV
	err = fc.Write("raw/test.csv", testData)
	if err != nil {
		t.Errorf("Write() error = %v, want nil", err)
	}

	// Verify file exists
	filePath := filepath.Join(tempDir, "raw/test.csv")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("File %s was not created", filePath)
	}

	// Test reading CSV
	readData, err := fc.Read("raw/test.csv")
	if err != nil {
		t.Errorf("Read() error = %v, want nil", err)
	}

	// Verify data integrity
	if len(readData) != len(testData) {
		t.Errorf("Read() returned %d records, want %d", len(readData), len(testData))
	}

	// Basic check that all expected fields are present
	for i, record := range readData {
		for key := range testData[i] {
			if _, exists := record[key]; !exists {
				t.Errorf("Read() record %d missing field %s", i, key)
			}
		}
	}

	// Test reading non-existent file
	_, err = fc.Read("non-existent.csv")
	if err == nil {
		t.Error("Read() with non-existent file should return error")
	}

	// Test unsupported format
	_, err = fc.Read("test.unsupported")
	if err == nil {
		t.Error("Read() with unsupported format should return error")
	}

	// Test writing unsupported format
	err = fc.Write("test.unsupported", testData)
	if err == nil {
		t.Error("Write() with unsupported format should return error")
	}
}

func TestReadWriteJSON(t *testing.T) {
	// Create a temp directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create connector
	fc, err := New(tempDir)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	// Test data
	testData := []map[string]any{
		{"id": "1", "name": "Alice", "age": 30},
		{"id": "2", "name": "Bob", "age": 25},
	}

	// Test writing JSON
	err = fc.Write("raw/test.json", testData)
	if err != nil {
		t.Errorf("Write() error = %v, want nil", err)
	}

	// Verify file exists
	filePath := filepath.Join(tempDir, "raw/test.json")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("File %s was not created", filePath)
	}

	// Test reading JSON
	readData, err := fc.Read("raw/test.json")
	if err != nil {
		t.Errorf("Read() error = %v, want nil", err)
	}

	// Verify data integrity
	if len(readData) != len(testData) {
		t.Errorf("Read() returned %d records, want %d", len(readData), len(testData))
	}

	// Basic check that all expected fields are present and values match
	for i, record := range readData {
		for key, expectedVal := range testData[i] {
			actualVal, exists := record[key]
			if !exists {
				t.Errorf("Read() record %d missing field %s", i, key)
				continue
			}
			
			// For numeric values, JSON unmarshaling will convert to float64
			if key == "age" {
				// Convert expected int to float64 for comparison
				expectedFloat, ok := expectedVal.(float64)
				if !ok {
					expectedFloat = float64(expectedVal.(int))
				}
				actualFloat, ok := actualVal.(float64)
				if !ok {
					t.Errorf("Read() record %d field %s is not a number", i, key)
					continue
				}
				if expectedFloat != actualFloat {
					t.Errorf("Read() record %d field %s = %v, want %v", i, key, actualVal, expectedVal)
				}
			} else if expectedVal != actualVal {
				t.Errorf("Read() record %d field %s = %v, want %v", i, key, actualVal, expectedVal)
			}
		}
	}
}

func TestReadWriteJSONL(t *testing.T) {
	// Create a temp directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create connector
	fc, err := New(tempDir)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	// Test data
	testData := []map[string]any{
		{"id": "1", "name": "Alice", "age": 30},
		{"id": "2", "name": "Bob", "age": 25},
	}

	// Test writing JSONL
	err = fc.Write("raw/test.jsonl", testData)
	if err != nil {
		t.Errorf("Write() error = %v, want nil", err)
	}

	// Verify file exists
	filePath := filepath.Join(tempDir, "raw/test.jsonl")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("File %s was not created", filePath)
	}

	// Verify file contents (each line should be a valid JSON object)
	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		t.Errorf("Failed to read JSONL file: %v", err)
	}
	
	lines := 0
	for _, line := range fileContent {
		if line == '\n' {
			lines++
		}
	}
	
	if lines != len(testData) {
		t.Errorf("JSONL file has %d lines, want %d", lines, len(testData))
	}

	// Test reading JSONL
	readData, err := fc.Read("raw/test.jsonl")
	if err != nil {
		t.Errorf("Read() error = %v, want nil", err)
	}

	// Verify data integrity
	if len(readData) != len(testData) {
		t.Errorf("Read() returned %d records, want %d", len(readData), len(testData))
	}

	// Basic check that all expected fields are present
	for i, record := range readData {
		for key, expectedVal := range testData[i] {
			actualVal, exists := record[key]
			if !exists {
				t.Errorf("Read() record %d missing field %s", i, key)
				continue
			}
			
			// For numeric values, JSON unmarshaling will convert to float64
			if key == "age" {
				// Convert expected int to float64 for comparison
				expectedFloat, ok := expectedVal.(float64)
				if !ok {
					expectedFloat = float64(expectedVal.(int))
				}
				actualFloat, ok := actualVal.(float64)
				if !ok {
					t.Errorf("Read() record %d field %s is not a number", i, key)
					continue
				}
				if expectedFloat != actualFloat {
					t.Errorf("Read() record %d field %s = %v, want %v", i, key, actualVal, expectedVal)
				}
			} else if expectedVal != actualVal {
				t.Errorf("Read() record %d field %s = %v, want %v", i, key, actualVal, expectedVal)
			}
		}
	}
}