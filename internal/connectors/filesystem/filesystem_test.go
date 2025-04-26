package filesystem

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/andrew-a-hale/mdf/internal/parser"
)

func TestNew(t *testing.T) {
	// Create a temp directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tests := []struct {
		name        string
		basePath    string
		partition   string
		expectError bool
		fields      []parser.FieldConfig
	}{
		{
			name:        "Valid Configuration",
			basePath:    tempDir,
			partition:   "daily",
			expectError: false,
			fields:      nil,
		},
		{
			name:        "Invalid Base Path",
			basePath:    "/non/existent/directory",
			partition:   "daily",
			expectError: true,
			fields:      nil,
		},
		{
			name:        "Empty Partition",
			basePath:    tempDir,
			partition:   "",
			expectError: true,
			fields:      nil,
		},
		{
			name:        "Invalid Partition Type",
			basePath:    tempDir,
			partition:   "invalid",
			expectError: true,
			fields:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc, err := New(tt.basePath, tt.partition, tt.fields)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Failed to create connector: %v", err)
				}

				// Verify struct fields
				if fc.BasePath != tt.basePath {
					t.Errorf("BasePath = %v, want %v", fc.BasePath, tt.basePath)
				}

				if fc.Partition != tt.partition {
					t.Errorf("Partition = %v, want %v", fc.Partition, tt.partition)
				}

				// Check that ProcessedFiles map is initialized
				if fc.ProcessedFiles == nil {
					t.Error("ProcessedFiles map should be initialized")
				}

				// Check that db is initialized
				if fc.db == nil {
					t.Error("DuckDB connection should be initialized")
				}

				// Close the db connection
				if err := fc.Close(); err != nil {
					t.Errorf("Failed to close db connection: %v", err)
				}
			}
		})
	}
}

func TestWrite(t *testing.T) {
	// Create a temp directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem-test-write-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fields := []parser.FieldConfig{
		{Label: "id", DataType: "int"},
		{Label: "name", DataType: "string"},
		{Label: "age", DataType: "int"},
	}

	// Create connector
	fc, err := New(tempDir, "daily", fields)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}
	defer fc.Close()

	// Test writing empty data
	if err := fc.Write([]map[string]any{}); err != nil {
		t.Errorf("Write with empty data should not error: %v", err)
	}

	// Test writing actual data
	testData := []map[string]any{
		{
			"id":   "1",
			"name": "Test User",
			"age":  30,
		},
		{
			"id":   "2",
			"name": "Another User",
			"age":  25,
		},
	}

	if err := fc.Write(testData); err != nil {
		t.Errorf("Failed to write test data: %v", err)
	}

	// Verify that a partition directory was created
	// We can't predict the exact directory name as it depends on current date,
	// so just check that there's at least one subdirectory
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	foundPartition := false
	for _, entry := range entries {
		if entry.IsDir() {
			foundPartition = true
			// Check that at least one .parquet file exists in the partition directory
			partitionDir := filepath.Join(tempDir, entry.Name())
			partitionFiles, err := os.ReadDir(partitionDir)
			if err != nil {
				t.Fatalf("Failed to read partition directory: %v", err)
			}

			foundParquet := false
			for _, file := range partitionFiles {
				if !file.IsDir() && filepath.Ext(file.Name()) == ".parquet" {
					foundParquet = true
					break
				}
			}

			if !foundParquet {
				t.Errorf("No .parquet file found in partition directory %s", partitionDir)
			}
			break
		}
	}

	if !foundPartition {
		t.Error("No partition directory created")
	}
}

func TestIsSupportedFileType(t *testing.T) {
	tests := []struct {
		ext      string
		expected bool
	}{
		{".csv", true},
		{".CSV", true},
		{".json", true},
		{".jsonl", true},
		{".parquet", true},
		{".txt", false},
		{".pdf", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.ext, func(t *testing.T) {
			if got := isSupportedFileType(tt.ext); got != tt.expected {
				t.Errorf("isSupportedFileType(%q) = %v, want %v", tt.ext, got, tt.expected)
			}
		})
	}
}

func TestRead(t *testing.T) {
	// Create a temp directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem-test-read-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	subDir := filepath.Join(tempDir, "raw")
	err = os.MkdirAll(subDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	fields := []parser.FieldConfig{
		{Label: "id", DataType: "int"},
		{Label: "name", DataType: "string"},
		{Label: "age", DataType: "int"},
	}

	// Create test files
	csvFile := filepath.Join(subDir, "csv", "test.csv")
	writeCsv := func(csvFile string) {
		err = os.MkdirAll(filepath.Join(subDir, "csv"), 0755)
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		csvData := "id,name,age\n1,Test User,30\n2,Another User,25"
		if err := os.WriteFile(csvFile, []byte(csvData), 0644); err != nil {
			t.Fatalf("Failed to create test CSV file: %v", err)
		}
	}

	jsonFile := filepath.Join(subDir, "json", "test.json")
	writeJson := func(jsonFile string) {
		err = os.MkdirAll(filepath.Join(subDir, "json"), 0755)
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		jsonData := `[{"id":"3","name":"JSON User","age":35},{"id":"4","name":"Another JSON User","age":40}]`
		if err := os.WriteFile(jsonFile, []byte(jsonData), 0644); err != nil {
			t.Fatalf("Failed to create test JSON file: %v", err)
		}
	}

	tests := []struct {
		name          string
		basePath      string
		partition     string
		expectError   bool
		expectedItems int
		fileType      string
		writeFunc     func(string)
	}{
		{
			name:          "Read CSV File",
			basePath:      csvFile,
			partition:     "daily",
			expectError:   false,
			expectedItems: 2,
			writeFunc:     writeCsv,
		},
		{
			name:          "Read JSON File",
			basePath:      jsonFile,
			partition:     "daily",
			expectError:   false,
			expectedItems: 2,
			writeFunc:     writeJson,
		},
		{
			name:          "Read Directory",
			basePath:      subDir,
			partition:     "daily",
			expectError:   false,
			expectedItems: 4, // 2 from CSV + 2 from JSON
			writeFunc:     func(string) {},
		},
	}

	// Create a non-existent file test case
	nonExistentFile := filepath.Join(subDir, "nonexistent.csv")

	for _, tt := range tests {
		tt.writeFunc(tt.basePath)

		t.Run(tt.name, func(t *testing.T) {
			fc, err := New(filepath.Dir(tt.basePath), tt.partition, fields)
			if err != nil {
				t.Fatalf("Failed to create connector: %v", err)
			}
			defer fc.Close()

			data, err := fc.Read()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Failed to read data: %v", err)
				}

				if len(data) != tt.expectedItems {
					t.Errorf("Got %d items, want %d", len(data), tt.expectedItems)
				}

				// Verify some data properties
				if len(data) > 0 {
					if _, ok := data[0]["id"]; !ok {
						t.Errorf("Expected 'id' field in result")
					}
					if _, ok := data[0]["name"]; !ok {
						t.Errorf("Expected 'name' field in result")
					}
					if _, ok := data[0]["age"]; !ok {
						t.Errorf("Expected 'age' field in result")
					}
				}
			}

			// Test reading the same file again - ProcessedFiles should skip it
			if !tt.expectError && tt.basePath == subDir {
				data2, err := fc.Read()
				if err != nil {
					t.Fatalf("Failed to read data second time: %v", err)
				}

				if len(data2) != 0 {
					t.Errorf("Expected 0 items on second read (all files processed), got %d", len(data2))
				}
			}
		})
	}

	// Test for non-existent file
	t.Run("Read Non-existent File", func(t *testing.T) {
		// Ensure the file doesn't exist
		os.Remove(nonExistentFile)

		// Attempting to create a connector with non-existent path should fail
		_, err := New(nonExistentFile, "daily", nil)
		if err == nil {
			t.Errorf("Expected error when creating connector with non-existent path but got nil")
		}
	})
}
