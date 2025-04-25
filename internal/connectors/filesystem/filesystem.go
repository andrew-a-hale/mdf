package filesystem

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/writer"
)

// FilesystemConnector represents a filesystem connector
type FilesystemConnector struct {
	BasePath string
}

// New creates a new filesystem connector
func New(basePath string) (*FilesystemConnector, error) {
	// Ensure the base path exists
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		slog.Error("Base path does not exist", "path", basePath)
		return nil, fmt.Errorf("base path does not exist: %s", basePath)
	}

	slog.Info("Initialized filesystem connector", "basePath", basePath)
	return &FilesystemConnector{BasePath: basePath}, nil
}

// Read reads data from a file
func (fc *FilesystemConnector) Read(resource string) ([]map[string]any, error) {
	filePath := filepath.Join(fc.BasePath, resource)
	ext := filepath.Ext(filePath)

	slog.Info("Reading from file", "path", filePath, "format", ext)

	switch strings.ToLower(ext) {
	case ".csv":
		return fc.readCSV(filePath)
	case ".json":
		return fc.readJSON(filePath)
	case ".jsonl":
		return fc.readJSONL(filePath)
	case ".parquet":
		return fc.readParquet(filePath)
	default:
		slog.Error("Unsupported file format", "format", ext, "file", filePath)
		return nil, fmt.Errorf("unsupported file format: %s", ext)
	}
}

// Write writes data to a file
func (fc *FilesystemConnector) Write(resource string, data []map[string]any) error {
	filePath := filepath.Join(fc.BasePath, resource)
	ext := filepath.Ext(filePath)

	slog.Info("Writing to file", "path", filePath, "format", ext, "records", len(data))

	// Ensure the directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("Failed to create directory", "dir", dir, "error", err)
		return fmt.Errorf("failed to create directory: %w", err)
	}

	switch strings.ToLower(ext) {
	case ".csv":
		return fc.writeCSV(filePath, data)
	case ".json":
		return fc.writeJSON(filePath, data)
	case ".jsonl":
		return fc.writeJSONL(filePath, data)
	case ".parquet":
		return fc.writeParquet(filePath, data)
	default:
		slog.Error("Unsupported file format", "format", ext, "file", filePath)
		return fmt.Errorf("unsupported file format: %s", ext)
	}
}

// readCSV reads a CSV file and returns the data as a slice of maps
func (fc *FilesystemConnector) readCSV(filePath string) ([]map[string]any, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header row
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	var result []map[string]any

	// Read data rows
	for {
		row, err := reader.Read()
		if err != nil {
			break // End of file or error
		}

		// Create a map for this row
		rowMap := make(map[string]any)
		for i, field := range row {
			if i < len(header) {
				rowMap[header[i]] = field
			}
		}

		result = append(result, rowMap)
	}

	return result, nil
}

// writeCSV writes data to a CSV file
func (fc *FilesystemConnector) writeCSV(filePath string, data []map[string]any) error {
	if len(data) == 0 {
		// Create an empty file
		return os.WriteFile(filePath, []byte{}, 0644)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Extract headers from the first row
	headers := make([]string, 0, len(data[0]))
	for key := range data[0] {
		headers = append(headers, key)
	}

	// Write header row
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	// Write data rows
	for _, row := range data {
		record := make([]string, len(headers))
		for i, header := range headers {
			val, exists := row[header]
			if exists {
				record[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

// readJSON reads a JSON file and returns the data as a slice of maps
func (fc *FilesystemConnector) readJSON(filePath string) ([]map[string]any, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var data []map[string]any
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %w", err)
	}

	return data, nil
}

// writeJSON writes data to a JSON file
func (fc *FilesystemConnector) writeJSON(filePath string, data []map[string]any) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	return nil
}

// readJSONL reads a JSONL file and returns the data as a slice of maps
func (fc *FilesystemConnector) readJSONL(filePath string) ([]map[string]any, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var data []map[string]any
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("failed to decode JSON at line %d: %w", lineNum, err)
		}
		data = append(data, record)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading JSONL file: %w", err)
	}

	return data, nil
}

// writeJSONL writes data to a JSONL file
func (fc *FilesystemConnector) writeJSONL(filePath string, data []map[string]any) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, record := range data {
		jsonBytes, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to encode JSON record: %w", err)
		}

		if _, err := writer.Write(jsonBytes); err != nil {
			return fmt.Errorf("failed to write JSON record: %w", err)
		}
		if _, err := writer.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
	}

	return nil
}

// readParquet reads a Parquet file and returns the data as a slice of maps
func (fc *FilesystemConnector) readParquet(filePath string) ([]map[string]any, error) {
	// Use the local file reader from parquet-go
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open Parquet file: %w", err)
	}
	defer fr.Close()

	// Create a parquet reader
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet reader: %w", err)
	}
	defer pr.ReadStop()

	// Get number of records in the file
	rowCount := int(pr.GetNumRows())
	if rowCount == 0 {
		return []map[string]any{}, nil
	}

	// Read all records
	data, err := pr.ReadByNumber(rowCount)
	if err != nil {
		return nil, fmt.Errorf("failed to read Parquet records: %w", err)
	}

	// Convert to []map[string]any
	var result []map[string]any
	for _, record := range data {
		m := make(map[string]any)
		err = mapstructure.Decode(record, &m)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Parquet record: %w", err)
		}
		result = append(result, m)
	}

	return result, nil
}

// writeParquet writes data to a Parquet file
func (fc *FilesystemConnector) writeParquet(filePath string, data []map[string]any) error {
	if len(data) == 0 {
		// Create an empty file
		return os.WriteFile(filePath, []byte{}, 0644)
	}

	// Use the local file writer from parquet-go
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file writer: %w", err)
	}
	defer fw.Close()

	// Create a simple schema from the first record
	schemaStr := "message schema {"
	firstRecord := data[0]
	for key, val := range firstRecord {
		var typeName string
		switch val.(type) {
		case string:
			typeName = "UTF8"
		case int:
			typeName = "INT32"
		case int64:
			typeName = "INT64"
		case float32, float64:
			typeName = "DOUBLE"
		case bool:
			typeName = "BOOLEAN"
		default:
			typeName = "UTF8" // Default to string for unknown types
		}
		schemaStr += fmt.Sprintf(" required %s %s;", typeName, key)
	}
	schemaStr += "}"

	// Create a parquet writer
	pw, err := writer.NewParquetWriter(fw, nil, 4)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128MB
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	defer pw.WriteStop()

	// Convert string schema to schema handler
	schemaHandler, err := schema.NewSchemaHandlerFromJSON(schemaStr)
	if err != nil {
		return fmt.Errorf("failed to create schema handler: %w", err)
	}
	pw.SchemaHandler = schemaHandler

	// Write data
	for _, record := range data {
		if err := pw.Write(record); err != nil {
			return fmt.Errorf("failed to write Parquet record: %w", err)
		}
	}

	return nil
}

