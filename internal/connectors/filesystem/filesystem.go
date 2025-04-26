package filesystem

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/andrew-a-hale/mdf/internal/parser"
	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb" // Import for side effect of registering driver
)

// FilesystemConnector represents a filesystem connector using DuckDB as the engine
type FilesystemConnector struct {
	BasePath       string
	Partition      string
	db             *sql.DB
	ProcessedFiles map[string]bool
	Fields         []parser.FieldConfig
}

// New creates a new filesystem connector
func New(basePath string, partition string, fields []parser.FieldConfig) (*FilesystemConnector, error) {
	// Ensure the base path exists
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		slog.Error("Base path does not exist", "path", basePath)
		return nil, fmt.Errorf("base path does not exist: %s", basePath)
	}

	// Validate that partition is provided and not empty
	if partition == "" {
		slog.Error("Partition is required", "partition", partition)
		return nil, fmt.Errorf("partition is required")
	}

	// Validate partition type
	switch partition {
	case "daily", "hourly", "monthly":
		// Valid partition types
	default:
		slog.Error("Invalid partition type", "partition", partition)
		return nil, fmt.Errorf("invalid partition type: %s, must be one of: daily, hourly, monthly", partition)
	}

	// Initialize DuckDB in-memory database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		slog.Error("Failed to initialize DuckDB", "error", err)
		return nil, fmt.Errorf("failed to initialize DuckDB: %w", err)
	}

	slog.Info("Initialized filesystem connector", "basePath", basePath, "partition", partition)
	return &FilesystemConnector{
		BasePath:       basePath,
		Partition:      partition,
		db:             db,
		ProcessedFiles: make(map[string]bool),
		Fields:         fields,
	}, nil
}

// GetPartition returns the partition type
func (fc *FilesystemConnector) GetPartition() string {
	return fc.Partition
}

// Close closes the database connection
func (fc *FilesystemConnector) Close() error {
	if fc.db != nil {
		return fc.db.Close()
	}
	return nil
}

// Read reads data from a file or directory using DuckDB
func (fc *FilesystemConnector) Read() ([]map[string]any, error) {
	fileInfo, err := os.Stat(fc.BasePath)
	if err != nil {
		slog.Error("Resource not found", "resource", fc.BasePath)
		return nil, fmt.Errorf("resource not found: %s", fc.BasePath)
	}

	// If it's a directory, process it as a directory resource
	if fileInfo.IsDir() {
		return fc.readFromDirectory()
	}

	// It's a single file, process it directly
	ext := filepath.Ext(fc.BasePath)
	slog.Info("Reading from file", "path", fc.BasePath, "format", ext)

	return fc.readFile(fc.BasePath)
}

// readFromDirectory reads all files from a directory and combines the results using DuckDB
func (fc *FilesystemConnector) readFromDirectory() ([]map[string]any, error) {
	var allFiles []string
	err := filepath.Walk(fc.BasePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			// TODO: Check if the file has already been processed
			ext := filepath.Ext(path)
			if isSupportedFileType(ext) {
				allFiles = append(allFiles, path)
			}
		}
		return nil
	})
	if err != nil {
		slog.Error("Failed to walk directory", "dir", fc.BasePath, "error", err)
		return nil, fmt.Errorf("failed to walk directory: %w", err)
	}

	if len(allFiles) == 0 {
		slog.Info("No files to process in directory", "dir", fc.BasePath)
		return []map[string]any{}, nil
	}

	// Use DuckDB to read all files at once
	result, err := fc.readFiles(allFiles)
	if err != nil {
		return nil, err
	}

	// Mark files as processed
	for _, filePath := range allFiles {
		fc.ProcessedFiles[filePath] = true
	}

	slog.Info("Read from directory", "dir", fc.BasePath, "files", len(allFiles), "records", len(result))
	return result, nil
}

// readFiles reads multiple files using DuckDB
func (fc *FilesystemConnector) readFiles(filePaths []string) ([]map[string]any, error) {
	if len(filePaths) == 0 {
		return []map[string]any{}, nil
	}

	// Create a temporary view that unifies all files
	viewName := fmt.Sprintf("temp_view_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))

	// Group files by extension
	filesByExt := make(map[string][]string)
	for _, path := range filePaths {
		ext := strings.ToLower(filepath.Ext(path))
		filesByExt[ext] = append(filesByExt[ext], path)
	}

	// Process each file type and union the results
	var unionQueries []string

	for ext, files := range filesByExt {
		var format string
		switch ext {
		case ".csv":
			format = "csv"
		case ".json":
			format = "json"
		case ".jsonl":
			format = "jsonline"
		case ".parquet":
			format = "parquet"
		default:
			continue
		}

		// Create temp view for each file type
		for i, file := range files {
			subViewName := fmt.Sprintf("%s_%s_%d", viewName, format, i)
			readQuery := fmt.Sprintf("CREATE TEMPORARY VIEW %s AS SELECT * FROM read_%s_auto('%s')",
				subViewName, format, file)

			_, err := fc.db.Exec(readQuery)
			if err != nil {
				slog.Error("Failed to create temporary view", "query", readQuery, "error", err)
				return nil, fmt.Errorf("failed to create temporary view: %w", err)
			}

			unionQueries = append(unionQueries, fmt.Sprintf("SELECT * FROM %s", subViewName))
		}
	}

	if len(unionQueries) == 0 {
		return []map[string]any{}, nil
	}

	// Create a unified view with all data
	unifiedQuery := fmt.Sprintf("CREATE TEMPORARY VIEW %s AS %s", viewName, strings.Join(unionQueries, " UNION ALL "))
	_, err := fc.db.Exec(unifiedQuery)
	if err != nil {
		slog.Error("Failed to create unified view", "query", unifiedQuery, "error", err)
		return nil, fmt.Errorf("failed to create unified view: %w", err)
	}

	// Query the unified view
	return fc.queryView(viewName)
}

// readFile reads a single file using DuckDB
func (fc *FilesystemConnector) readFile(filePath string) ([]map[string]any, error) {
	ext := strings.ToLower(filepath.Ext(filePath))

	if !isSupportedFileType(ext) {
		slog.Error("Unsupported file format", "format", ext, "file", filePath)
		return nil, fmt.Errorf("unsupported file format: %s", ext)
	}

	var format string
	switch ext {
	case ".csv":
		format = "csv"
	case ".json":
		format = "json"
	case ".jsonl":
		format = "jsonline"
	case ".parquet":
		format = "parquet"
	}

	// Create a temporary view for the file
	viewName := fmt.Sprintf("temp_view_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
	readQuery := fmt.Sprintf("CREATE TEMPORARY VIEW %s AS SELECT * FROM read_%s_auto('%s')",
		viewName, format, filePath)

	_, err := fc.db.Exec(readQuery)
	if err != nil {
		slog.Error("Failed to create temporary view", "query", readQuery, "error", err)
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	return fc.queryView(viewName)
}

// queryView executes a query against a view and returns the results as a slice of maps
func (fc *FilesystemConnector) queryView(viewName string) ([]map[string]any, error) {
	// Query the view
	rows, err := fc.db.Query(fmt.Sprintf("SELECT * FROM %s", viewName))
	if err != nil {
		slog.Error("Failed to query view", "view", viewName, "error", err)
		return nil, fmt.Errorf("failed to query view: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		slog.Error("Failed to get column names", "error", err)
		return nil, fmt.Errorf("failed to get column names: %w", err)
	}

	// Prepare for scan
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Process rows
	var result []map[string]any
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			slog.Error("Failed to scan row", "error", err)
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create map for this row
		entry := make(map[string]any)
		for i, col := range columns {
			// Convert DuckDB types to appropriate Go types
			val := values[i]
			if val != nil {
				entry[col] = val
			} else {
				entry[col] = nil
			}
		}
		result = append(result, entry)
	}

	if err := rows.Err(); err != nil {
		slog.Error("Error iterating rows", "error", err)
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// Drop the temporary view
	_, err = fc.db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName))
	if err != nil {
		slog.Warn("Failed to drop temporary view", "view", viewName, "error", err)
	}

	return result, nil
}

// Write writes data to a partitioned directory using DuckDB
func (fc *FilesystemConnector) Write(data []map[string]any) error {
	if len(data) == 0 {
		slog.Info("No data to write")
		return nil
	}

	// Create partition directory name based on current time
	var partitionName string
	now := time.Now().UTC()

	switch fc.Partition {
	case "hourly":
		partitionName = now.Format("2006-01-02-15")
	case "daily":
		partitionName = now.Format("2006-01-02")
	case "monthly":
		partitionName = now.Format("2006-01")
	default:
		return fmt.Errorf("invalid partition type: %s", fc.Partition)
	}

	// Generate a unique filename for this write
	resourceFile := fmt.Sprintf("%s.parquet", uuid.New().String())

	// Create the full path including the partition
	partitionDir := filepath.Join(fc.BasePath, partitionName)
	partitionPath := filepath.Join(partitionDir, resourceFile)

	// Ensure the partition directory exists
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		slog.Error("Failed to create partition directory", "dir", partitionDir, "error", err)
		return fmt.Errorf("failed to create partition directory: %w", err)
	}

	// Create a temporary table
	tx, err := fc.db.Begin()
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer tx.Commit()

	tableName := fmt.Sprintf("temp_table_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
	var cols []string
	for _, field := range fc.Fields {
		cols = append(cols, fmt.Sprintf("%s %s", field.Label, field.DataType))
	}
	createTableSql := fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s)", tableName, strings.Join(cols, ","))
	_, err = tx.Exec(createTableSql)
	if err != nil {
		slog.Error("Failed to create table", "error", err)
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert data into table
	clear(cols)
	for _, field := range fc.Fields {
		cols = append(cols, field.Label)
	}
	insertSql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(cols, ","),
		strings.Repeat("?", len(cols)),
	)

	for _, row := range data {
		var fields []any
		for _, field := range row {
			fields = append(fields, field)
		}
		tx.Exec(insertSql, fields)
	}

	// Write the data to Parquet file using DuckDB's COPY statement
	copySQL := fmt.Sprintf("COPY (SELECT * FROM %s) TO '%s' (FORMAT PARQUET)", tableName, partitionPath)
	_, err = tx.Exec(copySQL)
	if err != nil {
		slog.Error("Failed to write data to Parquet file", "path", partitionPath, "error", err)
		return fmt.Errorf("failed to write data to Parquet file: %w", err)
	}

	slog.Info("Wrote data to partitioned file", "path", partitionPath, "records", len(data), "partition", partitionName)
	return nil
}

// isSupportedFileType checks if the file extension is supported
func isSupportedFileType(ext string) bool {
	ext = strings.ToLower(ext)
	return ext == ".csv" || ext == ".json" || ext == ".jsonl" || ext == ".parquet"
}
