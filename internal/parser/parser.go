package parser

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config represents the global configuration
type (
	Configs []Config
	Config  struct {
		Connectors map[string]any `yaml:"connectors"`
		DataSource DataSource     `yaml:"data_source"`
	}
)

// DataSource represents a data source configuration
type DataSource struct {
	Domain      string            `yaml:"domain"`
	Name        string            `yaml:"name"`
	Source      SourceConfig      `yaml:"source"`
	Destination DestinationConfig `yaml:"destination"`
	Schedule    ScheduleConfig    `yaml:"schedule"`
	Validate    ValidationConfig  `yaml:"validate"`
	Fields      []FieldConfig     `yaml:"fields"`
}

// SourceConfig represents the source configuration
type SourceConfig struct {
	Connector      string   `yaml:"connector"`
	FQNResource    string   `yaml:"fqn_resource"`
	IsCDC          bool     `yaml:"is_cdc"`
	PrimaryKey     []string `yaml:"primary_key"`
	TimestampField string   `yaml:"timestamp_field"`
}

// DestinationConfig represents the destination configuration
type DestinationConfig struct {
	Connector string   `yaml:"connector"`
	Ordering  []string `yaml:"ordering"`
}

// ScheduleConfig represents the schedule configuration
type ScheduleConfig struct {
	Cron         string `yaml:"cron"`
	RandomOffset bool   `yaml:"random_offset"`
}

// ValidationConfig represents the validation configuration
type ValidationConfig struct {
	NotNull []string `yaml:"not_null"`
	Unique  []string `yaml:"unique"`
}

// FieldConfig represents a field configuration
type FieldConfig struct {
	Label    string `yaml:"label"`
	DataType string `yaml:"data_type"`
}

// ParseConfigFile parses a YAML config file into a Config struct
func ParseConfigFile(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := &Config{}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file: %w", err)
	}

	return config, nil
}

// ParseConfigDirectory parses all YAML files in a directory into a Config struct
func ParseConfigDirectory(dirPath string) (*Configs, error) {
	var configs Configs

	// Track the number of files processed
	filesProcessed := 0

	filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		return nil
	})
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config directory: %w", err)
	}

	// Process each YAML file in the directory
	for _, entry := range entries {
		if entry.IsDir() {
			continue // Skip subdirectories
		}

		filename := entry.Name()
		ext := filepath.Ext(filename)
		if ext != ".yaml" && ext != ".yml" {
			continue // Skip non-YAML files
		}

		filePath := filepath.Join(dirPath, filename)
		slog.Info("Processing config file", "file", filePath)

		// Parse the individual config file
		config, err := ParseConfigFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to parse config file %s: %w", filePath, err)
		}

		configs = append(configs, *config)
		filesProcessed++
	}

	if filesProcessed == 0 {
		return nil, fmt.Errorf("no YAML config files found in directory: %s", dirPath)
	}

	slog.Info(
		"Processed config files",
		"count",
		filesProcessed,
		"configs",
		len(configs),
	)

	return &configs, nil
}
