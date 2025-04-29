package parser

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"

	"gopkg.in/yaml.v3"
)

// Config represents the global configuration
type (
	Configs []Config
	Config  struct {
		Id         string         `yaml:"id"`
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
	Trigger     TriggerConfig     `yaml:"trigger"`
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

// TriggerConfig represents the schedule configuration
type TriggerConfig struct {
	Cron         string `yaml:"cron,omitempty"`
	EventId      string `yaml:"event,omitempty"`
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

	var seen []string
	err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		ext := filepath.Ext(d.Name())
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}

		slog.Info("Processing config file", "file", path)

		// Parse the individual config file
		config, parseErr := ParseConfigFile(path)
		if parseErr != nil {
			return fmt.Errorf("failed to parse config file %s: %w", path, parseErr)
		}

		if slices.Contains(seen, config.Id) {
			return fmt.Errorf("failed to parse config file %s: config with same id already exists", path)
		}
		seen = append(seen, config.Id)

		configs = append(configs, *config)
		filesProcessed++
		return nil
	})
	if err != nil {
		return nil, err
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

// ParseConfigRemoteDirectory parse configs from blob storage
func ParseConfigRemoteDirectory(dirPath string) (*Configs, error) {
	return &Configs{}, nil
}
