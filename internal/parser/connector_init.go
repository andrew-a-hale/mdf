package parser

import (
	"fmt"
	"log/slog"

	"github.com/andy/mdf/internal/connectors"
	"github.com/andy/mdf/internal/connectors/filesystem"
)

// InitializeConnectors initializes all connectors in the config
func (c *Config) InitializeConnectors() error {
	// Initialize the map if it's nil
	if c.InitializedConnectors == nil {
		c.InitializedConnectors = make(map[string]connectors.Connector)
	}

	// Initialize each connector
	for name, config := range c.Connectors {
		// Skip already initialized connectors
		if _, exists := c.InitializedConnectors[name]; exists {
			continue
		}

		// Convert config to map[string]any if it's not already
		configMap, ok := config.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid connector config for %s: expected map[string]any", name)
		}

		// Extract connector type
		connType, ok := configMap["type"].(string)
		if !ok {
			return fmt.Errorf("connector %s is missing 'type' configuration", name)
		}

		// Create connector based on type
		var connector connectors.Connector
		var err error

		switch connType {
		case "filesystem":
			connector, err = initializeFilesystemConnector(configMap)
		default:
			return fmt.Errorf("unsupported connector type: %s", connType)
		}

		if err != nil {
			return fmt.Errorf("failed to initialize connector %s: %w", name, err)
		}

		// Store initialized connector
		c.InitializedConnectors[name] = connector
		slog.Info("Initialized connector", "name", name, "type", connType)
	}

	return nil
}

// initializeFilesystemConnector initializes a filesystem connector
func initializeFilesystemConnector(config map[string]any) (connectors.Connector, error) {
	// Extract base path from config
	basePath, ok := config["base_path"].(string)
	if !ok {
		return nil, fmt.Errorf("filesystem connector is missing 'base_path' configuration")
	}

	// Create connector
	return filesystem.New(basePath)
}
