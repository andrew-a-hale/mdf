package connectors

import (
	"fmt"
	"log/slog"
)

// ConnectorFactory creates connectors based on configuration
type ConnectorFactory struct{}

// NewConnectorFactory creates a new connector factory
func NewConnectorFactory() *ConnectorFactory {
	return &ConnectorFactory{}
}

// CreateConnector creates a connector from configuration
func (cf *ConnectorFactory) CreateConnector(name string, config map[string]any) (Connector, error) {
	// Extract connector type from config
	connType, ok := config["type"].(string)
	if !ok {
		return nil, fmt.Errorf("connector %s is missing 'type' configuration", name)
	}

	// Log connector creation
	slog.Info("Creating connector", "name", name, "type", connType)

	// Create connector based on type
	switch connType {
	case "filesystem":
		return cf.createFilesystemConnector(config)
		// Add more connector types here as needed
	default:
		return nil, fmt.Errorf("unsupported connector type: %s", connType)
	}
}

// createFilesystemConnector creates a filesystem connector
func (cf *ConnectorFactory) createFilesystemConnector(config map[string]any) (Connector, error) {
	// Extract base path from config
	_, ok := config["base_path"].(string)
	if !ok {
		return nil, fmt.Errorf("filesystem connector is missing 'base_path' configuration")
	}

	// Import and create filesystem connector here
	// This requires circular import prevention - see implementation approach below
	// For now we'll handle it via the actual CreateConnector implementation
	return nil, fmt.Errorf("filesystem connector creation should be handled by the real factory")
}
