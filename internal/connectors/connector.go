package connectors

const (
	FILESYSTEM = "filesystem"
)

// Connector defines the interface for data connectors
type Connector interface {
	// Read reads data from a resource
	Read() ([]map[string]any, error)

	// Write writes data to a resource
	Write([]map[string]any) error

	// Close closes the connector
	Close() error
}
