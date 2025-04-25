package connectors

// Connector defines the interface for data connectors
type Connector interface {
	// Read reads data from a resource
	Read(resource string) ([]map[string]any, error)
	
	// Write writes data to a resource
	Write(resource string, data []map[string]any) error
}