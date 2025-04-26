package validator

import (
	"testing"

	"github.com/andrew-a-hale/mdf/internal/parser"
)

func TestNew(t *testing.T) {
	// Create validation config
	validateConfig := parser.ValidationConfig{
		NotNull: []string{"id", "name"},
		Unique:  []string{"id"},
	}

	// Test validator creation
	v := New(validateConfig)
	if v == nil {
		t.Fatal("New() returned nil")
	}
}

func TestValidate(t *testing.T) {
	// Create validation config
	validateConfig := parser.ValidationConfig{
		NotNull: []string{"id", "name"},
		Unique:  []string{"id"},
	}

	// Create validator
	v := New(validateConfig)

	// Test valid data
	validData := []map[string]any{
		{"id": "1", "name": "Alice", "age": 30},
		{"id": "2", "name": "Bob", "age": 25},
	}

	err := v.Validate(validData)
	if err != nil {
		t.Errorf("Validate() error = %v, want nil", err)
	}

	// Test data with null fields
	nullData := []map[string]any{
		{"id": "1", "name": nil, "age": 30},
		{"id": "2", "name": "Bob", "age": 25},
	}

	err = v.Validate(nullData)
	if err == nil {
		t.Error("Validate() with null fields should return error")
	}

	// Test data with duplicate IDs
	duplicateData := []map[string]any{
		{"id": "1", "name": "Alice", "age": 30},
		{"id": "1", "name": "Bob", "age": 25}, // Duplicate ID
	}

	err = v.Validate(duplicateData)
	if err == nil {
		t.Error("Validate() with duplicate IDs should return error")
	}
}
