package validator

import (
	"fmt"
	"log/slog"

	"github.com/andrew-a-hale/mdf/internal/parser"
)

// Validator handles data validation
type Validator struct {
	config parser.ValidationConfig
}

// New creates a new validator instance
func New(config parser.ValidationConfig) *Validator {
	return &Validator{
		config: config,
	}
}

// Validate validates a dataset against the validation rules
func (v *Validator) Validate(data []map[string]any) error {
	// Validate not null fields
	err := v.validateNotNull(data)
	if err != nil {
		return err
	}

	// Validate unique fields
	err = v.validateUnique(data)
	if err != nil {
		return err
	}

	return nil
}

// validateNotNull checks that fields are not null
func (v *Validator) validateNotNull(data []map[string]any) error {
	for _, field := range v.config.NotNull {
		for i, row := range data {
			val, exists := row[field]
			if !exists || val == nil {
				slog.Error("Not null validation failed",
					"field", field,
					"row", i)
				return fmt.Errorf("validation error: field '%s' cannot be null (row %d)", field, i)
			}
		}
	}
	return nil
}

// validateUnique checks that fields are unique
func (v *Validator) validateUnique(data []map[string]any) error {
	for _, field := range v.config.Unique {
		values := make(map[any]bool)
		for i, row := range data {
			val, exists := row[field]
			if !exists {
				continue
			}

			if _, found := values[val]; found {
				slog.Error("Unique validation failed",
					"field", field,
					"row", i,
					"value", val)
				return fmt.Errorf("validation error: field '%s' must be unique (row %d)", field, i)
			}

			values[val] = true
		}
	}
	return nil
}
