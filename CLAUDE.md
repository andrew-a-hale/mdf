# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## Build/Test Commands

- Build: `go install main.go`
- Run tests: `go test ./...`
- Run single test:
  `go test -v github.com/andy/mdf/internal/path/to/package -run TestName`
- Build and test: `make build`

## Code Style Guidelines

- Go version: 1.21+
- Error handling: Return errors with context using
  `fmt.Errorf("failed to X: %w", err)`
- Logging: Use structured logging with `log/slog` package
- Naming: Use CamelCase for exported types/fields, camelCase for private
- Comments: Document exported functions with godoc-style comments
- Imports: Group standard library, then external, then internal packages
- Prefer composition over inheritance
- Return early from functions instead of nesting conditionals
- For key data types, include validation in constructors (use New() functions)
- Use interfaces for connector implementations
- Unit test each package, following table-driven test patterns

