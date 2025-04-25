# Metadata-Driven Framework for Data Ingestion

This project implements a metadata-driven framework for data ingestion based on
YAML configuration files.

## Features

- Configuration-driven data ingestion
- Modular connector architecture
- Local filesystem connector included
- Scheduled execution with cron expressions
- Data validation (not null, unique constraints)
- Structured logging with slog

## Project Structure

```text
├── cmd/              # Command-line applications
│   └── ingestion/    # Main ingestion application
├── configs/          # Configuration files
├── internal/         # Internal packages
│   ├── connectors/   # Data source/destination connectors
│   ├── eventlog/     # Event logging
│   ├── executor/     # Job execution
│   ├── notifier/     # Notifications
│   ├── parser/       # Configuration parsing
│   ├── scheduler/    # Job scheduling
│   └── validator/    # Data validation
├── pkg/              # Public packages
├── README.md         # Project documentation
└── go.mod            # Go module file
```

## Getting Started

### Prerequisites

- Go 1.21 or higher

### Installation

```bash
git clone https://github.com/yourusername/metadata-driven-framework.git
cd metadata-driven-framework
go mod tidy
```

### Running

```bash
go run cmd/ingestion/main.go -config configs/example.yaml
```

## Configuration

See `configs/example.yaml` for an example configuration file.

## Components

### Parser

Parses YAML configuration files into Go structs.

### Scheduler

Schedules data ingestion jobs based on cron expressions.

### Executor

Executes data ingestion jobs using the appropriate connectors.

### Validator

Validates data against defined constraints (not null, unique).

### Notifier

Sends notifications on job success or failure.

### Event Logger

Provides structured logging (using slog) for all components for monitoring and debugging.

### Connectors

Provide connectivity to data sources and destinations. Currently supported:

- Local filesystem (CSV files)
