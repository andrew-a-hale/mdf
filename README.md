# Metadata-Driven Framework for Data Ingestion

This project implements a metadata-driven framework for data ingestion based on
YAML configuration files.

## Features

- Configuration-driven data ingestion
- Modular connector architecture
- Scheduled execution with cron expressions
- Data validation (not null, unique constraints)

## Project Structure

```text
├── configs/          # Configuration files
├── internal/         # Internal packages
│   ├── connectors/   # Data source/destination connectors
│   ├── eventlog/     # Event persistence
│   ├── executor/     # Job execution
│   ├── notifier/     # Notifications
│   ├── parser/       # Configuration parsing
│   ├── scheduler/    # Job scheduling
│   ├── triggerer/    # Event Triggered scheduling
│   └── validator/    # Data validation
├── pkg/              # Public packages
├── README.md         # Project documentation
├── main.go           # Entrypoint
└── go.mod            # Go module file
```

## Getting Started

### Prerequisites

- Go 1.23 or higher

### Installation

```bash
git clone https://github.com/andrew-a-hale/mdf.git
cd mdf
go mod tidy
go install
```

## Configuration

See `configs/example.yaml` for an example configuration file.
