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
```

### Running

```bash
go run main.go
```

## Configuration

See `configs/example.yaml` for an example configuration file.

## Components

### Parser

Parses YAML configuration files into Go structs.

### Scheduler

Schedules data ingestion jobs based on cron expressions.

### Trigger

Triggers data ingestion jobs based on events in a queue.

### Executor

Executes data ingestion jobs using the appropriate connectors.

### Validator

Validates data against defined constraints (not null, unique).

Currently supported:

- Not null
- Unique

### Notifier

Sends notifications on job success or failure.

To be added:

- Add Email Notifier
- Add Slack Notifier
- Add Teams Notifier

### Event Logger

Provides structured logging for all components for monitoring and debugging.

### Connectors

Provide connectivity to data sources and destinations.

Currently supported:

- Local filesystem

To be added:

- AWS S3
- Azure Blob Store
- GCP Cloud Storage
- Snowflake
- Databricks
- jdbc
- odbc

#### Watermarks

Watermarks are used to ensure that data is only processed once. For non-cdc
sources custom logic is used to derive watermarks that are stored in a local
metadata database for each ingestion.

Watermarks will be based on filenames for Filesystem and Blob Storage. It is
assumed that files will be stored in folders with the timestamp to minimise the
amount of data to scan.

For other data sources watermarks will be based on the timestamp field
specified in the configuration file.

For CDC source it is assumed that the when the data is read it only not be
accessible again without intervention in the source system.

When data is written to a Filesystem connector it should partition files into
folders that are timestamps this is specified in the partition field in the
configuration for the connector.

#### Formats

Only supporting CSV, JSON, JSONL, and PARQUET as read formats and write format
is PARQUET.
