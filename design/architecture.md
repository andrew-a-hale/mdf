# Architecture

## Parser

Parses YAML configuration files into Go structs.

## Triggerer

Triggers data ingestion jobs based on events in a queue.

## Scheduler

Schedules data ingestion jobs for the executor.

## Executor

Executes data ingestion jobs using the appropriate connectors.

## Validator

Validates data against defined constraints (not null, unique).

## Notifier

Sends notifications on job success or failure.

## Event Logger

Track processed events to avoid re-processing and allow for replaying of
events using Valkey.

## Connectors

Provide connectivity to data sources and destinations.

### Watermarks

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

### Formats

Only supporting CSV, JSON, JSONL, and PARQUET as read formats and write format
is PARQUET.
