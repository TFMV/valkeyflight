# ValkeyFlight Example: Transaction Processing Pipeline

This example demonstrates a complete data processing pipeline using ValkeyFlight for efficient, zero-copy data movement and lightweight coordination.

## Overview

This example implements a transaction processing pipeline that:

1. Generates synthetic transaction data
2. Processes data in parallel across workers using Arrow record batches
3. Performs fraud detection and data enrichment
4. Provides aggregate statistics on processing results

## Components

The pipeline consists of:

- **Data Generator**: Creates synthetic financial transaction data
- **Flight Server**: Handles efficient Arrow data movement
- **Valkey Coordinator**: Manages batch processing metadata
- **Worker Pool**: Processes batches with fraud detection in parallel
- **Transaction Processor**: Implements business logic for fraud detection

## Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│             │      │             │      │             │
│    Data     │─────▶│   Flight    │◀────▶│   Worker    │
│  Generator  │      │   Server    │      │    Pool     │
│             │      │             │      │             │
└─────────────┘      └─────────────┘      └─────────────┘
                            │                    │
                            │                    │
                            ▼                    ▼
                     ┌─────────────┐      ┌─────────────┐
                     │             │      │             │
                     │   Valkey    │◀────▶│ Transaction │
                     │ Coordinator │      │  Processor  │
                     │             │      │             │
                     └─────────────┘      └─────────────┘
```

## Files in This Example

- `data_generator.go`: Generates synthetic transaction data
- `transaction_processor.go`: Implements fraud detection and data enrichment
- `main.go`: Orchestrates the pipeline components
- `config.go`: Configuration structures and parsing
- `go.mod`: Module dependencies
- `config.yaml`: Configuration file
- `run.sh`: Convenience script to run the example

## Requirements

- Go 1.18+
- Running Valkey server
- Apache Arrow Flight dependencies

## Running the Example

1. Start a Valkey server:

   ```bash
   docker run -p 6379:6379 valkey/valkey:latest
   ```

2. Run the example using the provided script:

   ```bash
   ./run.sh
   ```

   The script will:
   - Check if Valkey is running (starts it via Docker if needed)
   - Create necessary directories
   - Build the transaction pipeline
   - Run the pipeline with the default configuration

## Configuration

The `config.yaml` file contains all configuration options for the pipeline:

- Flight server settings
- Valkey coordinator settings
- Worker pool configuration
- Data generation parameters
- Processing settings (fraud detection thresholds)
- Output options

## Expected Output

The example will:

1. Generate 1,000,000 synthetic transactions
2. Process them in batches of 10,000 records
3. Apply transaction enrichment and fraud detection
4. Output aggregated statistics

Example output (your numbers may vary):

```
Flight server started on localhost:8080
Flight client connected to localhost:8080
Coordinator connected to Valkey at localhost:6379
Generating 1000000 transactions in batches of 10000...
Generated 1000000 synthetic transactions to data/transactions.arrow
Loading 1000000 records in batches of 10000...
Loaded 100/100 batches
Successfully loaded 100 batches
Starting worker pool with 4 workers
Worker 0 started
Worker 1 started
Worker 2 started
Worker 3 started
All workers shut down successfully

=== Transaction Processing Statistics ===
Total Transactions: 1000000
Fraudulent Transactions: 1243 (0.12%)
Processing Duration: 12.4s
Throughput: 80450.00 records/second
Batch Count: 100
```
