# Getting Started with ValkeyFlight

This guide will help you set up and start using ValkeyFlight for high-performance data processing.

## Prerequisites

Before you begin, ensure you have the following installed:

- [Go 1.24+](https://golang.org/dl/)
- [Valkey](https://github.com/valkey-io/valkey) (or Redis)
- Git

## Installation

1. Clone the repository:

```bash
git clone https://github.com/TFMV/valkeyflight.git
cd valkeyflight
```

2. Build the application:

```bash
go build -o valkeyflight ./cmd/valkeyflight
```

## Running

### Start Valkey

First, make sure Valkey is running. If you don't have it set up yet, you can use Docker:

```bash
docker run -d --name valkey -p 6379:6379 valkey-io/valkey:latest
```

### Run ValkeyFlight

#### All Components

To run all components (Flight server, coordinator, and workers) in a single process:

```bash
./valkeyflight --mode=all --valkey-addr=localhost:6379 --flight-addr=localhost:8080
```

#### Distributed Mode

For production deployments, you might want to run components separately:

1. Run the Flight server:

```bash
./valkeyflight --mode=flight --flight-addr=localhost:8080
```

2. Run the coordinator:

```bash
./valkeyflight --mode=coordinator --valkey-addr=localhost:6379
```

3. Run workers:

```bash
./valkeyflight --mode=worker --valkey-addr=localhost:6379 --flight-addr=localhost:8080 --workers=4
```

## Using ValkeyFlight in Your Application

### Importing the Client Libraries

```go
import (
    "github.com/TFMV/valkeyflight/flight_client"
    "github.com/TFMV/valkeyflight/coordinator"
    "github.com/apache/arrow-go/v18/arrow/memory"
)
```

### Creating a Flight Client

```go
// Initialize a memory allocator
allocator := memory.NewGoAllocator()

// Create a client configuration
clientConfig := flight_client.ClientConfig{
    Addr:      "localhost:8080",
    Allocator: allocator,
}

// Create the client
client, err := flight_client.NewFlightClient(clientConfig)
if err != nil {
    log.Fatalf("Failed to create Flight client: %v", err)
}
defer client.Close()
```

### Creating a Coordinator Client

```go
// Create coordinator configuration
coordConfig := coordinator.Config{
    ValKeyAddr:     "localhost:6379",
    ValKeyPassword: "",
    MaxRetries:     3,
}

// Create the coordinator
coord, err := coordinator.New(coordConfig)
if err != nil {
    log.Fatalf("Failed to create coordinator: %v", err)
}
defer coord.Shutdown(context.Background())
```

## Example: Processing Data with ValkeyFlight

Here's a complete example showing how to store and process data:

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/TFMV/valkeyflight/flight_client"
    "github.com/TFMV/valkeyflight/coordinator"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
    // Create context
    ctx := context.Background()

    // Initialize a memory allocator
    allocator := memory.NewGoAllocator()

    // Create Flight client
    flightClient, err := flight_client.NewFlightClient(flight_client.ClientConfig{
        Addr:      "localhost:8080",
        Allocator: allocator,
    })
    if err != nil {
        log.Fatalf("Failed to create Flight client: %v", err)
    }
    defer flightClient.Close()

    // Create coordinator
    coord, err := coordinator.New(coordinator.Config{
        ValKeyAddr: "localhost:6379",
    })
    if err != nil {
        log.Fatalf("Failed to create coordinator: %v", err)
    }
    defer coord.Shutdown(ctx)

    // Create an Arrow record batch
    recordBatch := createSampleRecordBatch(allocator)
    defer recordBatch.Release()

    // Store the batch in Flight server
    batchID, err := flightClient.StoreBatch(ctx, recordBatch)
    if err != nil {
        log.Fatalf("Failed to store batch: %v", err)
    }
    log.Printf("Stored batch with ID: %s", batchID)

    // Register the batch with the coordinator
    err = coord.RegisterBatch(ctx, batchID)
    if err != nil {
        log.Fatalf("Failed to register batch: %v", err)
    }
    log.Printf("Registered batch %s for processing", batchID)

    // In a real application, workers would pick up the batch
    // For demonstration, we'll simulate checking the status
    for i := 0; i < 10; i++ {
        time.Sleep(1 * time.Second)
        
        metadata, err := coord.GetBatchMetadata(ctx, batchID)
        if err != nil {
            log.Printf("Error getting batch metadata: %v", err)
            continue
        }
        
        log.Printf("Batch %s status: %s, retry count: %d", 
            batchID, metadata.Status, metadata.RetryCount)
            
        if metadata.Status == "completed" || metadata.Status == "failed" {
            break
        }
    }
}

// createSampleRecordBatch creates a simple Arrow record batch for testing
func createSampleRecordBatch(allocator *memory.GoAllocator) arrow.Record {
    // Create a simple schema
    schema := arrow.NewSchema(
        []arrow.Field{
            {Name: "id", Type: arrow.PrimitiveTypes.Int64},
            {Name: "name", Type: arrow.BinaryTypes.String},
        },
        nil,
    )

    // Create builders for each column
    idBuilder := array.NewInt64Builder(allocator)
    defer idBuilder.Release()
    nameBuilder := array.NewStringBuilder(allocator)
    defer nameBuilder.Release()

    // Add data
    idBuilder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
    nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie", "Dave", "Eve"}, nil)

    // Build the columns
    idCol := idBuilder.NewArray()
    defer idCol.Release()
    nameCol := nameBuilder.NewArray()
    defer nameCol.Release()

    // Create the record batch
    return array.NewRecord(schema, []arrow.Array{idCol, nameCol}, 5)
}
```

## Processing Logic

When working with ValkeyFlight, you typically implement these patterns:

### Producer Pattern

```go
// Generate or receive data
recordBatch := generateData()

// Store in Flight server
batchID, err := flightClient.StoreBatch(ctx, recordBatch)
if err != nil {
    return err
}

// Register for processing
err = coordinator.RegisterBatch(ctx, batchID)
if err != nil {
    return err
}

// Return or log the batch ID for tracking
return batchID
```

### Worker/Consumer Pattern

This is typically handled by the built-in worker pool, but you can also implement custom worker logic:

```go
// Get pending batches
pendingBatches, err := coordinator.ListPendingBatches(ctx)
if err != nil {
    return err
}

// Process each batch
for _, batchID := range pendingBatches {
    // Mark as processing
    err = coordinator.MarkProcessing(ctx, batchID)
    if err != nil {
        continue
    }
    
    // Get the batch data
    batch, err := flightClient.GetBatch(ctx, batchID)
    if err != nil {
        coordinator.IncrementRetry(ctx, batchID)
        continue
    }
    
    // Process the batch
    err = processData(batch)
    
    // Update status based on result
    if err != nil {
        coordinator.IncrementRetry(ctx, batchID)
    } else {
        coordinator.MarkCompleted(ctx, batchID)
    }
}
```

## Monitoring

To monitor batches and their status, you can query the coordinator:

```go
// Get batch metadata
metadata, err := coordinator.GetBatchMetadata(ctx, batchID)
if err != nil {
    log.Printf("Error getting batch metadata: %v", err)
    return
}

log.Printf("Batch: %s", batchID)
log.Printf("Status: %s", metadata.Status)
log.Printf("Created: %s", metadata.CreatedAt)
log.Printf("Last Attempt: %s", metadata.LastAttempt)
log.Printf("Retry Count: %d", metadata.RetryCount)
```

## Advanced Configuration

For more advanced scenarios, you can configure:

### Custom TTL for batches

```go
// The Flight server configuration supports TTL
flightServerConfig := flight_server.ServerConfig{
    Addr:      "localhost:8080",
    Allocator: allocator,
    TTL:       time.Hour * 24, // Keep batches for 24 hours
}
```

### Worker Concurrency

```go
// Configure worker concurrency
workerConfig := worker.Config{
    Coordinator:  coord,
    FlightClient: flightClient,
    WorkerCount:  8,             // 8 concurrent workers
    PollInterval: time.Second * 2, // Poll every 2 seconds
}
```

### Retry Configuration

```go
// Configure retry behavior
coordConfig := coordinator.Config{
    ValKeyAddr:     "localhost:6379",
    MaxRetries:     5,              // Maximum 5 retries
    HeartbeatInterval: time.Second * 10, // Heartbeat every 10 seconds
}
```
