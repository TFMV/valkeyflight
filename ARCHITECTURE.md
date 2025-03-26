# ValkeyFlight System Architecture

ValkeyFlight is designed as a high-performance, zero-copy data pipeline system that leverages Apache Arrow Flight for data transport and Valkey (a Redis fork) for lightweight coordination.

## System Overview

ValkeyFlight addresses the need for high-throughput data processing without the overhead of a full workflow engine like Temporal. It focuses on efficient batch processing with reliable tracking and retry mechanics.

## Core Components

### 1. Apache Arrow Flight Server

The Flight Server provides a high-performance transport layer for moving Arrow record batches with zero-copy semantics. Unlike traditional data transport mechanisms, Flight allows data to be moved without serialization/deserialization overhead.

**Key responsibilities:**

- Store Arrow record batches with associated batch IDs
- Retrieve batches by ID
- Maintain in-memory batch storage with TTL-based cleanup
- Implement Flight protocol (DoGet, DoPut, GetFlightInfo)

**Implementation details:**

- Uses thread-safe in-memory storage with mutex protection
- Provides cleanup goroutine to remove expired batches
- Implements Arrow Flight gRPC service interfaces

### 2. Valkey Coordinator

The coordinator provides the control plane, tracking batch metadata and status using Valkey as the backing store. It maintains batch lifecycle state and coordinates workers.

**Key responsibilities:**

- Register new batches with initial metadata
- Track batch status (pending, processing, completed, failed)
- Manage retry counts and last attempt timestamps
- Detect and handle stale batches (when workers die)

**Implementation details:**

- Uses Valkey sets to track pending and processing batches
- Stores JSON-serialized metadata for each batch
- Uses atomic transactions for consistent state updates
- Implements background cleanup of stale batches

### 3. Worker Pool

The worker pool polls for pending batches, processes them, and manages retry logic with exponential backoff.

**Key responsibilities:**

- Poll the coordinator for pending batches
- Fetch batch data from the Flight server
- Process batches (apply business logic)
- Update batch status based on processing outcome
- Implement retry logic with exponential backoff

**Implementation details:**

- Maintains a pool of worker goroutines
- Uses a semaphore to limit concurrent processing
- Implements heartbeats during long-running operations
- Handles graceful shutdown with context cancellation

## Data Flow

1. **Batch Registration:**
   - Client stores an Arrow record batch in the Flight server
   - Client receives a batch ID
   - Client registers the batch with the coordinator

2. **Batch Processing:**
   - Workers poll the coordinator for pending batches
   - When a batch is available, a worker marks it as processing
   - The worker retrieves the batch data from the Flight server
   - The worker processes the batch
   - The worker updates the batch status (completed or retry)

3. **Error Handling:**
   - If processing fails, the retry count is incremented
   - The batch is moved back to pending status (or failed if max retries reached)
   - Workers implement exponential backoff based on retry count

4. **Stale Batch Handling:**
   - The coordinator periodically checks for stale batches (processing for too long)
   - Stale batches are moved back to pending status with retry incremented

## Technical Design Decisions

### Zero-Copy Data Movement

ValkeyFlight uses Arrow Flight for zero-copy data movement. This means:

- Data is passed between processes without serialization/deserialization
- Memory layout is preserved, enabling CPU-efficient operations
- Network transfer is optimized for Arrow's columnar format

### Lightweight Coordination

Instead of a full workflow engine, ValkeyFlight uses Valkey for lightweight coordination:

- Metadata operations are simple key-value and set operations
- No complex persistence schema required
- Low latency for control operations

### Fault Tolerance

ValkeyFlight ensures fault tolerance through:

- Heartbeats to detect worker failures
- Automatic retry mechanism with exponential backoff
- Stale batch detection and recovery
- TTL-based cleanup to prevent resource leaks

### Scalability

The system is designed to scale horizontally:

- Stateless Flight servers can be deployed behind load balancers
- Worker pools can be distributed across multiple machines
- Valkey can be configured as a cluster for high availability

## Comparison with Temporal

ValkeyFlight provides a lightweight alternative to Temporal with these tradeoffs:

| Aspect | ValkeyFlight | Temporal |
|--------|--------------|----------|
| Focus | High-performance data movement | Complex workflow orchestration |
| Data Transport | Arrow Flight (zero-copy) | gRPC with serialization |
| State Management | Simple metadata in Valkey | Full workflow history |
| Features | Basic retry, metadata tracking | Versioning, query, signals, timers |
| Scalability | Horizontally scalable | Horizontally scalable |
| Resource Usage | Lower | Higher |
| Use Case | High-volume data processing | Complex business processes |

## Configuration and Tuning

ValkeyFlight can be tuned for different workloads:

- **Memory Management**: Configure the Arrow memory allocator to control memory usage
- **Worker Count**: Adjust based on CPU cores and workload characteristics
- **Polling Interval**: Balance responsiveness against coordination overhead
- **Retry Settings**: Configure max retries and backoff policy for different error types
- **TTL Values**: Set appropriate expiration times based on data volume and processing time

## Development and Extension

ValkeyFlight is designed to be extensible:

- Custom processing logic can be implemented in worker handlers
- Additional metadata fields can be added to the batch metadata
- The system can be integrated with monitoring tools via metrics and logging
- Authentication and authorization can be added to Flight and Valkey connections
