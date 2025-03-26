// Package worker provides a worker service that processes Arrow record batches.
package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/TFMV/valkeyflight/coordinator"
	"github.com/TFMV/valkeyflight/internal/flight"
	"github.com/apache/arrow-go/v18/arrow"
)

// BatchProcessor is an interface for processing Arrow record batches.
type BatchProcessor interface {
	// ProcessBatch processes a record batch and returns any error.
	ProcessBatch(ctx context.Context, batchID string, batch arrow.Record) error
}

// DefaultBatchProcessor is a no-op batch processor that can be used as a placeholder.
type DefaultBatchProcessor struct{}

// ProcessBatch is a no-op implementation that simply acknowledges the batch.
func (p *DefaultBatchProcessor) ProcessBatch(ctx context.Context, batchID string, batch arrow.Record) error {
	fmt.Printf("Processed batch %s with %d rows and schema %s\n", batchID, batch.NumRows(), batch.Schema())
	return nil
}

// Config contains configuration options for the Worker.
type Config struct {
	// Coordinator is the metadata coordinator.
	Coordinator *coordinator.Coordinator

	// FlightClient is the client used to retrieve batches.
	FlightClient *flight.FlightClient

	// BatchProcessor is used to process batches.
	BatchProcessor BatchProcessor

	// WorkerCount is the number of worker goroutines to spawn.
	WorkerCount int

	// PollInterval is how often to poll for pending batches.
	PollInterval time.Duration

	// HeartbeatInterval is how often to send heartbeats for in-progress batches.
	HeartbeatInterval time.Duration

	// BackoffMin is the minimum backoff duration for retrying failed batches.
	BackoffMin time.Duration

	// BackoffMax is the maximum backoff duration for retrying failed batches.
	BackoffMax time.Duration

	// BackoffFactor is the multiplicative factor for retry backoff.
	BackoffFactor float64

	// BackoffRandomization is a randomization factor for jittering retry backoff.
	BackoffRandomization float64
}

// Worker manages a pool of workers that process batches.
type Worker struct {
	cfg          Config
	workers      int
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
	wg           sync.WaitGroup
}

// New creates a new Worker with the given configuration.
func New(cfg Config) (*Worker, error) {
	if cfg.Coordinator == nil {
		return nil, errors.New("coordinator is required")
	}

	if cfg.FlightClient == nil {
		return nil, errors.New("flight client is required")
	}

	if cfg.BatchProcessor == nil {
		cfg.BatchProcessor = &DefaultBatchProcessor{}
	}

	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 1
	}

	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Second
	}

	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}

	if cfg.BackoffMin == 0 {
		cfg.BackoffMin = 1 * time.Second
	}

	if cfg.BackoffMax == 0 {
		cfg.BackoffMax = 5 * time.Minute
	}

	if cfg.BackoffFactor == 0 {
		cfg.BackoffFactor = 2.0
	}

	if cfg.BackoffRandomization == 0 {
		cfg.BackoffRandomization = 0.2
	}

	return &Worker{
		cfg:        cfg,
		workers:    cfg.WorkerCount,
		shutdownCh: make(chan struct{}),
	}, nil
}

// Start starts the worker pool.
func (w *Worker) Start() {
	fmt.Printf("Starting worker pool with %d workers\n", w.workers)

	// Start the worker goroutines
	w.wg.Add(w.workers)
	for i := 0; i < w.workers; i++ {
		go w.workerLoop(i)
	}
}

// Shutdown gracefully shuts down the worker pool.
func (w *Worker) Shutdown(ctx context.Context) error {
	var shutdownErr error

	w.shutdownOnce.Do(func() {
		fmt.Println("Shutting down worker pool")

		// Signal all workers to stop
		close(w.shutdownCh)

		// Create a channel to signal when wait is done
		done := make(chan struct{})
		go func() {
			w.wg.Wait()
			close(done)
		}()

		// Wait for workers to finish or context to expire
		select {
		case <-done:
			fmt.Println("All workers shut down successfully")
		case <-ctx.Done():
			shutdownErr = ctx.Err()
			fmt.Printf("Shutdown timed out: %v\n", shutdownErr)
		}
	})

	return shutdownErr
}

// workerLoop is the main loop for a worker goroutine.
func (w *Worker) workerLoop(workerID int) {
	defer w.wg.Done()

	fmt.Printf("Worker %d started\n", workerID)

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Poll for pending batches
			if err := w.processPendingBatch(context.Background()); err != nil {
				if !errors.Is(err, ErrNoBatchAvailable) {
					fmt.Printf("Worker %d error: %v\n", workerID, err)
				}
			}
		case <-w.shutdownCh:
			fmt.Printf("Worker %d shutting down\n", workerID)
			return
		}
	}
}

// ErrNoBatchAvailable is returned when no batches are available for processing.
var ErrNoBatchAvailable = errors.New("no batch available")

// processPendingBatch processes a single pending batch from the coordinator.
func (w *Worker) processPendingBatch(ctx context.Context) error {
	// List pending batches
	pendingBatches, err := w.cfg.Coordinator.ListPendingBatches(ctx)
	if err != nil {
		return fmt.Errorf("failed to list pending batches: %w", err)
	}

	// If no pending batches, return early
	if len(pendingBatches) == 0 {
		return ErrNoBatchAvailable
	}

	// Pick a random batch from the pending list
	// This avoids multiple workers all grabbing the same batch
	batchID := pendingBatches[rand.Intn(len(pendingBatches))]

	// Get the batch metadata
	metadata, err := w.cfg.Coordinator.GetBatchMetadata(ctx, batchID)
	if err != nil {
		return fmt.Errorf("failed to get metadata for batch %s: %w", batchID, err)
	}

	// Check if we should apply backoff based on retry count
	if metadata.RetryCount > 0 {
		backoffDuration := w.calculateBackoff(metadata.RetryCount)
		sinceLastAttempt := time.Since(metadata.LastAttempt)

		if sinceLastAttempt < backoffDuration {
			// Not time to retry yet
			return ErrNoBatchAvailable
		}
	}

	// Mark the batch as processing
	if err := w.cfg.Coordinator.MarkProcessing(ctx, batchID); err != nil {
		return fmt.Errorf("failed to mark batch %s as processing: %w", batchID, err)
	}

	// Start heartbeat goroutine
	heartbeatCtx, cancelHeartbeat := context.WithCancel(context.Background())
	var heartbeatWg sync.WaitGroup
	heartbeatWg.Add(1)

	go func() {
		defer heartbeatWg.Done()
		w.heartbeatLoop(heartbeatCtx, batchID)
	}()

	// Ensure the heartbeat is stopped when we're done
	defer func() {
		cancelHeartbeat()
		heartbeatWg.Wait()
	}()

	// Get the batch from Flight
	batch, err := w.cfg.FlightClient.GetBatch(ctx, batchID)
	if err != nil {
		if err := w.cfg.Coordinator.IncrementRetry(ctx, batchID); err != nil {
			fmt.Printf("Failed to increment retry for batch %s: %v\n", batchID, err)
		}
		return fmt.Errorf("failed to get batch %s from Flight: %w", batchID, err)
	}
	defer batch.Release()

	// Process the batch
	err = w.cfg.BatchProcessor.ProcessBatch(ctx, batchID, batch)
	if err != nil {
		// If processing failed, increment retry count
		if incrementErr := w.cfg.Coordinator.IncrementRetry(ctx, batchID); incrementErr != nil {
			fmt.Printf("Failed to increment retry for batch %s: %v\n", batchID, incrementErr)
		}
		return fmt.Errorf("failed to process batch %s: %w", batchID, err)
	}

	// If processing succeeded, mark as completed
	if err := w.cfg.Coordinator.MarkCompleted(ctx, batchID); err != nil {
		return fmt.Errorf("failed to mark batch %s as completed: %w", batchID, err)
	}

	fmt.Printf("Successfully processed batch %s\n", batchID)
	return nil
}

// heartbeatLoop periodically sends heartbeats to the coordinator for a batch that's being processed.
func (w *Worker) heartbeatLoop(ctx context.Context, batchID string) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := w.cfg.Coordinator.HeartbeatBatch(ctx, batchID); err != nil {
				fmt.Printf("Failed to send heartbeat for batch %s: %v\n", batchID, err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// calculateBackoff calculates the backoff duration based on retry count and jitter.
func (w *Worker) calculateBackoff(retryCount int) time.Duration {
	// Calculate backoff with exponential increase
	backoff := float64(w.cfg.BackoffMin) * math.Pow(w.cfg.BackoffFactor, float64(retryCount-1))

	// Apply max cap
	if backoff > float64(w.cfg.BackoffMax) {
		backoff = float64(w.cfg.BackoffMax)
	}

	// Apply jitter
	jitter := 1.0 + (rand.Float64()*2.0-1.0)*w.cfg.BackoffRandomization
	backoff = backoff * jitter

	return time.Duration(backoff)
}
