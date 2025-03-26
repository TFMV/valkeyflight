// Package coordinator provides a Valkey-backed metadata storage layer for batch processing.
package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/TFMV/valkeyflight/internal/types"
	valkey "github.com/valkey-io/valkey-go"
)

const (
	// keyPrefix is used for all keys in Valkey to avoid collisions with other applications
	keyPrefix = "valkeyflight:"

	// pendingSetKey is the key for the set of pending batch IDs
	pendingSetKey = keyPrefix + "pending_batches"

	// processingSetKey is the key for the set of processing batch IDs
	processingSetKey = keyPrefix + "processing_batches"

	// metadataKeyPrefix is the prefix for batch metadata
	metadataKeyPrefix = keyPrefix + "metadata:"

	// defaultHeartbeatInterval is how often workers should update the last attempt time
	defaultHeartbeatInterval = 5 * time.Second

	// defaultMaxRetries is the default maximum number of retries for a batch
	defaultMaxRetries = 3
)

// Config contains configuration options for the Coordinator
type Config struct {
	// ValKeyAddr is the address of the Valkey server
	ValKeyAddr string

	// ValKeyPassword is the password for the Valkey server (optional)
	ValKeyPassword string

	// HeartbeatInterval is how often workers should update the last attempt time
	HeartbeatInterval time.Duration

	// MaxRetries is the maximum number of retries for a batch before it's considered permanently failed
	MaxRetries int
}

// Coordinator manages batch metadata using Valkey
type Coordinator struct {
	client               valkey.Client
	cfg                  Config
	shutdownCh           chan struct{}
	wg                   sync.WaitGroup
	expireStaleBatchesMu sync.Mutex
}

// New creates a new Coordinator with the given configuration
func New(cfg Config) (*Coordinator, error) {
	if cfg.ValKeyAddr == "" {
		cfg.ValKeyAddr = "localhost:6379"
	}

	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}

	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = defaultMaxRetries
	}

	// Initialize the Valkey client
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{cfg.ValKeyAddr},
		Password:    cfg.ValKeyPassword,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Valkey client: %w", err)
	}

	// Ping the server to ensure connectivity
	pingCmd := client.B().Ping().Build()
	if err := client.Do(context.Background(), pingCmd).Error(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to Valkey: %w", err)
	}

	c := &Coordinator{
		client:     client,
		cfg:        cfg,
		shutdownCh: make(chan struct{}),
	}

	// Start background task to expire stale batches
	c.wg.Add(1)
	go c.expireStaleBatchesLoop()

	return c, nil
}

// metadataKey returns the full key for a batch's metadata
func metadataKey(batchID string) string {
	return metadataKeyPrefix + batchID
}

// RegisterBatch registers a new batch with the given ID
func (c *Coordinator) RegisterBatch(ctx context.Context, batchID string) error {
	// Create metadata for the batch
	metadata := types.BatchMetadata{
		BatchID:     batchID,
		Status:      types.BatchStatusPending,
		RetryCount:  0,
		CreatedAt:   time.Now(),
		LastAttempt: time.Time{}, // Zero time
	}

	// Serialize the metadata to JSON
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal batch metadata: %w", err)
	}

	// Use a MULTI to execute both commands atomically
	multiCmd := c.client.B().Multi().Build()
	setCmd := c.client.B().Set().Key(metadataKey(batchID)).Value(string(metadataBytes)).Ex(0).Build()
	saddCmd := c.client.B().Sadd().Key(pendingSetKey).Member(batchID).Build()
	execCmd := c.client.B().Exec().Build()

	resp := c.client.Do(ctx, multiCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to initiate MULTI for batch %s: %w", batchID, err)
	}

	// Queue the commands
	resp = c.client.Do(ctx, setCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to queue SET for batch %s: %w", batchID, err)
	}

	resp = c.client.Do(ctx, saddCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to queue SADD for batch %s: %w", batchID, err)
	}

	// Execute the transaction
	resp = c.client.Do(ctx, execCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to EXEC transaction for batch %s: %w", batchID, err)
	}

	return nil
}

// MarkProcessing updates a batch status to processing
func (c *Coordinator) MarkProcessing(ctx context.Context, batchID string) error {
	// Get the current metadata
	getCmd := c.client.B().Get().Key(metadataKey(batchID)).Build()
	resp := c.client.Do(ctx, getCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to get metadata for batch %s: %w", batchID, err)
	}

	metadataStr, err := resp.ToString()
	if err != nil {
		return fmt.Errorf("failed to convert metadata to string for batch %s: %w", batchID, err)
	}

	// Unmarshal the metadata
	var metadata types.BatchMetadata
	if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata for batch %s: %w", batchID, err)
	}

	// Update metadata
	metadata.Status = types.BatchStatusProcessing
	metadata.LastAttempt = time.Now()

	// Serialize the updated metadata
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal updated metadata for batch %s: %w", batchID, err)
	}

	// Use a MULTI to update metadata and sets atomically
	multiCmd := c.client.B().Multi().Build()
	setCmd := c.client.B().Set().Key(metadataKey(batchID)).Value(string(metadataBytes)).Ex(0).Build()
	sremCmd := c.client.B().Srem().Key(pendingSetKey).Member(batchID).Build()
	saddCmd := c.client.B().Sadd().Key(processingSetKey).Member(batchID).Build()
	execCmd := c.client.B().Exec().Build()

	resp = c.client.Do(ctx, multiCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to initiate MULTI for batch %s: %w", batchID, err)
	}

	resp = c.client.Do(ctx, setCmd)
	resp = c.client.Do(ctx, sremCmd)
	resp = c.client.Do(ctx, saddCmd)

	resp = c.client.Do(ctx, execCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to EXEC transaction for batch %s: %w", batchID, err)
	}

	return nil
}

// MarkCompleted updates a batch status to completed
func (c *Coordinator) MarkCompleted(ctx context.Context, batchID string) error {
	// Get the current metadata
	getCmd := c.client.B().Get().Key(metadataKey(batchID)).Build()
	resp := c.client.Do(ctx, getCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to get metadata for batch %s: %w", batchID, err)
	}

	metadataStr, err := resp.ToString()
	if err != nil {
		return fmt.Errorf("failed to convert metadata to string for batch %s: %w", batchID, err)
	}

	// Unmarshal the metadata
	var metadata types.BatchMetadata
	if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata for batch %s: %w", batchID, err)
	}

	// Update metadata
	metadata.Status = types.BatchStatusCompleted
	metadata.LastAttempt = time.Now()

	// Serialize the updated metadata
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal updated metadata for batch %s: %w", batchID, err)
	}

	// Use a MULTI to update metadata and sets atomically
	multiCmd := c.client.B().Multi().Build()
	setCmd := c.client.B().Set().Key(metadataKey(batchID)).Value(string(metadataBytes)).Ex(0).Build()
	sremPendingCmd := c.client.B().Srem().Key(pendingSetKey).Member(batchID).Build()
	sremProcessingCmd := c.client.B().Srem().Key(processingSetKey).Member(batchID).Build()
	execCmd := c.client.B().Exec().Build()

	resp = c.client.Do(ctx, multiCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to initiate MULTI for batch %s: %w", batchID, err)
	}

	resp = c.client.Do(ctx, setCmd)
	resp = c.client.Do(ctx, sremPendingCmd)
	resp = c.client.Do(ctx, sremProcessingCmd)

	resp = c.client.Do(ctx, execCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to EXEC transaction for batch %s: %w", batchID, err)
	}

	return nil
}

// IncrementRetry increments the retry count for a batch and updates its status
func (c *Coordinator) IncrementRetry(ctx context.Context, batchID string) error {
	// Get the current metadata
	getCmd := c.client.B().Get().Key(metadataKey(batchID)).Build()
	resp := c.client.Do(ctx, getCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to get metadata for batch %s: %w", batchID, err)
	}

	metadataStr, err := resp.ToString()
	if err != nil {
		return fmt.Errorf("failed to convert metadata to string for batch %s: %w", batchID, err)
	}

	// Unmarshal the metadata
	var metadata types.BatchMetadata
	if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata for batch %s: %w", batchID, err)
	}

	// Increment retry count
	metadata.RetryCount++
	metadata.LastAttempt = time.Now()

	// If we've exceeded max retries, mark as failed, otherwise back to pending
	if metadata.RetryCount > c.cfg.MaxRetries {
		metadata.Status = types.BatchStatusFailed
	} else {
		metadata.Status = types.BatchStatusPending
	}

	// Serialize the updated metadata
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal updated metadata for batch %s: %w", batchID, err)
	}

	// Use a MULTI to update metadata and sets atomically
	multiCmd := c.client.B().Multi().Build()
	setCmd := c.client.B().Set().Key(metadataKey(batchID)).Value(string(metadataBytes)).Ex(0).Build()
	sremProcessingCmd := c.client.B().Srem().Key(processingSetKey).Member(batchID).Build()

	// Add or remove from pending based on status
	var pendingCmd valkey.Completed
	if metadata.Status == types.BatchStatusPending {
		pendingCmd = c.client.B().Sadd().Key(pendingSetKey).Member(batchID).Build()
	} else {
		pendingCmd = c.client.B().Srem().Key(pendingSetKey).Member(batchID).Build()
	}

	execCmd := c.client.B().Exec().Build()

	resp = c.client.Do(ctx, multiCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to initiate MULTI for batch %s: %w", batchID, err)
	}

	resp = c.client.Do(ctx, setCmd)
	resp = c.client.Do(ctx, sremProcessingCmd)
	resp = c.client.Do(ctx, pendingCmd)

	resp = c.client.Do(ctx, execCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to EXEC transaction for batch %s: %w", batchID, err)
	}

	return nil
}

// ListPendingBatches returns a list of pending batch IDs
func (c *Coordinator) ListPendingBatches(ctx context.Context) ([]string, error) {
	// Get all members of the pending set
	smembersCmd := c.client.B().Smembers().Key(pendingSetKey).Build()
	resp := c.client.Do(ctx, smembersCmd)
	if err := resp.Error(); err != nil {
		return nil, fmt.Errorf("failed to list pending batches: %w", err)
	}

	// Get members as string slice
	members, err := resp.AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("failed to convert pending batches to string slice: %w", err)
	}
	return members, nil
}

// GetBatchMetadata retrieves the metadata for a batch
func (c *Coordinator) GetBatchMetadata(ctx context.Context, batchID string) (*types.BatchMetadata, error) {
	// Get the metadata as string
	getCmd := c.client.B().Get().Key(metadataKey(batchID)).Build()
	resp := c.client.Do(ctx, getCmd)
	if err := resp.Error(); err != nil {
		return nil, fmt.Errorf("failed to get metadata for batch %s: %w", batchID, err)
	}

	metadataStr, err := resp.ToString()
	if err != nil {
		return nil, fmt.Errorf("failed to convert metadata to string for batch %s: %w", batchID, err)
	}

	// Unmarshal the metadata
	var metadata types.BatchMetadata
	if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata for batch %s: %w", batchID, err)
	}

	return &metadata, nil
}

// expireStaleBatchesLoop runs in the background and periodically expires stale batches
func (c *Coordinator) expireStaleBatchesLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.cfg.HeartbeatInterval * 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.expireStaleBatches(context.Background())
		case <-c.shutdownCh:
			return
		}
	}
}

// expireStaleBatches checks for batches that have been processing for too long and marks them for retry
func (c *Coordinator) expireStaleBatches(ctx context.Context) {
	c.expireStaleBatchesMu.Lock()
	defer c.expireStaleBatchesMu.Unlock()

	// Get all batches in the processing set
	smembersCmd := c.client.B().Smembers().Key(processingSetKey).Build()
	resp := c.client.Do(ctx, smembersCmd)
	if err := resp.Error(); err != nil {
		fmt.Printf("Failed to list processing batches: %v\n", err)
		return
	}

	processingBatches, err := resp.AsStrSlice()
	if err != nil {
		fmt.Printf("Failed to convert processing batches to string slice: %v\n", err)
		return
	}
	staleThreshold := time.Now().Add(-c.cfg.HeartbeatInterval * 3)

	for _, batchID := range processingBatches {
		metadata, err := c.GetBatchMetadata(ctx, batchID)
		if err != nil {
			fmt.Printf("Failed to get metadata for batch %s: %v\n", batchID, err)
			continue
		}

		// Check if the batch is stale
		if metadata.LastAttempt.Before(staleThreshold) {
			// Worker likely died, so increment retry count
			if err := c.IncrementRetry(ctx, batchID); err != nil {
				fmt.Printf("Failed to increment retry for stale batch %s: %v\n", batchID, err)
			}
		}
	}
}

// ExpireOldBatches removes batches older than the specified TTL
func (c *Coordinator) ExpireOldBatches(ctx context.Context, ttl time.Duration) error {
	// Get all keys with metadata prefix
	keysCmd := c.client.B().Keys().Pattern(metadataKeyPrefix + "*").Build()
	resp := c.client.Do(ctx, keysCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to list batch metadata keys: %w", err)
	}

	keys, err := resp.AsStrSlice()
	if err != nil {
		return fmt.Errorf("failed to convert keys to string slice: %w", err)
	}
	expireThreshold := time.Now().Add(-ttl)

	for _, key := range keys {
		batchID := key[len(metadataKeyPrefix):]
		metadata, err := c.GetBatchMetadata(ctx, batchID)
		if err != nil {
			fmt.Printf("Failed to get metadata for key %s: %v\n", key, err)
			continue
		}

		// Check if the batch is older than the TTL
		if metadata.CreatedAt.Before(expireThreshold) {
			// Use a MULTI to remove the batch
			multiCmd := c.client.B().Multi().Build()
			sremPendingCmd := c.client.B().Srem().Key(pendingSetKey).Member(batchID).Build()
			sremProcessingCmd := c.client.B().Srem().Key(processingSetKey).Member(batchID).Build()
			delCmd := c.client.B().Del().Key(key).Build()
			execCmd := c.client.B().Exec().Build()

			resp = c.client.Do(ctx, multiCmd)
			if err := resp.Error(); err != nil {
				fmt.Printf("Failed to initiate MULTI for expired batch %s: %v\n", batchID, err)
				continue
			}

			c.client.Do(ctx, sremPendingCmd)
			c.client.Do(ctx, sremProcessingCmd)
			c.client.Do(ctx, delCmd)

			resp = c.client.Do(ctx, execCmd)
			if err := resp.Error(); err != nil {
				fmt.Printf("Failed to EXEC transaction for expired batch %s: %v\n", batchID, err)
			}
		}
	}

	return nil
}

// HeartbeatBatch updates the LastAttempt time for a batch that's still being processed
func (c *Coordinator) HeartbeatBatch(ctx context.Context, batchID string) error {
	// Get the current metadata
	metadata, err := c.GetBatchMetadata(ctx, batchID)
	if err != nil {
		return fmt.Errorf("failed to get metadata for batch %s: %w", batchID, err)
	}

	// Only update if the batch is still processing
	if metadata.Status != types.BatchStatusProcessing {
		return nil
	}

	// Update the last attempt time
	metadata.LastAttempt = time.Now()

	// Serialize the updated metadata
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal updated metadata for batch %s: %w", batchID, err)
	}

	// Set the updated metadata
	setCmd := c.client.B().Set().Key(metadataKey(batchID)).Value(string(metadataBytes)).Ex(0).Build()
	resp := c.client.Do(ctx, setCmd)
	if err := resp.Error(); err != nil {
		return fmt.Errorf("failed to update heartbeat for batch %s: %w", batchID, err)
	}

	return nil
}

// Shutdown gracefully shuts down the coordinator
func (c *Coordinator) Shutdown(ctx context.Context) error {
	// Signal background goroutines to exit
	close(c.shutdownCh)

	// Wait for goroutines to finish
	waitCh := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(waitCh)
	}()

	// Wait for either context done or goroutines finished
	select {
	case <-ctx.Done():
		return fmt.Errorf("shutdown timed out: %w", ctx.Err())
	case <-waitCh:
		// All goroutines finished
	}

	// Close the client
	c.client.Close()
	return nil
}
