// Package types provides the common data types for the valkeyflight system.
package types

import (
	"encoding/json"
	"time"
)

// BatchStatus represents the possible states of a batch in the processing pipeline.
type BatchStatus string

const (
	// BatchStatusPending indicates the batch is registered but not yet being processed.
	BatchStatusPending BatchStatus = "pending"

	// BatchStatusProcessing indicates the batch is currently being processed by a worker.
	BatchStatusProcessing BatchStatus = "processing"

	// BatchStatusCompleted indicates the batch has been successfully processed.
	BatchStatusCompleted BatchStatus = "completed"

	// BatchStatusFailed indicates the batch failed processing but may be retried.
	BatchStatusFailed BatchStatus = "failed"
)

// BatchMetadata contains the metadata for a batch in the system.
type BatchMetadata struct {
	// BatchID is the unique identifier for the batch.
	BatchID string `json:"batch_id"`

	// Status indicates the current status of the batch.
	Status BatchStatus `json:"status"`

	// RetryCount is the number of times this batch has been retried.
	RetryCount int `json:"retry_count"`

	// CreatedAt is when this batch was first registered.
	CreatedAt time.Time `json:"created_at"`

	// LastAttempt is when this batch was last processed or attempted.
	LastAttempt time.Time `json:"last_attempt"`
}

// MarshalBinary converts the BatchMetadata to a binary format (JSON).
// This implements the encoding.BinaryMarshaler interface.
func (b BatchMetadata) MarshalBinary() ([]byte, error) {
	return json.Marshal(b)
}

// UnmarshalBinary parses the JSON-encoded data and stores the result
// in the BatchMetadata. This implements the encoding.BinaryUnmarshaler interface.
func (b *BatchMetadata) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, b)
}
