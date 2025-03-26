// Package flight_server provides a Flight server for handling Arrow record batches.
package flight_server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// FlightServer implements a simple Arrow Flight server for sharing Arrow RecordBatches
// between services with minimal serialization.
type FlightServer struct {
	flight.BaseFlightServer
	server      *grpc.Server
	listener    net.Listener
	addr        string
	batches     map[string]arrow.Record
	batchesMu   sync.RWMutex
	allocator   memory.Allocator
	expirations map[string]time.Time
	ttl         time.Duration
	cancel      context.CancelFunc // Cancel function for cleanup goroutine
}

// ServerConfig contains configuration options for the Flight server
type ServerConfig struct {
	// Address to listen on (e.g., "localhost:8080")
	Addr string
	// Memory allocator to use
	Allocator memory.Allocator
	// TTL for stored batches (default: 1 hour)
	TTL time.Duration
}

// NewFlightServer creates a new Arrow Flight server
func NewFlightServer(config ServerConfig) (*FlightServer, error) {
	if config.Addr == "" {
		config.Addr = "localhost:8080"
	}
	if config.Allocator == nil {
		config.Allocator = memory.NewGoAllocator()
	}
	if config.TTL == 0 {
		config.TTL = 1 * time.Hour
	}

	// Create the server without starting the listener yet
	server := &FlightServer{
		addr:        config.Addr,
		batches:     make(map[string]arrow.Record),
		expirations: make(map[string]time.Time),
		allocator:   config.Allocator,
		ttl:         config.TTL,
	}

	// Create a gRPC server with appropriate options
	server.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(64*1024*1024), // 64MB max message size
		grpc.MaxSendMsgSize(64*1024*1024), // 64MB max message size
	)

	// Register the Flight service
	flight.RegisterFlightServiceServer(server.server, server)

	// Start a goroutine to clean up expired batches
	ctx, cancel := context.WithCancel(context.Background())
	server.cancel = cancel
	go server.cleanupExpiredBatches(ctx)

	return server, nil
}

// Start starts the Flight server
func (s *FlightServer) Start() error {
	fmt.Printf("Starting Arrow Flight server on %s\n", s.addr)

	// Create a listener
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	// Store the listener
	s.listener = listener

	// Serve in the current goroutine
	return s.server.Serve(listener)
}

// Stop stops the Flight server
func (s *FlightServer) Stop() {
	fmt.Println("Stopping Arrow Flight server")

	// Cancel the cleanup goroutine
	if s.cancel != nil {
		s.cancel()
	}

	// Clear all batches to release memory
	s.batchesMu.Lock()
	for id, batch := range s.batches {
		batch.Release()
		delete(s.batches, id)
		delete(s.expirations, id)
	}
	s.batchesMu.Unlock()

	// Stop the gRPC server gracefully
	if s.server != nil {
		s.server.GracefulStop()
	}

	// Close the listener if it exists
	if s.listener != nil {
		s.listener.Close()
	}

	fmt.Println("Arrow Flight server stopped")
}

// GetFlightInfo implements the Flight GetFlightInfo method
func (s *FlightServer) GetFlightInfo(ctx context.Context, request *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	cmd := string(request.Cmd)

	s.batchesMu.RLock()
	batch, ok := s.batches[cmd]
	s.batchesMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("batch with ID %s not found", cmd)
	}

	endpoint := &flight.FlightEndpoint{
		Ticket: &flight.Ticket{Ticket: []byte(cmd)},
		Location: []*flight.Location{
			{Uri: fmt.Sprintf("grpc://%s", s.addr)},
		},
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(batch.Schema(), s.allocator),
		FlightDescriptor: request,
		Endpoint:         []*flight.FlightEndpoint{endpoint},
		TotalRecords:     batch.NumRows(),
		TotalBytes:       -1, // Unknown size
	}, nil
}

// DoGet implements the Flight DoGet method
func (s *FlightServer) DoGet(request *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	batchID := string(request.Ticket)

	s.batchesMu.RLock()
	batch, ok := s.batches[batchID]
	s.batchesMu.RUnlock()

	if !ok {
		return fmt.Errorf("batch with ID %s not found", batchID)
	}

	// Create a writer for the stream
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(batch.Schema()))

	// Write the batch to the stream and handle errors
	if err := writer.Write(batch); err != nil {
		// Make sure to close the writer even if writing fails
		writer.Close()
		return fmt.Errorf("failed to write batch to stream: %w", err)
	}

	// Close the writer to signal the end of the stream
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	return nil
}

// DoPut implements the Flight DoPut method
func (s *FlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	// Create a reader for the stream
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	// Read the first record
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return fmt.Errorf("error reading record: %w", err)
		}
		return fmt.Errorf("no record received")
	}

	// Get the record and retain it
	batch := reader.Record()
	batch.Retain() // Retain the batch so it's not released when the reader is released
	defer func() {
		// If we exit with an error, make sure to release the batch
		if batch != nil {
			batch.Release()
		}
	}()

	// Generate a unique ID for the batch
	batchID := generateBatchID()

	// Store the batch
	s.batchesMu.Lock()
	s.batches[batchID] = batch
	s.expirations[batchID] = time.Now().Add(s.ttl)
	s.batchesMu.Unlock()

	// We've successfully stored the batch, so don't release it on exit
	batch = nil

	// Send the batch ID back to the client
	err = stream.Send(&flight.PutResult{
		AppMetadata: []byte(batchID),
	})
	if err != nil {
		// If we fail to send the result, remove the batch from storage
		s.batchesMu.Lock()
		if storedBatch, ok := s.batches[batchID]; ok {
			storedBatch.Release()
			delete(s.batches, batchID)
			delete(s.expirations, batchID)
		}
		s.batchesMu.Unlock()
		return fmt.Errorf("failed to send result: %w", err)
	}

	return nil
}

// cleanupExpiredBatches periodically removes expired batches
func (s *FlightServer) cleanupExpiredBatches(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			s.batchesMu.Lock()
			for id, expiry := range s.expirations {
				if now.After(expiry) {
					if batch, ok := s.batches[id]; ok {
						batch.Release()
						delete(s.batches, id)
					}
					delete(s.expirations, id)
					fmt.Printf("Removed expired batch %s\n", id)
				}
			}
			s.batchesMu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// UpdateBatchTTL updates the expiration time for a batch
func (s *FlightServer) UpdateBatchTTL(batchID string, ttl time.Duration) bool {
	s.batchesMu.Lock()
	defer s.batchesMu.Unlock()

	if _, ok := s.batches[batchID]; ok {
		s.expirations[batchID] = time.Now().Add(ttl)
		return true
	}
	return false
}

// HasBatch checks if a batch exists in the server
func (s *FlightServer) HasBatch(batchID string) bool {
	s.batchesMu.RLock()
	defer s.batchesMu.RUnlock()
	_, ok := s.batches[batchID]
	return ok
}

// DeleteBatch deletes a batch from the server
func (s *FlightServer) DeleteBatch(batchID string) bool {
	s.batchesMu.Lock()
	defer s.batchesMu.Unlock()

	if batch, ok := s.batches[batchID]; ok {
		batch.Release()
		delete(s.batches, batchID)
		delete(s.expirations, batchID)
		return true
	}
	return false
}

// ListBatches returns the IDs of all stored batches
func (s *FlightServer) ListBatches() []string {
	s.batchesMu.RLock()
	defer s.batchesMu.RUnlock()

	batchIDs := make([]string, 0, len(s.batches))
	for id := range s.batches {
		batchIDs = append(batchIDs, id)
	}
	return batchIDs
}

// generateBatchID generates a unique ID for a batch
func generateBatchID() string {
	return uuid.New().String()
}
