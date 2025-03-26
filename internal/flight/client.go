// Package flight provides Arrow Flight client and server implementations for batch operations.
package flight

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// FlightClient is a client for retrieving and storing Arrow record batches.
type FlightClient struct {
	client    flight.Client
	allocator memory.Allocator
	conn      *grpc.ClientConn
}

// FlightClientConfig contains configuration options for the Flight client.
type FlightClientConfig struct {
	// Address of the Flight server to connect to (e.g., "localhost:8080")
	Addr string

	// Allocator is the memory allocator to use
	Allocator memory.Allocator
}

// NewFlightClient creates a new Flight client.
func NewFlightClient(config FlightClientConfig) (*FlightClient, error) {
	if config.Addr == "" {
		config.Addr = "localhost:8080"
	}

	if config.Allocator == nil {
		config.Allocator = memory.NewGoAllocator()
	}

	// Connect to the Flight server
	conn, err := grpc.Dial(
		config.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024),
			grpc.MaxCallSendMsgSize(64*1024*1024),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Flight server: %w", err)
	}

	// Create a Flight client
	client, err := flight.NewClientWithMiddlewareCtx(
		context.Background(),
		config.Addr,
		nil, // auth handler
		nil, // middleware
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Flight client: %w", err)
	}

	return &FlightClient{
		client:    client,
		allocator: config.Allocator,
		conn:      conn,
	}, nil
}

// StoreBatch stores an Arrow record batch in the Flight server and returns the batch ID.
func (c *FlightClient) StoreBatch(ctx context.Context, batch arrow.Record) (string, error) {
	// Create a FlightDescriptor for the batch
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte("store"), // Command is just a marker, actual ID is returned from server
	}

	// Create a FlightPutWriter to send the batch to the server
	stream, err := c.client.DoPut(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create Flight writer: %w", err)
	}

	// Create a writer for the stream
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(batch.Schema()))

	// Set the descriptor for the first message
	writer.SetFlightDescriptor(descriptor)

	// Write the batch to the stream
	if err := writer.Write(batch); err != nil {
		writer.Close()
		return "", fmt.Errorf("failed to write batch to Flight server: %w", err)
	}

	// Close the writer to flush all data
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("failed to close Flight writer: %w", err)
	}

	// Receive the response from the server
	result, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("failed to receive response from server: %w", err)
	}

	// The server returns the batch ID in the app metadata of the first result
	if len(result.AppMetadata) == 0 {
		return "", fmt.Errorf("no batch ID received from server")
	}

	// Return the batch ID from the server
	return string(result.AppMetadata), nil
}

// GetBatch retrieves an Arrow record batch from the Flight server.
func (c *FlightClient) GetBatch(ctx context.Context, batchID string) (arrow.Record, error) {
	// Get flight info for the batch
	info, err := c.getFlightInfo(ctx, batchID)
	if err != nil {
		return nil, err
	}

	// Validate that we have at least one endpoint
	if len(info.Endpoint) == 0 {
		return nil, fmt.Errorf("no endpoints available for batch %s", batchID)
	}

	// Get the endpoint (should be just one for this simple case)
	endpoint := info.Endpoint[0]

	// Create a ticket from the endpoint
	ticket := endpoint.Ticket

	// DoGet to retrieve the batch
	stream, err := c.client.DoGet(ctx, ticket)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream for batch %s: %w", batchID, err)
	}

	// Create a reader from the stream
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to create record reader for batch %s: %w", batchID, err)
	}
	defer reader.Release()

	// We expect only a single batch to be returned
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("error reading batch %s: %w", batchID, err)
		}
		return nil, fmt.Errorf("no data returned for batch %s", batchID)
	}

	// Get the record and retain it so it's not released when the reader is released
	record := reader.Record()
	record.Retain()

	return record, nil
}

// getFlightInfo retrieves flight info for a batch.
func (c *FlightClient) getFlightInfo(ctx context.Context, batchID string) (*flight.FlightInfo, error) {
	// Create a descriptor for the batch
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(batchID),
	}

	// Get flight info
	info, err := c.client.GetFlightInfo(ctx, descriptor)
	if err != nil {
		return nil, fmt.Errorf("failed to get flight info for batch %s: %w", batchID, err)
	}

	return info, nil
}

// Close releases resources associated with the client.
func (c *FlightClient) Close() error {
	if closer, ok := c.client.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return c.conn.Close()
}
