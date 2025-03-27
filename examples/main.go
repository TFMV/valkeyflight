package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/TFMV/valkeyflight/coordinator"
	"github.com/TFMV/valkeyflight/flight_client"
	"github.com/TFMV/valkeyflight/flight_server"
	"github.com/TFMV/valkeyflight/internal/flight"
	"github.com/TFMV/valkeyflight/internal/worker"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalCh
		fmt.Println("\nReceived termination signal. Shutting down...")
		cancel()
	}()

	// Ensure output directory exists
	if err := os.MkdirAll(config.Output.Directory, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Start the transaction pipeline
	if err := runPipeline(ctx, config); err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	fmt.Println("Pipeline completed successfully")
}

func runPipeline(ctx context.Context, config *Config) error {
	// Step 1: Start Flight Server
	flightServer, err := startFlightServer(config)
	if err != nil {
		return fmt.Errorf("failed to start Flight server: %w", err)
	}
	defer flightServer.Stop()

	// Step 2: Create Flight Client
	flightClient, err := createFlightClient(config)
	if err != nil {
		return fmt.Errorf("failed to create Flight client: %w", err)
	}
	defer flightClient.Close()

	// Step 3: Create Coordinator
	coordinator, err := createCoordinator(config)
	if err != nil {
		return fmt.Errorf("failed to create coordinator: %w", err)
	}
	defer coordinator.Shutdown(ctx)

	// Step 4: Generate test data
	generator := NewTransactionGenerator(config)
	if err := generator.GenerateAllTransactions(); err != nil {
		return fmt.Errorf("failed to generate transactions: %w", err)
	}

	// Step 5: Create and configure processor
	processor := NewTransactionProcessor(config)

	// Step 6: Load and Process Data
	err = loadAndProcessData(ctx, config, flightClient, coordinator, processor)
	if err != nil {
		return fmt.Errorf("failed to load and process data: %w", err)
	}

	// Step 7: Print statistics
	printStatistics(processor)

	return nil
}

func startFlightServer(config *Config) (*flight_server.FlightServer, error) {
	serverConfig := flight_server.ServerConfig{
		Addr:      config.Flight.Address,
		Allocator: memory.NewGoAllocator(),
		TTL:       1 * time.Hour,
	}

	server, err := flight_server.NewFlightServer(serverConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Flight server: %w", err)
	}

	// Start server in a goroutine
	go func() {
		if err := server.Start(); err != nil {
			log.Printf("Flight server error: %v", err)
		}
	}()

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("Flight server started on %s\n", config.Flight.Address)

	return server, nil
}

func createFlightClient(config *Config) (*flight_client.FlightClient, error) {
	// Create a client configuration with the server address
	clientConfig := flight_client.ClientConfig{
		Addr:      config.Flight.Address,
		Allocator: memory.NewGoAllocator(),
	}

	// Create a context for the client
	ctx := context.Background()

	// Create the flight client
	client, err := flight_client.NewFlightClient(ctx, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Flight client: %w", err)
	}

	fmt.Printf("Flight client connected to %s\n", config.Flight.Address)
	return client, nil
}

func createCoordinator(config *Config) (*coordinator.Coordinator, error) {
	coordConfig := coordinator.Config{
		ValKeyAddr:     config.Valkey.Address,
		ValKeyPassword: config.Valkey.Password,
		MaxRetries:     config.Valkey.MaxRetries,
	}

	coord, err := coordinator.New(coordConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator: %w", err)
	}

	fmt.Printf("Coordinator connected to Valkey at %s\n", config.Valkey.Address)
	return coord, nil
}

func loadAndProcessData(ctx context.Context, config *Config, flightClient *flight_client.FlightClient,
	coordinator *coordinator.Coordinator, processor *TransactionProcessor) error {

	// Create an internal flight client directly - this is what the worker expects
	internalFlightClient, err := flight.NewFlightClient(flight.FlightClientConfig{
		Addr:      config.Flight.Address,
		Allocator: memory.NewGoAllocator(),
	})
	if err != nil {
		return fmt.Errorf("failed to create internal flight client: %w", err)
	}
	defer internalFlightClient.Close()

	// Create a worker pool for processing batches
	workerConfig := worker.Config{
		Coordinator:       coordinator,
		FlightClient:      internalFlightClient,
		BatchProcessor:    processor,
		WorkerCount:       config.Workers.Count,
		PollInterval:      config.Workers.PollInterval,
		HeartbeatInterval: config.Valkey.HeartbeatInterval,
		BackoffMin:        config.Workers.Backoff.Min,
		BackoffMax:        config.Workers.Backoff.Max,
		BackoffFactor:     config.Workers.Backoff.Factor,
	}

	workerPool, err := worker.New(workerConfig)
	if err != nil {
		return fmt.Errorf("failed to create worker pool: %w", err)
	}

	// Path to the generated data file
	dataFilePath := filepath.Join(config.DataGeneration.OutputDir, "transactions.arrow")

	// Load batches from the data file and register with coordinator
	err = loadDataToBatches(ctx, dataFilePath, flightClient, coordinator, config)
	if err != nil {
		return fmt.Errorf("failed to load data to batches: %w", err)
	}

	// Start worker pool
	fmt.Printf("Starting worker pool with %d workers\n", config.Workers.Count)
	workerPool.Start()

	// Wait for processing to complete (all batches processed)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Check if there are any pending or processing batches
			pendingBatches, err := coordinator.ListPendingBatches(ctx)
			if err != nil {
				return fmt.Errorf("failed to list pending batches: %w", err)
			}

			// We're done when there are no more pending batches
			if len(pendingBatches) == 0 {
				// Double check if there are any in-progress batches
				time.Sleep(2 * time.Second) // Give a little more time for any last batches
				pendingBatches, _ = coordinator.ListPendingBatches(ctx)
				if len(pendingBatches) == 0 {
					// Shutdown worker pool
					workerPool.Shutdown(ctx)
					return nil
				}
			}
		}
	}
}

func loadDataToBatches(ctx context.Context, dataFilePath string,
	flightClient *flight_client.FlightClient,
	coordinator *coordinator.Coordinator, config *Config) error {

	// Read generated data and store in Arrow Flight server
	// In a real application, you might stream this from a file or other source
	// For this example, we'll simulate by creating batches directly

	// Create data generator to make batches
	generator := NewTransactionGenerator(config)

	// Get batch size from config
	batchSize := config.DataGeneration.BatchSize
	recordCount := config.DataGeneration.RecordCount

	fmt.Printf("Loading %d records in batches of %d...\n", recordCount, batchSize)

	// Calculate number of batches
	numBatches := recordCount / batchSize
	if recordCount%batchSize > 0 {
		numBatches++
	}

	for i := 0; i < numBatches; i++ {
		// For the last batch, adjust size if needed
		size := batchSize
		if i == numBatches-1 && recordCount%batchSize > 0 {
			size = recordCount % batchSize
		}

		// Generate a batch
		batch := generator.GenerateTransactionBatch(size)
		defer batch.Release()

		// Store batch in Flight server
		batchID, err := flightClient.StoreBatch(ctx, batch)
		if err != nil {
			return fmt.Errorf("failed to store batch in Flight server: %w", err)
		}

		// Register batch with coordinator
		if err := coordinator.RegisterBatch(ctx, batchID); err != nil {
			return fmt.Errorf("failed to register batch with coordinator: %w", err)
		}

		if i%10 == 0 || i == numBatches-1 {
			fmt.Printf("Loaded %d/%d batches\n", i+1, numBatches)
		}
	}

	fmt.Printf("Successfully loaded %d batches\n", numBatches)
	return nil
}

func printStatistics(processor *TransactionProcessor) {
	stats := processor.GetStatistics()

	fmt.Println("\n=== Transaction Processing Statistics ===")
	fmt.Printf("Total Transactions: %d\n", stats["total_transactions"])
	fmt.Printf("Fraudulent Transactions: %d (%.2f%%)\n",
		stats["fraudulent_count"], stats["fraud_percentage"])
	fmt.Printf("Processing Duration: %v\n", stats["total_duration"])
	fmt.Printf("Throughput: %.2f records/second\n", stats["throughput"])
	fmt.Printf("Batch Count: %d\n", stats["batch_count"])
}
