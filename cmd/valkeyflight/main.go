// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/valkeyflight/coordinator"
	"github.com/TFMV/valkeyflight/internal/flight"
	"github.com/TFMV/valkeyflight/internal/worker"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Options for command-line flags
type Options struct {
	// Mode determines which components to run
	Mode string

	// Valkey connection parameters
	ValKeyAddr     string
	ValKeyPassword string

	// Flight connection parameters
	FlightAddr string

	// Worker parameters
	WorkerCount  int
	PollInterval time.Duration
	MaxRetries   int

	// Log level
	Verbose bool
}

func main() {
	// Parse command line flags
	opts := parseFlags()

	// Set up context with signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Printf("Received signal %v, initiating shutdown...\n", sig)
		cancel()
	}()

	// Create allocator for Arrow
	allocator := memory.NewGoAllocator()

	// Start components based on mode
	var coord *coordinator.Coordinator
	var flightClient *flight.FlightClient
	var work *worker.Worker

	// Start components based on the selected mode
	switch opts.Mode {
	case "all", "coordinator", "worker":
		// Create coordinator
		coordCfg := coordinator.Config{
			ValKeyAddr:     opts.ValKeyAddr,
			ValKeyPassword: opts.ValKeyPassword,
			MaxRetries:     opts.MaxRetries,
		}

		var err error
		coord, err = coordinator.New(coordCfg)
		if err != nil {
			log.Fatalf("Failed to create coordinator: %v", err)
		}
		fmt.Println("Coordinator started")
	}

	switch opts.Mode {
	case "all", "flight", "worker":
		// Create Flight client
		flightCfg := flight.FlightClientConfig{
			Addr:      opts.FlightAddr,
			Allocator: allocator,
		}

		var err error
		flightClient, err = flight.NewFlightClient(flightCfg)
		if err != nil {
			log.Fatalf("Failed to create Flight client: %v", err)
		}
		fmt.Println("Flight client started")
	}

	switch opts.Mode {
	case "all", "worker":
		// Create and start worker
		workerCfg := worker.Config{
			Coordinator:  coord,
			FlightClient: flightClient,
			WorkerCount:  opts.WorkerCount,
			PollInterval: opts.PollInterval,
		}

		var err error
		work, err = worker.New(workerCfg)
		if err != nil {
			log.Fatalf("Failed to create worker: %v", err)
		}

		work.Start()
		fmt.Println("Worker started")
	}

	// Wait for shutdown signal
	<-ctx.Done()
	fmt.Println("Shutdown initiated")

	// Create a context with timeout for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown components in reverse order
	if work != nil {
		if err := work.Shutdown(shutdownCtx); err != nil {
			fmt.Printf("Error shutting down worker: %v\n", err)
		}
	}

	if flightClient != nil {
		if err := flightClient.Close(); err != nil {
			fmt.Printf("Error closing Flight client: %v\n", err)
		}
	}

	if coord != nil {
		if err := coord.Shutdown(shutdownCtx); err != nil {
			fmt.Printf("Error shutting down coordinator: %v\n", err)
		}
	}

	fmt.Println("Shutdown complete")
}

func parseFlags() *Options {
	opts := &Options{}

	// Define flags
	flag.StringVar(&opts.Mode, "mode", "all", "Operation mode: 'all', 'coordinator', 'worker', or 'flight'")
	flag.StringVar(&opts.ValKeyAddr, "valkey-addr", "localhost:6379", "Valkey server address")
	flag.StringVar(&opts.ValKeyPassword, "valkey-password", "", "Valkey server password")
	flag.StringVar(&opts.FlightAddr, "flight-addr", "localhost:8080", "Arrow Flight server address")
	flag.IntVar(&opts.WorkerCount, "workers", 2, "Number of worker goroutines")
	flag.DurationVar(&opts.PollInterval, "poll-interval", 5*time.Second, "Polling interval for workers")
	flag.IntVar(&opts.MaxRetries, "max-retries", 3, "Maximum number of retries for failed batches")
	flag.BoolVar(&opts.Verbose, "verbose", false, "Enable verbose logging")

	// Parse flags
	flag.Parse()

	// Validate mode
	switch opts.Mode {
	case "all", "coordinator", "worker", "flight":
		// Valid modes
	default:
		fmt.Printf("Invalid mode: %s\n", opts.Mode)
		flag.Usage()
		os.Exit(1)
	}

	return opts
}
