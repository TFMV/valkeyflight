#!/bin/bash
set -e

# Function to check if Valkey is running
check_valkey() {
  echo "Checking if Valkey is running..."
  if nc -z localhost 6379 >/dev/null 2>&1; then
    echo "✅ Valkey is running"
    return 0
  else
    echo "❌ Valkey is not running"
    return 1
  fi
}

# Function to start Valkey if not running
start_valkey() {
  echo "Attempting to start Valkey using Docker..."
  if docker --version >/dev/null 2>&1; then
    echo "Docker found, starting Valkey container..."
    docker run --name valkeyflight-example-valkey -d -p 6379:6379 valkey/valkey:latest
    echo "Waiting for Valkey to initialize..."
    sleep 3
    if check_valkey; then
      echo "✅ Valkey started successfully"
    else
      echo "❌ Failed to start Valkey"
      exit 1
    fi
  else
    echo "⚠️  Docker not found. Please start Valkey manually on port 6379"
    exit 1
  fi
}

# Create necessary directories
mkdir -p data output

# Check if Valkey is running
if ! check_valkey; then
  start_valkey
fi

# Build the example
echo "Building transaction pipeline example..."
go build -o transaction_pipeline 

# Run the example
echo "Running transaction pipeline example..."
./transaction_pipeline --config config.yaml

# Cleanup
echo "Pipeline completed. Results available in the output directory." 