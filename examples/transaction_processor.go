package main

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FraudScore represents a fraud detection score
type FraudScore struct {
	Score        float64
	Reason       string
	IsFraudulent bool
}

// TransactionProcessor processes transaction data, applies fraud detection and enrichment
type TransactionProcessor struct {
	config              *Config
	allocator           memory.Allocator
	fraudScores         map[string]FraudScore    // Map of transaction ID to fraud score
	processingDurations map[string]time.Duration // Map of batch ID to processing duration
	totalTransactions   int
	fraudulentCount     int
}

// NewTransactionProcessor creates a new transaction processor
func NewTransactionProcessor(config *Config) *TransactionProcessor {
	return &TransactionProcessor{
		config:              config,
		allocator:           memory.NewGoAllocator(),
		fraudScores:         make(map[string]FraudScore),
		processingDurations: make(map[string]time.Duration),
		totalTransactions:   0,
		fraudulentCount:     0,
	}
}

// ProcessBatch implements the BatchProcessor interface
func (p *TransactionProcessor) ProcessBatch(ctx context.Context, batchID string, batch arrow.Record) error {
	startTime := time.Now()

	// Process the batch through the pipeline
	_, err := p.processPipeline(batch)
	if err != nil {
		return fmt.Errorf("failed to process batch %s: %w", batchID, err)
	}

	// Track metrics
	duration := time.Since(startTime)
	p.processingDurations[batchID] = duration
	p.totalTransactions += int(batch.NumRows())

	fmt.Printf("Processed batch %s with %d rows in %v\n", batchID, batch.NumRows(), duration)

	// Return the result (normally you would store this somewhere)
	return nil
}

// processPipeline runs the batch through all processing steps
func (p *TransactionProcessor) processPipeline(batch arrow.Record) (arrow.Record, error) {
	defer batch.Release()

	// Apply fraud detection
	if p.config.Processing.FraudDetection.Enabled {
		detectedBatch, err := p.detectFraud(batch)
		if err != nil {
			return nil, fmt.Errorf("fraud detection failed: %w", err)
		}
		batch = detectedBatch
	}

	// Apply data enrichment
	if p.config.Processing.Enrichment.Enabled {
		enrichedBatch, err := p.enrichData(batch)
		if err != nil {
			return nil, fmt.Errorf("data enrichment failed: %w", err)
		}
		batch = enrichedBatch
	}

	// Retain the final batch to prevent release by deferred function
	batch.Retain()
	return batch, nil
}

// detectFraud applies fraud detection logic to the batch
func (p *TransactionProcessor) detectFraud(batch arrow.Record) (arrow.Record, error) {
	// Get schema and create builder for the result
	schema := batch.Schema()
	fields := schema.Fields()

	// Add new fields for fraud detection
	newFields := make([]arrow.Field, len(fields)+3)
	copy(newFields, fields)
	newFields[len(fields)] = arrow.Field{Name: "fraud_score", Type: arrow.PrimitiveTypes.Float64}
	newFields[len(fields)+1] = arrow.Field{Name: "fraud_reason", Type: arrow.BinaryTypes.String}
	newFields[len(fields)+2] = arrow.Field{Name: "is_fraudulent", Type: &arrow.BooleanType{}}

	// Create a new schema with the additional fields
	newSchema := arrow.NewSchema(newFields, nil)

	// Create a record builder with the new schema
	recordBuilder := array.NewRecordBuilder(p.allocator, newSchema)
	defer recordBuilder.Release()

	// Copy all original columns
	for i, col := range batch.Columns() {
		switch col := col.(type) {
		case *array.String:
			builder := recordBuilder.Field(i).(*array.StringBuilder)
			for j := 0; j < col.Len(); j++ {
				if col.IsNull(j) {
					builder.AppendNull()
				} else {
					builder.Append(col.Value(j))
				}
			}
		case *array.Float64:
			builder := recordBuilder.Field(i).(*array.Float64Builder)
			for j := 0; j < col.Len(); j++ {
				if col.IsNull(j) {
					builder.AppendNull()
				} else {
					builder.Append(col.Value(j))
				}
			}
		case *array.Boolean:
			builder := recordBuilder.Field(i).(*array.BooleanBuilder)
			for j := 0; j < col.Len(); j++ {
				if col.IsNull(j) {
					builder.AppendNull()
				} else {
					builder.Append(col.Value(j))
				}
			}
		case *array.Timestamp:
			builder := recordBuilder.Field(i).(*array.TimestampBuilder)
			for j := 0; j < col.Len(); j++ {
				if col.IsNull(j) {
					builder.AppendNull()
				} else {
					builder.Append(col.Value(j))
				}
			}
		default:
			// For other types, we would add specific handling
			return nil, fmt.Errorf("unsupported column type: %T", col)
		}
	}

	// Find indices for columns we need for fraud detection
	idIdx := p.findColumnIndex(schema, "id")
	amountIdx := p.findColumnIndex(schema, "amount")
	transactionTypeIdx := p.findColumnIndex(schema, "transaction_type")
	isOnlineIdx := p.findColumnIndex(schema, "is_online")
	ipAddressIdx := p.findColumnIndex(schema, "ip_address")
	merchantCountryIdx := p.findColumnIndex(schema, "merchant_country")
	cardBINIdx := p.findColumnIndex(schema, "card_bin")

	if idIdx < 0 || amountIdx < 0 || transactionTypeIdx < 0 || isOnlineIdx < 0 {
		return nil, fmt.Errorf("required columns for fraud detection not found")
	}

	// Get columns for fraud detection
	idColumn := batch.Column(idIdx).(*array.String)
	amountColumn := batch.Column(amountIdx).(*array.Float64)
	transactionTypeColumn := batch.Column(transactionTypeIdx).(*array.String)
	isOnlineColumn := batch.Column(isOnlineIdx).(*array.Boolean)

	// Builders for fraud detection fields
	fraudScoreBuilder := recordBuilder.Field(len(fields)).(*array.Float64Builder)
	fraudReasonBuilder := recordBuilder.Field(len(fields) + 1).(*array.StringBuilder)
	isFraudulentBuilder := recordBuilder.Field(len(fields) + 2).(*array.BooleanBuilder)

	// Apply fraud detection rules to each row
	for i := 0; i < int(batch.NumRows()); i++ {
		// Get transaction data
		id := idColumn.Value(i)
		amount := amountColumn.Value(i)
		transactionType := transactionTypeColumn.Value(i)
		isOnline := isOnlineColumn.Value(i)

		var ipAddress string
		if ipAddressIdx >= 0 && !batch.Column(ipAddressIdx).IsNull(i) {
			ipAddress = batch.Column(ipAddressIdx).(*array.String).Value(i)
		}

		var merchantCountry string
		if merchantCountryIdx >= 0 {
			merchantCountry = batch.Column(merchantCountryIdx).(*array.String).Value(i)
		}

		var cardBIN string
		if cardBINIdx >= 0 {
			cardBIN = batch.Column(cardBINIdx).(*array.String).Value(i)
		}

		// Apply fraud detection rules
		fraudScore, reason := p.calculateFraudScore(amount, transactionType, isOnline, ipAddress, merchantCountry, cardBIN)
		isFraudulent := fraudScore >= p.config.Processing.FraudDetection.Threshold

		// Store fraud score
		p.fraudScores[id] = FraudScore{
			Score:        fraudScore,
			Reason:       reason,
			IsFraudulent: isFraudulent,
		}

		// Update fraud count
		if isFraudulent {
			p.fraudulentCount++
		}

		// Append fraud detection fields
		fraudScoreBuilder.Append(fraudScore)
		fraudReasonBuilder.Append(reason)
		isFraudulentBuilder.Append(isFraudulent)
	}

	// Build the new record
	result := recordBuilder.NewRecord()
	return result, nil
}

// calculateFraudScore applies fraud detection rules to a transaction
func (p *TransactionProcessor) calculateFraudScore(amount float64, transactionType string, isOnline bool, ipAddress, merchantCountry, cardBIN string) (float64, string) {
	var score float64
	var reasons []string

	// Rule 1: Large transaction amounts
	if amount > 500 {
		score += 0.3
		reasons = append(reasons, "large_amount")
	}

	// Rule 2: Online transactions (higher risk)
	if isOnline {
		score += 0.2
		reasons = append(reasons, "online_transaction")

		// Rule 2a: Suspicious IP addresses (simplistic example)
		if strings.HasPrefix(ipAddress, "192.168.") {
			score += 0.1
			reasons = append(reasons, "internal_ip")
		}
	}

	// Rule 3: Specific transaction types
	if transactionType == "authorization" {
		score += 0.1
		reasons = append(reasons, "authorization")
	}

	// Rule 4: Cross-border transactions (simplified example)
	if merchantCountry != "US" && merchantCountry != "" {
		score += 0.2
		reasons = append(reasons, "cross_border")
	}

	// Rule 5: Certain card BINs (simplified example)
	if strings.HasPrefix(cardBIN, "4") {
		score += 0.1
		reasons = append(reasons, "high_risk_bin")
	}

	// Ensure score is between 0 and 1
	score = math.Min(1.0, score)
	score = math.Max(0.0, score)

	return score, strings.Join(reasons, ",")
}

// enrichData applies data enrichment to the batch
func (p *TransactionProcessor) enrichData(batch arrow.Record) (arrow.Record, error) {
	// In a real implementation, this would add additional data from external sources
	// For this example, we'll just return the original batch
	batch.Retain()
	return batch, nil
}

// findColumnIndex finds the index of a column by name
func (p *TransactionProcessor) findColumnIndex(schema *arrow.Schema, name string) int {
	for i, field := range schema.Fields() {
		if field.Name == name {
			return i
		}
	}
	return -1
}

// GetStatistics returns processing statistics
func (p *TransactionProcessor) GetStatistics() map[string]interface{} {
	var totalDuration time.Duration
	for _, duration := range p.processingDurations {
		totalDuration += duration
	}

	var throughput float64
	if totalDuration > 0 {
		throughput = float64(p.totalTransactions) / totalDuration.Seconds()
	}

	return map[string]interface{}{
		"total_transactions": p.totalTransactions,
		"fraudulent_count":   p.fraudulentCount,
		"fraud_percentage":   float64(p.fraudulentCount) / float64(p.totalTransactions) * 100,
		"total_duration":     totalDuration,
		"throughput":         throughput,
		"batch_count":        len(p.processingDurations),
	}
}
