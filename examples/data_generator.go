package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/brianvoe/gofakeit/v6"
)

// Transaction represents a financial transaction
type Transaction struct {
	ID              string    `csv:"id" faker:"uuid"`
	Amount          float64   `csv:"amount"`
	Currency        string    `csv:"currency"`
	Timestamp       time.Time `csv:"timestamp"`
	CustomerID      string    `csv:"customer_id"`
	MerchantID      string    `csv:"merchant_id"`
	MerchantName    string    `csv:"merchant_name"`
	MerchantCountry string    `csv:"merchant_country"`
	MerchantCity    string    `csv:"merchant_city"`
	CardBIN         string    `csv:"card_bin"`
	CardLast4       string    `csv:"card_last4"`
	CardExpiry      string    `csv:"card_expiry"`
	CardType        string    `csv:"card_type"`
	TransactionType string    `csv:"transaction_type"`
	Status          string    `csv:"status"`
	IsOnline        bool      `csv:"is_online"`
	IPAddress       string    `csv:"ip_address"`
	UserAgent       string    `csv:"user_agent"`
	DeviceID        string    `csv:"device_id"`
}

// TransactionGenerator generates synthetic transaction data
type TransactionGenerator struct {
	config        *Config
	faker         *gofakeit.Faker
	customerIDs   []string
	merchantIDs   []string
	merchantNames []string
	countries     []string
	cities        map[string][]string
	cardTypes     []string
	transTypes    []string
}

// NewTransactionGenerator creates a new transaction data generator
func NewTransactionGenerator(config *Config) *TransactionGenerator {
	faker := gofakeit.New(int64(config.DataGeneration.Seed))

	// Create a pool of static data to make realistic looking transactions
	countries := []string{"US", "CA", "GB", "FR", "DE", "JP", "AU", "SG", "BR", "MX"}
	cities := map[string][]string{
		"US": {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix"},
		"CA": {"Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"},
		"GB": {"London", "Manchester", "Birmingham", "Glasgow", "Edinburgh"},
		"FR": {"Paris", "Marseille", "Lyon", "Toulouse", "Nice"},
		"DE": {"Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt"},
		"JP": {"Tokyo", "Osaka", "Kyoto", "Yokohama", "Sapporo"},
		"AU": {"Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"},
		"SG": {"Singapore"},
		"BR": {"Sao Paulo", "Rio de Janeiro", "Brasilia", "Salvador", "Fortaleza"},
		"MX": {"Mexico City", "Guadalajara", "Monterrey", "Puebla", "Tijuana"},
	}

	// Generate a set of realistic customer IDs
	customerIDs := make([]string, 1000)
	for i := range customerIDs {
		customerIDs[i] = faker.UUID()
	}

	// Generate a set of merchants
	merchantCount := 200
	merchantIDs := make([]string, merchantCount)
	merchantNames := make([]string, merchantCount)
	for i := range merchantIDs {
		merchantIDs[i] = faker.UUID()
		merchantNames[i] = faker.Company()
	}

	return &TransactionGenerator{
		config:        config,
		faker:         faker,
		customerIDs:   customerIDs,
		merchantIDs:   merchantIDs,
		merchantNames: merchantNames,
		countries:     countries,
		cities:        cities,
		cardTypes:     []string{"Visa", "Mastercard", "Amex", "Discover", "JCB"},
		transTypes:    []string{"purchase", "refund", "authorization", "capture", "void"},
	}
}

// GenerateTransaction creates a single transaction with random but realistic data
func (g *TransactionGenerator) GenerateTransaction() Transaction {
	// Pick a random merchant
	merchantIdx := g.faker.IntRange(0, len(g.merchantIDs)-1)
	merchantID := g.merchantIDs[merchantIdx]
	merchantName := g.merchantNames[merchantIdx]

	// Pick a random country and city
	country := g.countries[g.faker.IntRange(0, len(g.countries)-1)]
	cities := g.cities[country]
	city := cities[g.faker.IntRange(0, len(cities)-1)]

	// Generate a timestamp within the last 7 days
	now := time.Now()
	timestamp := now.Add(-time.Duration(g.faker.IntRange(0, 7*24)) * time.Hour)
	timestamp = timestamp.Add(-time.Duration(g.faker.IntRange(0, 60)) * time.Minute)
	timestamp = timestamp.Add(-time.Duration(g.faker.IntRange(0, 60)) * time.Second)

	// Determine if it's an online transaction
	isOnline := g.faker.Bool()

	// Generate card details
	cardType := g.cardTypes[g.faker.IntRange(0, len(g.cardTypes)-1)]
	cardBIN := fmt.Sprintf("%d", g.faker.IntRange(400000, 499999))
	cardLast4 := fmt.Sprintf("%04d", g.faker.IntRange(0, 9999))

	// Generate expiry date (1-5 years in the future)
	expYear := now.Year() + g.faker.IntRange(1, 5)
	expMonth := g.faker.IntRange(1, 12)
	cardExpiry := fmt.Sprintf("%02d/%02d", expMonth, expYear%100)

	// Transaction type and amount
	transactionType := g.transTypes[g.faker.IntRange(0, len(g.transTypes)-1)]
	var amount float64
	var status string

	// Make amount realistic based on transaction type
	switch transactionType {
	case "refund":
		amount = -float64(g.faker.IntRange(1, 200)) - g.faker.Float64()
		status = "completed"
	case "void":
		amount = 0
		status = "voided"
	case "authorization":
		amount = float64(g.faker.IntRange(1, 1000)) + g.faker.Float64()
		status = "pending"
	default:
		amount = float64(g.faker.IntRange(1, 1000)) + g.faker.Float64()
		status = "completed"
	}
	amount = float64(int(amount*100)) / 100 // Round to 2 decimal places

	// Create the transaction
	transaction := Transaction{
		ID:              g.faker.UUID(),
		Amount:          amount,
		Currency:        "USD",
		Timestamp:       timestamp,
		CustomerID:      g.customerIDs[g.faker.IntRange(0, len(g.customerIDs)-1)],
		MerchantID:      merchantID,
		MerchantName:    merchantName,
		MerchantCountry: country,
		MerchantCity:    city,
		CardBIN:         cardBIN,
		CardLast4:       cardLast4,
		CardExpiry:      cardExpiry,
		CardType:        cardType,
		TransactionType: transactionType,
		Status:          status,
		IsOnline:        isOnline,
	}

	// Add online-specific fields
	if isOnline {
		transaction.IPAddress = g.faker.IPv4Address()
		transaction.UserAgent = g.faker.UserAgent()
		transaction.DeviceID = g.faker.UUID()
	}

	return transaction
}

// GenerateTransactionBatch generates a batch of transactions as an Arrow record
func (g *TransactionGenerator) GenerateTransactionBatch(size int) arrow.Record {
	// Create Arrow schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
		{Name: "currency", Type: arrow.BinaryTypes.String},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ms},
		{Name: "customer_id", Type: arrow.BinaryTypes.String},
		{Name: "merchant_id", Type: arrow.BinaryTypes.String},
		{Name: "merchant_name", Type: arrow.BinaryTypes.String},
		{Name: "merchant_country", Type: arrow.BinaryTypes.String},
		{Name: "merchant_city", Type: arrow.BinaryTypes.String},
		{Name: "card_bin", Type: arrow.BinaryTypes.String},
		{Name: "card_last4", Type: arrow.BinaryTypes.String},
		{Name: "card_expiry", Type: arrow.BinaryTypes.String},
		{Name: "card_type", Type: arrow.BinaryTypes.String},
		{Name: "transaction_type", Type: arrow.BinaryTypes.String},
		{Name: "status", Type: arrow.BinaryTypes.String},
		{Name: "is_online", Type: &arrow.BooleanType{}},
		{Name: "ip_address", Type: arrow.BinaryTypes.String},
		{Name: "user_agent", Type: arrow.BinaryTypes.String},
		{Name: "device_id", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create array builders
	allocator := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(allocator, schema)
	defer recordBuilder.Release()

	idBuilder := recordBuilder.Field(0).(*array.StringBuilder)
	amountBuilder := recordBuilder.Field(1).(*array.Float64Builder)
	currencyBuilder := recordBuilder.Field(2).(*array.StringBuilder)
	timestampBuilder := recordBuilder.Field(3).(*array.TimestampBuilder)
	customerIDBuilder := recordBuilder.Field(4).(*array.StringBuilder)
	merchantIDBuilder := recordBuilder.Field(5).(*array.StringBuilder)
	merchantNameBuilder := recordBuilder.Field(6).(*array.StringBuilder)
	merchantCountryBuilder := recordBuilder.Field(7).(*array.StringBuilder)
	merchantCityBuilder := recordBuilder.Field(8).(*array.StringBuilder)
	cardBINBuilder := recordBuilder.Field(9).(*array.StringBuilder)
	cardLast4Builder := recordBuilder.Field(10).(*array.StringBuilder)
	cardExpiryBuilder := recordBuilder.Field(11).(*array.StringBuilder)
	cardTypeBuilder := recordBuilder.Field(12).(*array.StringBuilder)
	transactionTypeBuilder := recordBuilder.Field(13).(*array.StringBuilder)
	statusBuilder := recordBuilder.Field(14).(*array.StringBuilder)
	isOnlineBuilder := recordBuilder.Field(15).(*array.BooleanBuilder)
	ipAddressBuilder := recordBuilder.Field(16).(*array.StringBuilder)
	userAgentBuilder := recordBuilder.Field(17).(*array.StringBuilder)
	deviceIDBuilder := recordBuilder.Field(18).(*array.StringBuilder)

	// Generate and add transactions
	for i := 0; i < size; i++ {
		tx := g.GenerateTransaction()

		idBuilder.Append(tx.ID)
		amountBuilder.Append(tx.Amount)
		currencyBuilder.Append(tx.Currency)
		timestampBuilder.Append(arrow.Timestamp(tx.Timestamp.UnixMilli()))
		customerIDBuilder.Append(tx.CustomerID)
		merchantIDBuilder.Append(tx.MerchantID)
		merchantNameBuilder.Append(tx.MerchantName)
		merchantCountryBuilder.Append(tx.MerchantCountry)
		merchantCityBuilder.Append(tx.MerchantCity)
		cardBINBuilder.Append(tx.CardBIN)
		cardLast4Builder.Append(tx.CardLast4)
		cardExpiryBuilder.Append(tx.CardExpiry)
		cardTypeBuilder.Append(tx.CardType)
		transactionTypeBuilder.Append(tx.TransactionType)
		statusBuilder.Append(tx.Status)
		isOnlineBuilder.Append(tx.IsOnline)

		if tx.IsOnline {
			ipAddressBuilder.Append(tx.IPAddress)
			userAgentBuilder.Append(tx.UserAgent)
			deviceIDBuilder.Append(tx.DeviceID)
		} else {
			ipAddressBuilder.AppendNull()
			userAgentBuilder.AppendNull()
			deviceIDBuilder.AppendNull()
		}
	}

	// Build the record
	return recordBuilder.NewRecord()
}

// GenerateTransactionsToFile generates transactions and saves them to an Arrow IPC file
func (g *TransactionGenerator) GenerateTransactionsToFile(count int, batchSize int, outputPath string) error {
	// Create output directory if it doesn't exist
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Create output file
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	// Create Arrow file writer
	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(g.GenerateTransactionBatch(1).Schema()))
	if err != nil {
		return fmt.Errorf("failed to create Arrow file writer: %w", err)
	}
	defer writer.Close()

	// Generate and write batches
	numBatches := count / batchSize
	if count%batchSize > 0 {
		numBatches++
	}

	for i := 0; i < numBatches; i++ {
		// For the last batch, adjust size if needed
		size := batchSize
		if i == numBatches-1 && count%batchSize > 0 {
			size = count % batchSize
		}

		batch := g.GenerateTransactionBatch(size)
		defer batch.Release()

		if err := writer.Write(batch); err != nil {
			return fmt.Errorf("failed to write batch to file: %w", err)
		}
	}

	return nil
}

// GenerateAllTransactions generates the complete set of transactions as specified in the config
func (g *TransactionGenerator) GenerateAllTransactions() error {
	recordCount := g.config.DataGeneration.RecordCount
	batchSize := g.config.DataGeneration.BatchSize
	outputDir := g.config.DataGeneration.OutputDir

	// Ensure output directory exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	fmt.Printf("Generating %d transactions in batches of %d...\n", recordCount, batchSize)

	// Generate to a single file
	outputFile := filepath.Join(outputDir, "transactions.arrow")
	err := g.GenerateTransactionsToFile(recordCount, batchSize, outputFile)
	if err != nil {
		return fmt.Errorf("failed to generate transactions: %w", err)
	}

	fmt.Printf("Generated %d synthetic transactions to %s\n", recordCount, outputFile)
	return nil
}
