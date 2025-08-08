package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/GolangDemoGSRKafka/pkg/config"
	pb "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/GolangDemoGSRKafka/pkg/proto"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
)

func main() {
	// Command line arguments
	var (
		brokers    = flag.String("brokers", "localhost:9092", "Kafka brokers (comma-separated)")
		userTopic  = flag.String("user-topic", "user-events", "Topic for user messages")
		orderTopic = flag.String("order-topic", "order-events", "Topic for order messages")
		groupID    = flag.String("group-id", "gsr-demo-consumer", "Consumer group ID")
		topics     = flag.String("topics", "user-events,order-events", "Topics to consume (comma-separated)")
		configPath = flag.String("config", "./gsr.properties", "Path to the config file")
	)
	flag.Parse()

	cfg := config.NewConfig(*brokers, *userTopic, *orderTopic, *configPath)

	log.Printf("Starting Kafka Consumer with GSR...")
	log.Printf("Kafka Brokers: %v", cfg.Kafka.Brokers)
	log.Printf("Topics: %s", *topics)
	log.Printf("Group ID: %s", *groupID)

	// Parse topics
	topicList := parseTopics(*topics)

	// Set up readers for each topic
	readers := make(map[string]*kafka.Reader)
	for _, topic := range topicList {
		readers[topic] = kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.Kafka.Brokers,
			Topic:          topic,
			GroupID:        *groupID,
			StartOffset:    kafka.FirstOffset,
			CommitInterval: time.Second,
			MaxBytes:       10e6, // 10MB
		})
	}

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create GSR deserializers for each message type

	// Start consuming from each topic in separate goroutines
	for topic, topicReader := range readers {
		go func(topicName string, r *kafka.Reader) {
			defer r.Close()

			userDeserializer, err := createUserDeserializer(cfg.AWS.ConfigPath)
			if err != nil {
				log.Fatalf("Failed to create user deserializer: %v", err)
			}

			orderDeserializer, err := createOrderDeserializer(cfg.AWS.ConfigPath)
			if err != nil {
				log.Fatalf("Failed to create order deserializer: %v", err)
			}

			log.Printf("Started consuming from topic: %s", topicName)

			for {
				select {
				case <-ctx.Done():
					log.Printf("Stopping consumer for topic: %s", topicName)
					return
				default:
					// Set a read timeout to allow checking for cancellation
					readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
					log.Printf("Reading message from topic: %s", topicName)
					message, err := r.ReadMessage(readCtx)
					readCancel()

					if err != nil {
						if ctx.Err() != nil {
							// Context was cancelled, stop consuming
							return
						}
						if strings.Contains(err.Error(), "context deadline exceeded") {
							// Timeout is expected, continue
							continue
						}
						log.Printf("Error reading message from %s: %v", topicName, err)
						continue
					}

					// Process the message based on topic
					if err := processMessage(topicName, message, userDeserializer, orderDeserializer); err != nil {
						log.Printf("Error processing message from %s: %v", topicName, err)
					}
				}
			}
		}(topic, topicReader)
	}

	log.Println("Consumer started. Press Ctrl+C to stop...")

	// Wait for interrupt signal
	<-sigChan
	log.Println("Received interrupt signal, shutting down...")

	// Cancel context to stop all consumers
	cancel()

	// Give consumers time to shut down gracefully
	time.Sleep(2 * time.Second)
	log.Println("Consumer stopped")
}

// createUserDeserializer creates a GSR deserializer for User messages
func createUserDeserializer(gsrConfigAbsolutePath string) (*deserializer.Deserializer, error) {
	// Create a sample User message to get the message descriptor
	sampleUser := &pb.User{}
	messageDescriptor := sampleUser.ProtoReflect().Descriptor()


	// Create Protobuf configuration (same as integration test)
	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: messageDescriptor,
		common.GSRConfigPathKey: 			gsrConfigAbsolutePath, 
	}
	config := common.NewConfiguration(configMap)

	// Create GSR deserializer
	gsrDeserializer, err := deserializer.NewDeserializer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create GSR deserializer for User: %w", err)
	}

	return gsrDeserializer, nil
}

// createOrderDeserializer creates a GSR deserializer for Order messages
func createOrderDeserializer(gsrConfigAbsolutePath string) (*deserializer.Deserializer, error) {
	// Create a sample Order message to get the message descriptor
	sampleOrder := &pb.Order{}
	messageDescriptor := sampleOrder.ProtoReflect().Descriptor()

	// Create Protobuf configuration (same as integration test)
	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: messageDescriptor,
		common.GSRConfigPathKey: 			gsrConfigAbsolutePath, 
	}
	config := common.NewConfiguration(configMap)

	// Create GSR deserializer
	gsrDeserializer, err := deserializer.NewDeserializer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create GSR deserializer for Order: %w", err)
	}

	return gsrDeserializer, nil
}

// processMessage processes a Kafka message using appropriate GSR deserializer
func processMessage(topicName string, message kafka.Message, userDeserializer, orderDeserializer *deserializer.Deserializer) error {
	log.Printf("Received message from %s: key=%s, partition=%d, offset=%d, size=%d bytes",
		topicName, string(message.Key), message.Partition, message.Offset, len(message.Value))

	// Determine message type based on topic name
	if strings.Contains(topicName, "user") {
		return processUserMessage(message, userDeserializer)
	} else if strings.Contains(topicName, "order") {
		return processOrderMessage(message, orderDeserializer)
	}

	log.Printf("Unknown topic: %s", topicName)
	return nil
}

// processUserMessage deserializes and processes a User message
func processUserMessage(message kafka.Message, userDeserializer *deserializer.Deserializer) error {
	// Check if data can be deserialized
	canDeserialize, err := userDeserializer.CanDeserialize(message.Value)
	if err != nil {
		return fmt.Errorf("failed to check if user message can be deserialized: %w", err)
	}
	if !canDeserialize {
		return fmt.Errorf("user message cannot be deserialized")
	}

	// Deserialize the GSR-encoded data
	deserializedMessage, err := userDeserializer.Deserialize(message.Topic, message.Value)
	if err != nil {
		return fmt.Errorf("failed to deserialize user message: %w", err)
	}

	// Convert dynamic proto message to concrete User message (following integration test pattern)
	protoMessage, ok := deserializedMessage.(proto.Message)
	if !ok {
		return fmt.Errorf("deserialized message should be a proto.Message, got %T", deserializedMessage)
	}

	user, err := convertDynamicToUser(protoMessage)
	if err != nil {
		return fmt.Errorf("failed to convert dynamic message to User: %w", err)
	}

	// Print the user information
	printUser(user)
	return nil
}

// processOrderMessage deserializes and processes an Order message
func processOrderMessage(message kafka.Message, orderDeserializer *deserializer.Deserializer) error {
	// Check if data can be deserialized
	canDeserialize, err := orderDeserializer.CanDeserialize(message.Value)
	if err != nil {
		return fmt.Errorf("failed to check if order message can be deserialized: %w", err)
	}
	if !canDeserialize {
		return fmt.Errorf("order message cannot be deserialized")
	}

	// Deserialize the GSR-encoded data
	deserializedMessage, err := orderDeserializer.Deserialize(message.Topic, message.Value)
	if err != nil {
		return fmt.Errorf("failed to deserialize order message: %w", err)
	}

	// Convert dynamic proto message to concrete Order message (following integration test pattern)
	protoMessage, ok := deserializedMessage.(proto.Message)
	if !ok {
		return fmt.Errorf("deserialized message should be a proto.Message, got %T", deserializedMessage)
	}

	order, err := convertDynamicToOrder(protoMessage)
	if err != nil {
		return fmt.Errorf("failed to convert dynamic message to Order: %w", err)
	}

	// Print the order information
	printOrder(order)
	return nil
}

// convertDynamicToUser converts a dynamic protobuf message to a concrete User message (following integration test pattern)
func convertDynamicToUser(dynamic proto.Message) (*pb.User, error) {
	// Serialize the dynamic message to bytes
	data, err := proto.Marshal(dynamic)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal dynamic user message: %w", err)
	}

	// Deserialize into the concrete User type
	concrete := &pb.User{}
	if err := proto.Unmarshal(data, concrete); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to User: %w", err)
	}

	return concrete, nil
}

// convertDynamicToOrder converts a dynamic protobuf message to a concrete Order message (following integration test pattern)
func convertDynamicToOrder(dynamic proto.Message) (*pb.Order, error) {
	// Serialize the dynamic message to bytes
	data, err := proto.Marshal(dynamic)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal dynamic order message: %w", err)
	}

	// Deserialize into the concrete Order type
	concrete := &pb.Order{}
	if err := proto.Unmarshal(data, concrete); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to Order: %w", err)
	}

	return concrete, nil
}

// printUser displays user information in a readable format
func printUser(user *pb.User) {
	log.Println("=== USER MESSAGE ===")
	log.Printf("ID: %d", user.Id)
	log.Printf("Name: %s", user.Name)
	log.Printf("Email: %s", user.Email)
	log.Printf("Age: %d", user.Age)
	log.Printf("Hobbies: %v", user.Hobbies)
	if user.Address != nil {
		log.Printf("Address: %s, %s, %s %s, %s",
			user.Address.Street, user.Address.City, user.Address.State,
			user.Address.ZipCode, user.Address.Country)
	}
	log.Println("==================")
}

// printOrder displays order information in a readable format
func printOrder(order *pb.Order) {
	log.Println("=== ORDER MESSAGE ===")
	log.Printf("ID: %d", order.Id)
	log.Printf("User ID: %d", order.UserId)
	log.Printf("Total Amount: $%.2f", order.TotalAmount)
	log.Printf("Status: %s", order.Status.String())
	if order.CreatedAt != nil {
		log.Printf("Created At: %s", order.CreatedAt.AsTime().Format(time.RFC3339))
	}
	if len(order.Items) > 0 {
		log.Printf("Items (%d):", len(order.Items))
		for _, item := range order.Items {
			log.Printf("  - %s (ID: %d): %d x $%.2f = $%.2f",
				item.ProductName, item.ProductId, item.Quantity,
				item.UnitPrice, float64(item.Quantity)*item.UnitPrice)
		}
	}
	log.Println("====================")
}

// parseTopics splits comma-separated topics string and trims whitespace
func parseTopics(topics string) []string {
	parts := strings.Split(topics, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
