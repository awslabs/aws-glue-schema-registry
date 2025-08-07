package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/GolangDemoGSRKafka/pkg/config"
	pb "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/GolangDemoGSRKafka/pkg/proto"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
)

func main() {
	// Command line arguments
	var (
		brokers    = flag.String("brokers", "localhost:9092", "Kafka brokers (comma-separated)")
		userTopic  = flag.String("user-topic", "user-events", "Topic for user messages")
		orderTopic = flag.String("order-topic", "order-events", "Topic for order messages")
		awsRegion  = flag.String("aws-region", "us-east-1", "AWS region for GSR")
	)
	flag.Parse()

	cfg := config.NewConfig(*brokers, *userTopic, *orderTopic, *awsRegion)

	log.Printf("Starting Kafka Producer with GSR...")
	log.Printf("Kafka Brokers: %v", cfg.Kafka.Brokers)
	log.Printf("User Topic: %s", cfg.Kafka.UserTopic)
	log.Printf("Order Topic: %s", cfg.Kafka.OrderTopic)

	// Create topics if they don't exist
	if err := createTopics(cfg); err != nil {
		log.Fatalf("Failed to create topics: %v", err)
	}

	// Create Kafka writers
	userWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.UserTopic,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	defer userWriter.Close()

	orderWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.OrderTopic,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	defer orderWriter.Close()

	// Process users
	if err := processUsers(cfg, userWriter); err != nil {
		log.Fatalf("Failed to process users: %v", err)
	}

	// Process orders
	if err := processOrders(cfg, orderWriter); err != nil {
		log.Fatalf("Failed to process orders: %v", err)
	}

	log.Println("Producer finished successfully")
}

func processUsers(cfg *config.Config, writer *kafka.Writer) error {
	// Read user data from JSON file
	usersData, err := ioutil.ReadFile("data/users.json")
	if err != nil {
		return fmt.Errorf("failed to read users.json: %w", err)
	}

	var users []map[string]interface{}
	if err := json.Unmarshal(usersData, &users); err != nil {
		return fmt.Errorf("failed to parse users JSON: %w", err)
	}

	// Create a sample User message to get the message descriptor
	sampleUser := &pb.User{}
	messageDescriptor := sampleUser.ProtoReflect().Descriptor().ParentFile()

	// Create Protobuf configuration (same as integration test)
	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: messageDescriptor,
	}
	config := common.NewConfiguration(configMap)

	// Create GSR serializer once for all User messages
	gsrSerializer, err := serializer.NewSerializer(config)
	if err != nil {
		return fmt.Errorf("failed to create GSR serializer for User: %w", err)
	}

	log.Printf("Processing %d users...", len(users))

	for i, userData := range users {
		// Convert JSON to protobuf message
		user := &pb.User{
			Id:      int64(userData["id"].(float64)),
			Name:    userData["name"].(string),
			Email:   userData["email"].(string),
			Age:     int32(userData["age"].(float64)),
			Hobbies: convertStringSlice(userData["hobbies"].([]interface{})),
		}

		// Handle address
		if addrData, ok := userData["address"].(map[string]interface{}); ok {
			user.Address = &pb.Address{
				Street:  addrData["street"].(string),
				City:    addrData["city"].(string),
				State:   addrData["state"].(string),
				ZipCode: addrData["zip_code"].(string),
				Country: addrData["country"].(string),
			}
		}

		// Serialize with GSR (reusing the same serializer)
		gsrBytes, err := gsrSerializer.Serialize(cfg.Kafka.UserTopic, user)
		if err != nil {
			return fmt.Errorf("failed to GSR serialize user %d: %w", i, err)
		}

		// Send to Kafka
		message := kafka.Message{
			Key:   []byte(fmt.Sprintf("user-%d", user.Id)),
			Value: gsrBytes,
			Time:  time.Now(),
		}

		if err := writer.WriteMessages(context.Background(), message); err != nil {
			return fmt.Errorf("failed to send user %d to Kafka: %w", i, err)
		}

		log.Printf("Sent user: %s (ID: %d) - %d bytes", user.Name, user.Id, len(gsrBytes))
	}

	return nil
}

func processOrders(cfg *config.Config, writer *kafka.Writer) error {
	// Read order data from JSON file
	ordersData, err := ioutil.ReadFile("data/orders.json")
	if err != nil {
		return fmt.Errorf("failed to read orders.json: %w", err)
	}

	var orders []map[string]interface{}
	if err := json.Unmarshal(ordersData, &orders); err != nil {
		return fmt.Errorf("failed to parse orders JSON: %w", err)
	}

	// Create a sample Order message to get the message descriptor
	sampleOrder := &pb.Order{}
	messageDescriptor := sampleOrder.ProtoReflect().Descriptor().ParentFile()

	// Create Protobuf configuration (same as integration test)
	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: messageDescriptor,
	}
	config := common.NewConfiguration(configMap)

	// Create GSR serializer once for all Order messages
	gsrSerializer, err := serializer.NewSerializer(config)
	if err != nil {
		return fmt.Errorf("failed to create GSR serializer for Order: %w", err)
	}

	log.Printf("Processing %d orders...", len(orders))

	for i, orderData := range orders {
		// Convert JSON to protobuf message
		order := &pb.Order{
			Id:          int64(orderData["id"].(float64)),
			UserId:      int64(orderData["user_id"].(float64)),
			TotalAmount: orderData["total_amount"].(float64),
			Status:      pb.OrderStatus(int32(orderData["status"].(float64))),
		}

		// Handle created_at timestamp
		if createdAtStr, ok := orderData["created_at"].(string); ok {
			if createdAt, err := time.Parse(time.RFC3339, createdAtStr); err == nil {
				order.CreatedAt = timestamppb.New(createdAt)
			}
		}

		// Handle items
		if itemsData, ok := orderData["items"].([]interface{}); ok {
			for _, itemInterface := range itemsData {
				itemData := itemInterface.(map[string]interface{})
				item := &pb.OrderItem{
					ProductId:   int64(itemData["product_id"].(float64)),
					ProductName: itemData["product_name"].(string),
					Quantity:    int32(itemData["quantity"].(float64)),
					UnitPrice:   itemData["unit_price"].(float64),
				}
				order.Items = append(order.Items, item)
			}
		}

		// Serialize with GSR (reusing the same serializer)
		gsrBytes, err := gsrSerializer.Serialize(cfg.Kafka.OrderTopic, order)
		if err != nil {
			return fmt.Errorf("failed to GSR serialize order %d: %w", i, err)
		}

		// Send to Kafka
		message := kafka.Message{
			Key:   []byte(fmt.Sprintf("order-%d", order.Id)),
			Value: gsrBytes,
			Time:  time.Now(),
		}

		if err := writer.WriteMessages(context.Background(), message); err != nil {
			return fmt.Errorf("failed to send order %d to Kafka: %w", i, err)
		}

		log.Printf("Sent order: ID %d for user %d (%.2f total) - %d bytes", order.Id, order.UserId, order.TotalAmount, len(gsrBytes))
	}

	return nil
}

// createTopics creates Kafka topics if they don't exist
func createTopics(cfg *config.Config) error {
	// Create connection to Kafka
	conn, err := kafka.Dial("tcp", cfg.Kafka.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to dial Kafka: %w", err)
	}
	defer conn.Close()

	// Get controller connection
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
	}
	defer controllerConn.Close()

	// Topics to create
	topicsToCreate := []string{cfg.Kafka.UserTopic, cfg.Kafka.OrderTopic}

	// Check which topics already exist
	existingTopics, err := controllerConn.ReadPartitions()
	if err != nil {
		log.Printf("Warning: couldn't read existing partitions (will try to create topics anyway): %v", err)
	}

	// Create a map of existing topics for quick lookup
	existingTopicMap := make(map[string]bool)
	for _, partition := range existingTopics {
		existingTopicMap[partition.Topic] = true
	}

	// Create topics that don't exist
	var topicConfigs []kafka.TopicConfig
	for _, topic := range topicsToCreate {
		if !existingTopicMap[topic] {
			log.Printf("Creating topic: %s", topic)
			topicConfigs = append(topicConfigs, kafka.TopicConfig{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			})
		} else {
			log.Printf("Topic already exists: %s", topic)
		}
	}

	// Create the topics
	if len(topicConfigs) > 0 {
		if err := controllerConn.CreateTopics(topicConfigs...); err != nil {
			return fmt.Errorf("failed to create topics: %w", err)
		}
		log.Printf("Successfully created %d topic(s)", len(topicConfigs))
	} else {
		log.Println("All topics already exist")
	}

	return nil
}

// Helper function to convert []interface{} to []string
func convertStringSlice(input []interface{}) []string {
	result := make([]string, len(input))
	for i, v := range input {
		result[i] = v.(string)
	}
	return result
}
