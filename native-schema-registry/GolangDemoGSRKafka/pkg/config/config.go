package config

import (
	"strings"
)

type Config struct {
	Kafka KafkaConfig
	AWS   AWSConfig
}

type KafkaConfig struct {
	Brokers    []string
	UserTopic  string
	OrderTopic string
}

type AWSConfig struct {
	Region string
}

func NewConfig(brokers, userTopic, orderTopic, awsRegion string) *Config {
	return &Config{
		Kafka: KafkaConfig{
			Brokers:    parseBrokers(brokers),
			UserTopic:  userTopic,
			OrderTopic: orderTopic,
		},
		AWS: AWSConfig{
			Region: awsRegion,
		},
	}
}

// parseBrokers splits comma-separated broker string and trims whitespace
func parseBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
