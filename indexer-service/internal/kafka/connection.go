package kafka

import (
	"hackernews-services/config"
)

// KafkaConfig holds the configuration for Kafka
type KafkaConfig struct {
	BootstrapServers string `yaml:"bootstrap_servers"`
	ClientID         string `yaml:"client_id"`
	Acks             string `yaml:"acks"`
	Topic            string `yaml:"topics"`
}

// GetKafkaConfig returns the Kafka configuration from environment variables
func GetKafkaConfig() KafkaConfig {
	return KafkaConfig{
		BootstrapServers: config.GetEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		ClientID:         config.GetEnv("KAFKA_CLIENT_ID", "my-client"),
		Acks:             config.GetEnv("KAFKA_ACKS", "all"),
		Topic:            config.GetEnv("KAFKA_TOPICS", "StoriesTopic,CommentsTopic,AsksTopic,JobsTopic,PollsTopic,PollOptionsTopic,UsersTopic"),
	}
}
