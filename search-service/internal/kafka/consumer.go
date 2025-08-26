package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"search-service/internal/models"

	"github.com/segmentio/kafka-go"
)

func NewObjectConsumer[T any](topic string, handler func(T) error) error {
	config := GetKafkaConfig()

	// Use proper Reader instead of low-level DialLeader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{config.BootstrapServers},
		Topic:       topic,
		StartOffset: kafka.LastOffset,
		MinBytes:    1, // Read immediately when data available
		MaxBytes:    10e6,
		MaxWait:     30 * time.Second,
	})
	defer reader.Close()

	log.Printf("Starting REAL-TIME consumer for topic: %s", topic)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		message, err := reader.ReadMessage(ctx)
		cancel()

		if err != nil {
			if err != context.DeadlineExceeded {
				log.Printf(" Error reading message from %s: %v", topic, err)
			}

			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("Received message from %s (offset: %d, size: %d bytes)",
			topic, message.Offset, len(message.Value))

		var obj T
		if err := json.Unmarshal(message.Value, &obj); err != nil {
			log.Printf(" Error unmarshaling message from %s: %v", topic, err)
			continue
		}

		if err := handler(obj); err != nil {
			log.Printf(" Error handling message from %s: %v", topic, err)
			continue
		}

		log.Printf("Successfully processed message from %s (offset: %d)", topic, message.Offset)
	}
}

func NewStoryConsumer(topic string, handler func(*models.Story) error) error {
	return NewObjectConsumer(topic, handler)
}

func NewAskConsumer(topic string, handler func(*models.Ask) error) error {
	return NewObjectConsumer(topic, handler)
}

func NewCommentConsumer(topic string, handler func(*models.Comment) error) error {
	return NewObjectConsumer(topic, handler)
}

func NewJobConsumer(topic string, handler func(*models.Job) error) error {
	return NewObjectConsumer(topic, handler)
}

func NewPollConsumer(topic string, handler func(*models.Poll) error) error {
	return NewObjectConsumer(topic, handler)
}

func NewPollOptionConsumer(topic string, handler func(*models.PollOption) error) error {
	return NewObjectConsumer(topic, handler)
}

func NewUserConsumer(topic string, handler func(*models.User) error) error {
	return NewObjectConsumer(topic, handler)
}

func NewObjectConsumerWithContext[T any](ctx context.Context, topic string, handler func(T) error) error {
	config := GetKafkaConfig()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{config.BootstrapServers},
		Topic:       topic,
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     30 * time.Second,
	})
	defer reader.Close()

	log.Printf("Starting REAL-TIME consumer for topic: %s", topic)

	for {
		select {
		case <-ctx.Done():
			log.Println("Context canceled, stopping consumer...")
			return ctx.Err()
		default:
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				if err != context.DeadlineExceeded {
					log.Printf(" Error reading message from %s: %v", topic, err)
				}
				time.Sleep(2 * time.Second)
				continue
			}

			log.Printf("Received message from %s (offset: %d, size: %d bytes)",
				topic, message.Offset, len(message.Value))

			var obj T
			if err := json.Unmarshal(message.Value, &obj); err != nil {
				log.Printf("Error unmarshaling message from %s: %v", topic, err)
				continue
			}

			if err := handler(obj); err != nil {
				log.Printf("Error handling message from %s: %v", topic, err)
				continue
			}

			log.Printf("Successfully processed message from %s (offset: %d)", topic, message.Offset)
		}
	}
}
