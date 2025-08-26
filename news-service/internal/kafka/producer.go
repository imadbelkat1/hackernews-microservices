package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"news-service/internal/models"

	"github.com/segmentio/kafka-go"
)

func NewObjectProducer[T any](topic string, objects []T) error {
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", GetKafkaConfig().BootstrapServers, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	messages := make([]kafka.Message, len(objects))
	for i, obj := range objects {
		// Convert object to JSON
		jsonData, err := json.Marshal(obj)
		if err != nil {
			log.Printf("Error marshaling object %d: %v", i, err)
			continue
		}
		messages[i] = kafka.Message{Value: jsonData}
	}

	_, err = conn.WriteMessages(messages...)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	return nil
}

func NewStoryProducer(topic string, stories []*models.Story) error {
	return NewObjectProducer(topic, stories)
}

func NewAskProducer(topic string, asks []*models.Ask) error {
	return NewObjectProducer(topic, asks)
}

func NewCommentProducer(topic string, comments []*models.Comment) error {
	return NewObjectProducer(topic, comments)
}

func NewJobProducer(topic string, jobs []*models.Job) error {
	return NewObjectProducer(topic, jobs)
}

func NewPollProducer(topic string, polls []*models.Poll) error {
	return NewObjectProducer(topic, polls)
}

func NewPollOptionProducer(topic string, pollOptions []*models.PollOption) error {
	return NewObjectProducer(topic, pollOptions)
}

func NewUserProducer(topic string, users []*models.User) error {
	return NewObjectProducer(topic, users)
}

