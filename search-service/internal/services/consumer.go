package services

import (
	"context"
	"log"
	"sync"
	"time"

	"search-service/internal/kafka"
	"search-service/internal/models"
	"search-service/internal/opensearch"
)

type KafkaOpenSearchConsumerService struct {
	openSearchService *opensearch.OpenSearchService
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
}

func NewKafkaOpenSearchConsumerService() (*KafkaOpenSearchConsumerService, error) {
	osService, err := opensearch.NewOpenSearchService()
	if err != nil {
		return nil, err
	}

	return &KafkaOpenSearchConsumerService{
		openSearchService: osService,
	}, nil
}

func (s *KafkaOpenSearchConsumerService) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	log.Println("Starting simple batch consumers...")

	topics := []string{
		"StoriesTopic",
		"AsksTopic",
		"CommentsTopic",
		"JobsTopic",
		"PollsTopic",
		"PollOptionsTopic",
		"UsersTopic",
	}

	for _, topic := range topics {
		s.wg.Add(1)
		go s.consumeTopic(topic)
	}

	return nil
}

func (s *KafkaOpenSearchConsumerService) Stop() error {
	log.Println("Stopping consumers...")
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	log.Println("All consumers stopped")
	return nil
}

func (s *KafkaOpenSearchConsumerService) consumeTopic(topic string) {
	defer s.wg.Done()

	batch := make([]interface{}, 0, 50)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	messageCh := make(chan interface{}, 100)

	// Start Kafka consumer
	go func() {
		kafka.NewObjectConsumerWithContext(s.ctx, topic, func(msg interface{}) error {
			select {
			case messageCh <- msg:
				return nil
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
		})
	}()

	// Simple batch loop
	for {
		select {
		case msg := <-messageCh:
			batch = append(batch, msg)
			if len(batch) >= 50 {
				s.indexBatch(topic, batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				s.indexBatch(topic, batch)
				batch = batch[:0]
			}

		case <-s.ctx.Done():
			if len(batch) > 0 {
				s.indexBatch(topic, batch)
			}
			return
		}
	}
}

func (s *KafkaOpenSearchConsumerService) indexBatch(topic string, batch []interface{}) {
	if len(batch) == 0 {
		return
	}

	var err error
	switch topic {
	case "StoriesTopic":
		stories, convertErr := s.convertToStories(batch)
		if convertErr != nil {
			log.Printf("Failed to convert stories batch: %v", convertErr)
			return
		}
		err = s.openSearchService.BulkIndexStories(stories)

	case "AsksTopic":
		asks, convertErr := s.convertToAsks(batch)
		if convertErr != nil {
			log.Printf("Failed to convert asks batch: %v", convertErr)
			return
		}
		err = s.openSearchService.BulkIndexAsks(asks)

	case "CommentsTopic":
		comments, convertErr := s.convertToComments(batch)
		if convertErr != nil {
			log.Printf("Failed to convert comments batch: %v", convertErr)
			return
		}
		err = s.openSearchService.BulkIndexComments(comments)

	case "JobsTopic":
		jobs, convertErr := s.convertToJobs(batch)
		if convertErr != nil {
			log.Printf("Failed to convert jobs batch: %v", convertErr)
			return
		}
		err = s.openSearchService.BulkIndexJobs(jobs)

	case "PollsTopic":
		polls, convertErr := s.convertToPolls(batch)
		if convertErr != nil {
			log.Printf("Failed to convert polls batch: %v", convertErr)
			return
		}
		err = s.openSearchService.BulkIndexPolls(polls)

	case "PollOptionsTopic":
		pollOptions, convertErr := s.convertToPollOptions(batch)
		if convertErr != nil {
			log.Printf("Failed to convert poll options batch: %v", convertErr)
			return
		}
		err = s.openSearchService.BulkIndexPollOptions(pollOptions)

	case "UsersTopic":
		users, convertErr := s.convertToUsers(batch)
		if convertErr != nil {
			log.Printf("Failed to convert users batch: %v", convertErr)
			return
		}
		err = s.openSearchService.BulkIndexUsers(users)
	}

	if err != nil {
		log.Printf("Batch index failed for %s: %v", topic, err)
	} else {
		log.Printf("Indexed %d items from %s", len(batch), topic)
	}
}

// Helper functions to convert map[string]interface{} to typed structs
func (s *KafkaOpenSearchConsumerService) convertToStories(batch []interface{}) ([]*models.Story, error) {
	stories := make([]*models.Story, 0, len(batch))
	for _, item := range batch {
		data, ok := item.(map[string]interface{})
		if !ok {
			continue // Skip invalid items
		}

		story := &models.Story{}
		if id, ok := data["id"].(float64); ok {
			story.ID = int(id)
		}
		if title, ok := data["title"].(string); ok {
			story.Title = title
		}
		if url, ok := data["url"].(string); ok {
			story.URL = url
		}
		if score, ok := data["score"].(float64); ok {
			story.Score = int(score)
		}
		if author, ok := data["by"].(string); ok {
			story.Author = author
		}
		if time, ok := data["time"].(float64); ok {
			story.Created_At = int64(time)
		}
		if descendants, ok := data["descendants"].(float64); ok {
			story.Comments_count = int(descendants)
		}
		story.Type = "story"

		if story.IsValid() {
			stories = append(stories, story)
		}
	}
	return stories, nil
}

func (s *KafkaOpenSearchConsumerService) convertToUsers(batch []interface{}) ([]*models.User, error) {
	users := make([]*models.User, 0, len(batch))
	for _, item := range batch {
		data, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		user := &models.User{}
		if username, ok := data["id"].(string); ok {
			user.Username = username
		}
		if karma, ok := data["karma"].(float64); ok {
			user.Karma = int(karma)
		}
		if about, ok := data["about"].(string); ok {
			user.About = about
		}
		if created, ok := data["created"].(float64); ok {
			user.Created_At = int64(created)
		}

		if user.Username != "" {
			users = append(users, user)
		}
	}
	return users, nil
}

func (s *KafkaOpenSearchConsumerService) convertToAsks(batch []interface{}) ([]*models.Ask, error) {
	asks := make([]*models.Ask, 0, len(batch))
	for _, item := range batch {
		data, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		ask := &models.Ask{}
		if id, ok := data["id"].(float64); ok {
			ask.ID = int(id)
		}
		if title, ok := data["title"].(string); ok {
			ask.Title = title
		}
		if text, ok := data["text"].(string); ok {
			ask.Text = text
		}
		if score, ok := data["score"].(float64); ok {
			ask.Score = int(score)
		}
		if author, ok := data["by"].(string); ok {
			ask.Author = author
		}
		if time, ok := data["time"].(float64); ok {
			ask.Created_At = int64(time)
		}
		ask.Type = "ask"

		if ask.IsValid() {
			asks = append(asks, ask)
		}
	}
	return asks, nil
}

func (s *KafkaOpenSearchConsumerService) convertToComments(batch []interface{}) ([]*models.Comment, error) {
	comments := make([]*models.Comment, 0, len(batch))
	for _, item := range batch {
		data, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		comment := &models.Comment{}
		if id, ok := data["id"].(float64); ok {
			comment.ID = int(id)
		}
		if text, ok := data["text"].(string); ok {
			comment.Text = text
		}
		if author, ok := data["by"].(string); ok {
			comment.Author = author
		}
		if parent, ok := data["parent"].(float64); ok {
			comment.Parent = int(parent)
		}
		if time, ok := data["time"].(float64); ok {
			comment.Created_At = int64(time)
		}
		comment.Type = "comment"

		if comment.IsValid() {
			comments = append(comments, comment)
		}
	}
	return comments, nil
}

func (s *KafkaOpenSearchConsumerService) convertToJobs(batch []interface{}) ([]*models.Job, error) {
	jobs := make([]*models.Job, 0, len(batch))
	for _, item := range batch {
		data, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		job := &models.Job{}
		if id, ok := data["id"].(float64); ok {
			job.ID = int(id)
		}
		if title, ok := data["title"].(string); ok {
			job.Title = title
		}
		if text, ok := data["text"].(string); ok {
			job.Text = text
		}
		if url, ok := data["url"].(string); ok {
			job.URL = url
		}
		if author, ok := data["by"].(string); ok {
			job.Author = author
		}
		if time, ok := data["time"].(float64); ok {
			job.Created_At = int64(time)
		}
		job.Type = "job"

		if job.IsValid() {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func (s *KafkaOpenSearchConsumerService) convertToPolls(batch []interface{}) ([]*models.Poll, error) {
	polls := make([]*models.Poll, 0, len(batch))
	for _, item := range batch {
		data, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		poll := &models.Poll{}
		if id, ok := data["id"].(float64); ok {
			poll.ID = int(id)
		}
		if title, ok := data["title"].(string); ok {
			poll.Title = title
		}
		if score, ok := data["score"].(float64); ok {
			poll.Score = int(score)
		}
		if author, ok := data["by"].(string); ok {
			poll.Author = author
		}
		if time, ok := data["time"].(float64); ok {
			poll.Created_At = int64(time)
		}
		poll.Type = "poll"

		if poll.IsValid() {
			polls = append(polls, poll)
		}
	}
	return polls, nil
}

func (s *KafkaOpenSearchConsumerService) convertToPollOptions(batch []interface{}) ([]*models.PollOption, error) {
	pollOptions := make([]*models.PollOption, 0, len(batch))
	for _, item := range batch {
		data, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		pollOption := &models.PollOption{}
		if id, ok := data["id"].(float64); ok {
			pollOption.ID = int(id)
		}
		if text, ok := data["text"].(string); ok {
			pollOption.OptionText = text
		}
		if poll, ok := data["poll"].(float64); ok {
			pollOption.PollID = int(poll)
		}
		if author, ok := data["by"].(string); ok {
			pollOption.Author = author
		}
		if time, ok := data["time"].(float64); ok {
			pollOption.CreatedAt = int64(time)
		}
		if score, ok := data["score"].(float64); ok {
			pollOption.Votes = int(score)
		}
		pollOption.Type = "pollOption"

		if pollOption.IsValid() {
			pollOptions = append(pollOptions, pollOption)
		}
	}
	return pollOptions, nil
}
