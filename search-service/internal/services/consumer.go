package services

import (
	"context"
	"log"
	"news-service/internal/kafka"
	"news-service/internal/opensearch"
	"sync"

	"hackernews-services/pkg/models"
)

// KafkaOpenSearchConsumerService handles consuming from Kafka and indexing to OpenSearch
type KafkaOpenSearchConsumerService struct {
	openSearchService *opensearch.OpenSearchService
	running           bool
	stopCh            chan struct{}
	wg                sync.WaitGroup
}

// NewKafkaOpenSearchConsumerService creates a new consumer service
func NewKafkaOpenSearchConsumerService() (*KafkaOpenSearchConsumerService, error) {
	osService, err := opensearch.NewOpenSearchService()
	if err != nil {
		return nil, err
	}

	return &KafkaOpenSearchConsumerService{
		openSearchService: osService,
		stopCh:            make(chan struct{}),
	}, nil
}

// Start begins consuming from all Kafka topics
func (s *KafkaOpenSearchConsumerService) Start(ctx context.Context) error {
	if s.running {
		return nil
	}

	s.running = true
	log.Println("Starting Kafka to OpenSearch consumer service...")

	// Start consumers for each topic
	topics := []struct {
		name    string
		handler func() error
	}{
		{"StoriesTopic", s.consumeStories},
		{"AsksTopic", s.consumeAsks},
		{"CommentsTopic", s.consumeComments},
		{"JobsTopic", s.consumeJobs},
		{"PollsTopic", s.consumePolls},
		{"PollOptionsTopic", s.consumePollOptions},
		{"UsersTopic", s.consumeUsers},
	}

	for _, topic := range topics {
		s.wg.Add(1)
		go func(name string, handler func() error) {
			defer s.wg.Done()
			log.Printf("Starting consumer for topic: %s", name)
			if err := handler(); err != nil {
				log.Printf("Error in consumer for topic %s: %v", name, err)
			}
		}(topic.name, topic.handler)
	}

	log.Println("All Kafka consumers started successfully")
	return nil
}

// Stop gracefully stops all consumers
func (s *KafkaOpenSearchConsumerService) Stop() error {
	if !s.running {
		return nil
	}

	log.Println("Stopping Kafka to OpenSearch consumer service...")
	s.running = false
	close(s.stopCh)
	s.wg.Wait()
	log.Println("All consumers stopped successfully")
	return nil
}

// Topic-specific consumer functions

func (s *KafkaOpenSearchConsumerService) consumeStories() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for stop signal
	go func() {
		<-s.stopCh
		cancel()
	}()

	return kafka.NewObjectConsumerWithContext(ctx, "StoriesTopic", func(story *models.Story) error {
		if err := s.openSearchService.IndexStory(story); err != nil {
			log.Printf("Error indexing story %d: %v", story.ID, err)
			return err
		}
		log.Printf("Successfully indexed story: %d - %s", story.ID, story.Title)
		return nil
	})
}

func (s *KafkaOpenSearchConsumerService) consumeAsks() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for stop signal
	go func() {
		<-s.stopCh
		cancel()
	}()

	return kafka.NewObjectConsumerWithContext(ctx, "AsksTopic", func(ask *models.Ask) error {
		if err := s.openSearchService.IndexAsk(ask); err != nil {
			log.Printf("Error indexing ask %d: %v", ask.ID, err)
			return err
		}
		log.Printf("Successfully indexed ask: %d - %s", ask.ID, ask.Title)
		return nil
	})
}

func (s *KafkaOpenSearchConsumerService) consumeComments() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for stop signal
	go func() {
		<-s.stopCh
		cancel()
	}()

	return kafka.NewObjectConsumerWithContext(ctx, "CommentsTopic", func(comment *models.Comment) error {
		if err := s.openSearchService.IndexComment(comment); err != nil {
			log.Printf("Error indexing comment %d: %v", comment.ID, err)
			return err
		}
		log.Printf("Successfully indexed comment: %d by %s", comment.ID, comment.Author)
		return nil
	})
}

func (s *KafkaOpenSearchConsumerService) consumeJobs() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for stop signal
	go func() {
		<-s.stopCh
		cancel()
	}()

	return kafka.NewObjectConsumerWithContext(ctx, "JobsTopic", func(job *models.Job) error {
		if err := s.openSearchService.IndexJob(job); err != nil {
			log.Printf("Error indexing job %d: %v", job.ID, err)
			return err
		}
		log.Printf("Successfully indexed job: %d - %s", job.ID, job.Title)
		return nil
	})
}

func (s *KafkaOpenSearchConsumerService) consumePolls() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for stop signal
	go func() {
		<-s.stopCh
		cancel()
	}()

	return kafka.NewObjectConsumerWithContext(ctx, "PollsTopic", func(poll *models.Poll) error {
		if err := s.openSearchService.IndexPoll(poll); err != nil {
			log.Printf("Error indexing poll %d: %v", poll.ID, err)
			return err
		}
		log.Printf("Successfully indexed poll: %d - %s", poll.ID, poll.Title)
		return nil
	})
}

func (s *KafkaOpenSearchConsumerService) consumePollOptions() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for stop signal
	go func() {
		<-s.stopCh
		cancel()
	}()

	return kafka.NewObjectConsumerWithContext(ctx, "PollOptionsTopic", func(pollOption *models.PollOption) error {
		if err := s.openSearchService.IndexPollOption(pollOption); err != nil {
			log.Printf("Error indexing poll option %d: %v", pollOption.ID, err)
			return err
		}
		log.Printf("Successfully indexed poll option: %d - %s", pollOption.ID, pollOption.OptionText)
		return nil
	})
}

func (s *KafkaOpenSearchConsumerService) consumeUsers() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for stop signal
	go func() {
		<-s.stopCh
		cancel()
	}()

	return kafka.NewObjectConsumerWithContext(ctx, "UsersTopic", func(user *models.User) error {
		if err := s.openSearchService.IndexUser(user); err != nil {
			log.Printf("Error indexing user %s: %v", user.Username, err)
			return err
		}
		log.Printf("Successfully indexed user: %s (karma: %d)", user.Username, user.Karma)
		return nil
	})
}
