// indexer-service/main.go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"indexer-service/internal/kafka"
	"indexer-service/internal/models"
	repository "indexer-service/internal/repository"
	"indexer-service/pkg/database"
)

func main() {
	log.Println("----------Starting Indexer Service on port 8082...")
	log.Println("----------Processing data from Kafka → PostgreSQL...")

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to the database
	log.Println("Connecting to PostgreSQL database...")
	config := database.GetDefaultConfig()
	if err := database.Connect(config); err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			log.Printf("Failed to close database connection: %v", err)
		}
	}()

	log.Println("Database connection established")

	// Initialize repositories
	storyRepo := repository.NewStoryRepository()
	userRepo := repository.NewUserRepository()
	commentRepo := repository.NewCommentRepository()
	jobRepo := repository.NewJobRepository()
	askRepo := repository.NewAskRepository()
	pollRepo := repository.NewPollRepository()
	pollOptionRepo := repository.NewPollOptionRepository()

	// Start Kafka consumers
	consumers := []struct {
		name    string
		topic   string
		handler func() error
	}{
		{
			name:  "Stories Consumer",
			topic: "StoriesTopic",
			handler: func() error {
				return kafka.NewStoryConsumer("StoriesTopic", func(story *models.Story) error {
					log.Printf("📰 Processing story: ID=%d, Title=%s", story.ID, story.Title)

					exists, err := storyRepo.Exists(ctx, story.ID)
					if err != nil {
						return err
					}

					if exists {
						return storyRepo.Update(ctx, story)
					}
					return storyRepo.Create(ctx, story)
				})
			},
		},
		{
			name:  "Users Consumer",
			topic: "UsersTopic",
			handler: func() error {
				return kafka.NewUserConsumer("UsersTopic", func(user *models.User) error {
					log.Printf("👤 Processing user: Username=%s, Karma=%d", user.Username, user.Karma)

					exists, err := userRepo.UserExists(ctx, user.Username)
					if err != nil {
						return err
					}

					if exists {
						return userRepo.Update(ctx, user)
					}
					return userRepo.Create(ctx, user)
				})
			},
		},
		{
			name:  "Comments Consumer",
			topic: "CommentsTopic",
			handler: func() error {
				return kafka.NewCommentConsumer("CommentsTopic", func(comment *models.Comment) error {
					log.Printf("💬 Processing comment: ID=%d, Author=%s", comment.ID, comment.Author)

					exists, err := commentRepo.Exists(ctx, comment.ID)
					if err != nil {
						return err
					}

					if exists {
						return commentRepo.Update(ctx, comment)
					}
					return commentRepo.Create(ctx, comment)
				})
			},
		},
		{
			name:  "Jobs Consumer",
			topic: "JobsTopic",
			handler: func() error {
				return kafka.NewJobConsumer("JobsTopic", func(job *models.Job) error {
					log.Printf("💼 Processing job: ID=%d, Title=%s", job.ID, job.Title)

					exists, err := jobRepo.Exists(ctx, job.ID)
					if err != nil {
						return err
					}

					if exists {
						return jobRepo.Update(ctx, job)
					}
					return jobRepo.Create(ctx, job)
				})
			},
		},
		{
			name:  "Asks Consumer",
			topic: "AsksTopic",
			handler: func() error {
				return kafka.NewAskConsumer("AsksTopic", func(ask *models.Ask) error {
					log.Printf("❓ Processing ask: ID=%d, Title=%s", ask.ID, ask.Title)

					exists, err := askRepo.Exists(ctx, ask.ID)
					if err != nil {
						return err
					}

					if exists {
						return askRepo.Update(ctx, ask)
					}
					return askRepo.Create(ctx, ask)
				})
			},
		},
		{
			name:  "Polls Consumer",
			topic: "PollsTopic",
			handler: func() error {
				return kafka.NewPollConsumer("PollsTopic", func(poll *models.Poll) error {
					log.Printf("📊 Processing poll: ID=%d, Title=%s", poll.ID, poll.Title)

					exists, err := pollRepo.Exists(ctx, poll.ID)
					if err != nil {
						return err
					}

					if exists {
						return pollRepo.Update(ctx, poll)
					}
					return pollRepo.Create(ctx, poll)
				})
			},
		},
		{
			name:  "Poll Options Consumer",
			topic: "PollOptionsTopic",
			handler: func() error {
				return kafka.NewPollOptionConsumer("PollOptionsTopic", func(pollOption *models.PollOption) error {
					log.Printf(" Processing poll option: ID=%d, Text=%s", pollOption.ID, pollOption.OptionText)

					exists, err := pollOptionRepo.Exists(ctx, pollOption.ID)
					if err != nil {
						return err
					}

					if exists {
						return pollOptionRepo.Update(ctx, pollOption)
					}
					return pollOptionRepo.Create(ctx, pollOption)
				})
			},
		},
	}

	// Start all consumers
	for _, consumer := range consumers {
		wg.Add(1)
		go func(name, topic string, handler func() error) {
			defer wg.Done()
			log.Printf("Starting %s (Topic: %s)...", name, topic)
			if err := handler(); err != nil {
				log.Printf("Error in %s: %v", name, err)
			}
		}(consumer.name, consumer.topic, consumer.handler)
	}

	log.Println("Indexer Service started successfully!")
	log.Println("Listening to Kafka topics:")
	for _, consumer := range consumers {
		log.Printf("   - %s: %s", consumer.topic, consumer.name)
	}

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Graceful shutdown
	log.Println("Shutting down Indexer Service...")
	cancel() // Cancel context for consumers

	// Wait for all goroutines to finish
	wg.Wait()
	log.Println("Indexer Service stopped successfully")
}
