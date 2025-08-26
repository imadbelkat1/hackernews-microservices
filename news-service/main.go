// news-service/main.go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"news-service/internal/api"
	"news-service/internal/cron"
	"news-service/internal/services"
)

func main() {
	log.Println("🚀 Starting News Service on port 8081...")
	log.Println("📡 Fetching data from HackerNews API and sending to Kafka...")

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create HTTP client for HackerNews API
	client := api.NewHackerNewsApiClient()

	// Create all API services
	userService := services.NewUserApiService(client)
	storyService := services.NewStoryApiService(client)
	commentService := services.NewCommentApiService(client)
	jobService := services.NewJobApiService(client)
	askService := services.NewAskApiService(client)
	pollService := services.NewPollApiService(client)
	pollOptionService := services.NewPollOptionApiService(client)
	updateService := services.NewUpdateApiService(client)

	// Create and start data sync service (cron jobs)
	dataSyncService, err := cron.NewDataSyncService(
		client,
		userService,
		storyService,
		commentService,
		jobService,
		askService,
		pollService,
		pollOptionService,
		updateService,
	)
	if err != nil {
		log.Fatal("❌ Failed to create data sync service:", err)
	}

	// Start data sync service in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("⏰ Starting data sync service (HackerNews API → Redis Cache → Kafka Topics)...")
		if err := dataSyncService.Start(); err != nil {
			log.Printf("❌ Data sync service error: %v", err)
		}
	}()

	log.Println("✅ News Service started successfully!")
	log.Println("🔄 Cron jobs are running:")
	log.Println("   📰 Top Stories sync: every 1 minute")
	log.Println("   ❓ Ask Stories sync: every 1 minute")
	log.Println("   💼 Job Stories sync: every 1 minute")
	log.Println("   🔄 Updates sync: every 10 seconds")
	log.Println("")
	log.Println("📤 Sending data to Kafka topics:")
	log.Println("   - StoriesTopic")
	log.Println("   - UsersTopic")
	log.Println("   - CommentsTopic")
	log.Println("   - JobsTopic")
	log.Println("   - AsksTopic")
	log.Println("   - PollsTopic")
	log.Println("   - PollOptionsTopic")

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Graceful shutdown
	log.Println("🛑 Shutting down News Service...")
	cancel() // Cancel context

	// Stop data sync service
	if err := dataSyncService.Stop(); err != nil {
		log.Printf("❌ Error stopping data sync service: %v", err)
	} else {
		log.Println("✅ Data sync service stopped")
	}

	// Wait for all goroutines to finish
	wg.Wait()
	log.Println("👋 News Service stopped successfully")
}
