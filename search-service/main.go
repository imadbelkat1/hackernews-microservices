// search-service/main.go
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"search-service/internal/handlers"
	"search-service/internal/opensearch"
	"search-service/internal/services"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

func main() {
	log.Println("Starting Search Service on port 8083...")
	log.Println("Indexing data from Kafka → OpenSearch and providing search API...")

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize OpenSearch service
	openSearchService, err := opensearch.NewOpenSearchService()
	if err != nil {
		log.Printf("Failed to create OpenSearch service: %v", err)
		log.Println("Continuing without OpenSearch indexing...")
		openSearchService = nil
	} else {
		log.Println("OpenSearch service initialized")
	}

	// Create and start Kafka to OpenSearch consumer service (if OpenSearch is available)
	if openSearchService != nil {
		consumerService, err := services.NewKafkaOpenSearchConsumerService()
		if err != nil {
			log.Printf("Failed to create OpenSearch consumer service: %v", err)
			log.Println("Continuing without OpenSearch indexing...")
		} else {
			wg.Add(1)
			go func() {
				defer wg.Done()
				log.Println("Starting Kafka to OpenSearch consumer service...")
				if err := consumerService.Start(ctx); err != nil {
					log.Printf("Consumer service error: %v", err)
				}
			}()

			// Defer stopping the consumer service
			defer func() {
				if err := consumerService.Stop(); err != nil {
					log.Printf("Error stopping consumer service: %v", err)
				} else {
					log.Println("Consumer service stopped")
				}
			}()
		}
	}

	// Initialize HTTP handlers
	searchHandler := handlers.NewSearchHandler(openSearchService)

	// Setup HTTP routes
	router := mux.NewRouter()

	// API v1 routes
	api := router.PathPrefix("/api/v1").Subrouter()

	// Search endpoints
	api.HandleFunc("/search/stories", searchHandler.SearchStories).Methods("GET")
	api.HandleFunc("/search/users", searchHandler.SearchUsers).Methods("GET")
	api.HandleFunc("/search/comments", searchHandler.SearchComments).Methods("GET")
	api.HandleFunc("/search/jobs", searchHandler.SearchJobs).Methods("GET")
	api.HandleFunc("/search/asks", searchHandler.SearchAsks).Methods("GET")
	api.HandleFunc("/search/polls", searchHandler.SearchPolls).Methods("GET")
	api.HandleFunc("/search/all", searchHandler.SearchAll).Methods("GET")

	// Health check endpoint
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "healthy", "service": "search-service", "port": 8083}`))
	}).Methods("GET")

	// Setup CORS
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: true,
	})

	// Create HTTP server
	srv := &http.Server{
		Addr:         ":8083",
		Handler:      c.Handler(router),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	// Start HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Search Service HTTP server listening on :8083")
		log.Println("Available endpoints:")
		log.Println("   - GET /health")
		log.Println("   - GET /api/v1/search/stories?q=query")
		log.Println("   - GET /api/v1/search/users?q=query")
		log.Println("   - GET /api/v1/search/comments?q=query")
		log.Println("   - GET /api/v1/search/jobs?q=query")
		log.Println("   - GET /api/v1/search/asks?q=query")
		log.Println("   - GET /api/v1/search/polls?q=query")
		log.Println("   - GET /api/v1/search/all?q=query")

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Println("Search Service started successfully!")
	if openSearchService != nil {
		log.Println("Indexing data from Kafka topics:")
		log.Println("   - StoriesTopic → OpenSearch")
		log.Println("   - UsersTopic → OpenSearch")
		log.Println("   - CommentsTopic → OpenSearch")
		log.Println("   - JobsTopic → OpenSearch")
		log.Println("   - AsksTopic → OpenSearch")
		log.Println("   - PollsTopic → OpenSearch")
		log.Println("   - PollOptionsTopic → OpenSearch")
	}

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Graceful shutdown
	log.Println("Shutting down Search Service...")
	cancel() // Cancel context for consumers

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
	} else {
		log.Println("HTTP server stopped gracefully")
	}

	// Wait for all goroutines to finish
	wg.Wait()
	log.Println("Search Service stopped successfully")
}
