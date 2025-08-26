package cronjob

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	api "news-service/internal/api"
	"news-service/internal/kafka"
	"news-service/internal/redis"
	"news-service/internal/services"

	"github.com/go-co-op/gocron/v2"
)

type DataSyncService struct {
	scheduler         gocron.Scheduler
	apiClient         *api.HackerNewsApiClient
	userService       *services.UserApiService
	storyService      *services.StoryApiService
	commentService    *services.CommentApiService
	jobService        *services.JobApiService
	askService        *services.AskApiService
	pollService       *services.PollApiService
	pollOptionService *services.PollOptionApiService
	updateService     *services.UpdateApiService
}

// NewDataSyncService creates a new data sync service
func NewDataSyncService(
	apiClient *api.HackerNewsApiClient,
	userService *services.UserApiService,
	storyService *services.StoryApiService,
	commentService *services.CommentApiService,
	jobService *services.JobApiService,
	askService *services.AskApiService,
	pollService *services.PollApiService,
	pollOptionService *services.PollOptionApiService,
	updateService *services.UpdateApiService,
) (*DataSyncService, error) {
	// Create a single scheduler for all jobs
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("failed to create scheduler: %w", err)
	}

	return &DataSyncService{
		scheduler:         scheduler,
		apiClient:         apiClient,
		userService:       userService,
		storyService:      storyService,
		askService:        askService,
		jobService:        jobService,
		commentService:    commentService,
		pollService:       pollService,
		pollOptionService: pollOptionService,
		updateService:     updateService,
	}, nil
}

const (
	topStoriesCacheKey  = "topStoriesIDs"
	updatesCacheKey     = "updatesIDs"
	updatesUserCacheKey = "updatesUserIDs"
	storiesCacheKey     = "storiesIDs"
	commentsCacheKey    = "commentsIDs"
	jobsCacheKey        = "jobsIDs"
	asksCacheKey        = "pollOptionsIDs"
	pollsCacheKey       = "pollsIDs"
	pollOptionsCacheKey = "pollOptionsIDs"
	userCacheKey        = "userIDs"
)

// Start begins all scheduled jobs
func (d *DataSyncService) Start() error {
	// Register all jobs
	if err := d.registerJobs(); err != nil {
		return fmt.Errorf("failed to register jobs: %w", err)
	}

	// Start the scheduler
	d.scheduler.Start()
	log.Println("DataSyncService started with all cron jobs and database connection established!")
	return nil
}

// Stop gracefully stops all jobs
func (d *DataSyncService) Stop() error {
	if err := d.scheduler.Shutdown(); err != nil {
		return fmt.Errorf("failed to shutdown scheduler: %w", err)
	}
	log.Println("DataSyncService stopped")

	return nil
}

// registerJobs sets up all the cron jobs
func (d *DataSyncService) registerJobs() error {
	jobs := []struct {
		name      string
		interval  time.Duration
		task      func()
		immediate bool
	}{
		{
			name:     "sync-stories",
			interval: 1 * time.Minute,
			task:     func() { d.syncTopStories() },
		},
		{
			name:     "sync-asks",
			interval: 1 * time.Minute,
			task:     d.syncAsks,
		},
		{
			name:     "sync-jobs",
			interval: 1 * time.Minute,
			task:     d.syncJobs,
		},
		{
			name:      "sync-updates",
			interval:  10 * time.Second,
			task:      func() { d.syncUpdates() },
			immediate: true, // Run immediately on startup
		},
	}

	for _, job := range jobs {
		// Run immediately
		if job.immediate {
			log.Printf("Running job %s immediately...", job.name)
			go job.task()
		}
		_, err := d.scheduler.NewJob(
			gocron.DurationJob(job.interval),
			gocron.NewTask(job.task),
			gocron.WithName(job.name),
		)
		if err != nil {
			return fmt.Errorf("failed to create job %s: %w", job.name, err)
		}
		log.Printf("Registered job: %s (every %v)", job.name, job.interval)
	}

	return nil
}

// Job implementations

// syncTopStories fetches, check cache and saves top stories (may also contain Jobs)
func (d *DataSyncService) syncTopStories() {
	ctx := context.Background()

	var mu sync.Mutex

	var uncachedIDs []int

	log.Println("Starting story sync...")

	// Fetch top stories from the API
	ids, err := d.storyService.FetchTopStories(ctx)
	if err != nil {
		log.Printf("Error fetching top stories: %v", err)
		return
	}

	// Check if items are in cache so we don't fetch them again
	uncachedIDs, err = redis.GetUncachedItems(ctx, topStoriesCacheKey, ids)
	if err != nil {
		log.Printf("Error checking uncached items: %v", err)
		return
	} else if len(uncachedIDs) > 0 {
		log.Printf("Found %d uncached TOPSTORIES, proceeding to fetch and save them", len(uncachedIDs))
	} else {
		log.Println("All top stories are already cached, skipping fetch")
		return
	}

	storiesIDs := make([]int, 0, len(uncachedIDs))
	jobsIDs := make([]int, 0, len(uncachedIDs))

	const maxWorkers = 50
	semaphore := make(chan struct{}, maxWorkers)

	// Fetch stories concurrently
	var wg sync.WaitGroup
	for _, id := range uncachedIDs {
		wg.Add(1)
		go func(itemID int) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			var rawItem map[string]interface{}
			err := d.apiClient.GetItem(ctx, id, &rawItem)
			if err != nil {
				log.Printf("Error fetching item %d: %v", id, err)
				return
			}

			itemType, ok := rawItem["type"].(string)
			if !ok {
				log.Printf("Item %d has no valid type", id)
				return
			}

			mu.Lock()
			switch itemType {
			case "story":
				storiesIDs = append(storiesIDs, itemID)
			case "job":
				jobsIDs = append(jobsIDs, itemID)
			default:
				log.Printf("Unknown item type '%s' for item %d", itemType, itemID)
			}
			mu.Unlock()
		}(id)
	}

	wg.Wait()

	var processingWg sync.WaitGroup

	if len(storiesIDs) > 0 {
		processingWg.Add(1)
		go func() {
			defer processingWg.Done()
			stories, err := d.storyService.FetchMultiple(ctx, storiesIDs)
			if err != nil {
				log.Printf("Error fetching stories: %v", err)
				return
			}

			log.Printf("Successfully fetched %d stories", len(stories))
		}()
	}

	if len(jobsIDs) > 0 {
		processingWg.Add(1)
		go func() {
			defer processingWg.Done()

			jobs, err := d.jobService.FetchMultiple(ctx, jobsIDs)
			if err != nil {
				log.Printf("Error fetching jobs: %v", err)
				return
			}

			log.Printf("Successfully fetched %d jobs", len(jobs))
		}()
	}
	processingWg.Wait()

	allProcessedIDs := append(storiesIDs, jobsIDs...)

	if len(allProcessedIDs) > 0 {
		if err := redis.CacheIDs(ctx, topStoriesCacheKey, allProcessedIDs, 1*time.Hour); err != nil {
			log.Printf("Error caching processed IDs: %v", err)
		} else {
			log.Printf("Cached %d processed IDs to Redis", len(allProcessedIDs))
		}
	}

	log.Printf("Successfully synced %d top stories and %d top jobs", len(storiesIDs), len(jobsIDs))
}

func (d *DataSyncService) syncAsks() {
	log.Println("Starting ask sync...")

	ctx := context.Background()
	ids, err := d.askService.FetchAskStories(ctx)
	if err != nil {
		log.Printf("Error fetching ask stories: %v", err)
		return
	}

	uncachedAsks, err := redis.GetUncachedItems(ctx, asksCacheKey, ids)
	if err != nil {
		log.Printf("Error checking uncached asks: %v", err)
		return
	}

	asks, err := d.askService.FetchMultiple(ctx, uncachedAsks)
	if err != nil {
		log.Printf("Error fetching ask details: %v", err)
		return
	}

	log.Printf("Successfully synced %d asks", len(uncachedAsks))

	// Send full ask objects to Kafka instead of just IDs
	if err := kafka.NewAskProducer("AsksTopic", asks); err != nil {
		log.Printf("Error sending asks to Kafka: %v", err)
	} else {
		log.Printf("Sent %d ask objects to Kafka", len(asks))
		if err := redis.CacheIDs(ctx, asksCacheKey, uncachedAsks, 1*time.Hour); err != nil {
			log.Printf("Error caching asks to Redis: %v", err)
		} else {
			log.Printf("Cached %d asks to Redis", len(uncachedAsks))
		}
	}

	log.Println("Ask sync completed")
	log.Printf("Total asks synced: %d", len(asks))
}

func (d *DataSyncService) syncJobs() {
	log.Println("Starting job sync...")

	ctx := context.Background()
	ids, err := d.jobService.FetchJobStories(ctx)
	if err != nil {
		log.Printf("Error fetching job stories: %v", err)
		return
	}

	uncachedJobs, err := redis.GetUncachedItems(ctx, jobsCacheKey, ids)
	if err != nil {
		log.Printf("Error checking uncached jobs: %v", err)
		return
	}

	jobs, err := d.jobService.FetchMultiple(ctx, uncachedJobs)
	if err != nil {
		log.Printf("Error fetching job details: %v", err)
		return
	}

	log.Printf("Successfully synced %d jobs", len(jobs))

	// Send full job objects to Kafka instead of just IDs
	if err := kafka.NewJobProducer("JobsTopic", jobs); err != nil {
		log.Printf("Error sending jobs to Kafka: %v", err)
	} else {
		log.Printf("Sent %d job objects to Kafka", len(jobs))
		if err := redis.CacheIDs(ctx, jobsCacheKey, uncachedJobs, 1*time.Hour); err != nil {
			log.Printf("Error caching jobs to Redis: %v", err)
		} else {
			log.Printf("Cached %d jobs to Redis", len(uncachedJobs))
		}

		log.Println("Job sync completed")
		log.Printf("Total jobs synced: %d", len(uncachedJobs))
	}
}

func (d *DataSyncService) syncUpdates() {
	ctx := context.Background()

	// Use separate mutexes for different data types to reduce contention
	var storiesIDs []int
	var asksIDs []int
	var commentsIDs []int
	var jobsIDs []int
	var pollsIDs []int
	var pollOptionsIDs []int
	var userIDs []string

	var uncachedIDs []int
	var uncachedUsers []string

	var wg sync.WaitGroup
	var storiesMu, asksMu, commentsMu, jobsMu, pollsMu, pollOptionsMu sync.Mutex

	log.Println("Starting update sync...")

	update, err := d.updateService.FetchUpdates(ctx)
	if err != nil {
		log.Printf("Error fetching updates: %v", err)
		return
	}

	if len(update.IDs) != 0 || len(update.Profiles) != 0 {
		uncachedIDs, err = redis.GetUncachedItems(ctx, updatesCacheKey, update.IDs)
		if err != nil {
			log.Printf("Error checking uncached IDS: %v", err)
			return
		} else if len(uncachedIDs) == 0 {
			log.Println("All updates IDS are already cached, skipping fetch")
		}

		uncachedUsers, err = redis.GetUncachedUserIDs(ctx, updatesUserCacheKey, update.Profiles)
		if err != nil {
			log.Printf("Error checking uncached user IDs: %v", err)
			return
		} else if len(uncachedUsers) == 0 {
			log.Println("All user IDs are already cached, skipping fetch")
		}

		// Early return if nothing to process
		if len(uncachedIDs) == 0 && len(uncachedUsers) == 0 {
			log.Println("Nothing to sync, returning early")
			return
		}
	}

	// Process items to determine types
	for _, itemID := range uncachedIDs {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Fetch raw item to determine type
			var rawItem map[string]interface{}

			err := d.apiClient.GetItem(ctx, id, &rawItem)
			if err != nil {
				log.Printf("Error fetching item %d: %v", id, err)
				return
			}

			itemType, ok := rawItem["type"].(string)
			if !ok {
				log.Printf("Item %d has no valid type", id)
				return
			}

			// Process based on type with separate mutexes
			switch itemType {
			case "story":
				storiesMu.Lock()
				storiesIDs = append(storiesIDs, id)
				storiesMu.Unlock()

			case "ask":
				asksMu.Lock()
				asksIDs = append(asksIDs, id)
				asksMu.Unlock()

			case "comment":
				commentsMu.Lock()
				commentsIDs = append(commentsIDs, id)
				commentsMu.Unlock()

			case "job":
				jobsMu.Lock()
				jobsIDs = append(jobsIDs, id)
				jobsMu.Unlock()

			case "poll":
				pollsMu.Lock()
				pollsIDs = append(pollsIDs, id)
				pollsMu.Unlock()

			case "pollopt":
				pollOptionsMu.Lock()
				pollOptionsIDs = append(pollOptionsIDs, id)
				pollOptionsMu.Unlock()
			}
		}(itemID)
	}

	// Process users directly without goroutines (no API calls needed)
	userIDs = make([]string, len(uncachedUsers))
	copy(userIDs, uncachedUsers)

	wg.Wait()

	// Save to database concurrently
	var saveWg sync.WaitGroup

	// Save stories
	if len(storiesIDs) > 0 {
		saveWg.Add(1)
		go func(ids []int) {
			defer saveWg.Done()
			stories, err := d.storyService.FetchMultiple(ctx, ids)
			if err != nil {
				log.Printf("Error fetching STORIES: %v", err)
				return
			}
			if len(stories) != 0 {
				// Send full story objects instead of IDs
				if err := kafka.NewStoryProducer("StoriesTopic", stories); err != nil {
					log.Printf("Error sending STORIES to Kafka: %v", err)
				} else {
					log.Printf("Sent %d story objects to Kafka", len(stories))
					redis.CacheIDs(ctx, updatesCacheKey, ids, 120*time.Second)
					log.Printf("---------------Cached %d STORIES to Redis---------------", len(ids))
				}
			}
		}(storiesIDs)
	}

	// Save asks section - updated
	if len(asksIDs) > 0 {
		saveWg.Add(1)
		go func(ids []int) {
			defer saveWg.Done()
			asks, err := d.askService.FetchMultiple(ctx, ids)
			if err != nil {
				log.Printf("Error fetching ASKS: %v", err)
				return
			}
			if len(asks) != 0 {
				if err := kafka.NewAskProducer("AsksTopic", asks); err != nil {
					log.Printf("Error sending ASKS to Kafka: %v", err)
				} else {
					log.Printf("Sent %d ask objects to Kafka", len(asks))
					redis.CacheIDs(ctx, updatesCacheKey, ids, 120*time.Second)
					log.Printf("---------------Cached %d ASKS to Redis---------------", len(ids))
				}
			}
		}(asksIDs)
	}

	// Save comments section - updated
	if len(commentsIDs) > 0 {
		saveWg.Add(1)
		go func(ids []int) {
			defer saveWg.Done()
			comments, err := d.commentService.FetchMultiple(ctx, ids)
			if err != nil {
				log.Printf("Error fetching COMMENTS: %v", err)
				return
			}

			if len(comments) != 0 {
				if err := kafka.NewCommentProducer("CommentsTopic", comments); err != nil {
					log.Printf("Error sending COMMENTS to Kafka: %v", err)
				} else {
					log.Printf("Sent %d comment objects to Kafka", len(comments))
					redis.CacheIDs(ctx, updatesCacheKey, ids, 120*time.Second)
					log.Printf("---------------Cached %d COMMENTS to Redis---------------", len(ids))
				}
			}
		}(commentsIDs)
	}

	// Save jobs section - updated
	if len(jobsIDs) > 0 {
		saveWg.Add(1)
		go func(ids []int) {
			defer saveWg.Done()
			jobs, err := d.jobService.FetchMultiple(ctx, ids)
			if err != nil {
				log.Printf("Error fetching JOBS: %v", err)
				return
			}
			if len(jobs) != 0 {
				if err := kafka.NewJobProducer("JobsTopic", jobs); err != nil {
					log.Printf("Error sending JOBS to Kafka: %v", err)
				} else {
					log.Printf("Sent %d job objects to Kafka", len(jobs))
					redis.CacheIDs(ctx, updatesCacheKey, ids, 120*time.Second)
					log.Printf("---------------Cached %d JOBS to Redis---------------", len(ids))
				}
			}
		}(jobsIDs)
	}

	// Save polls section - updated
	if len(pollsIDs) > 0 {
		saveWg.Add(1)
		go func(ids []int) {
			defer saveWg.Done()
			polls, err := d.pollService.FetchMultiple(ctx, ids)
			if err != nil {
				log.Printf("Error fetching POLLS: %v", err)
				return
			}
			if len(polls) != 0 {
				if err := kafka.NewPollProducer("PollsTopic", polls); err != nil {
					log.Printf("Error sending POLLS to Kafka: %v", err)
				} else {
					log.Printf("Sent %d poll objects to Kafka", len(polls))
					redis.CacheIDs(ctx, updatesCacheKey, ids, 120*time.Second)
					log.Printf("---------------Cached %d POLLS to Redis---------------", len(ids))
				}
			}
		}(pollsIDs)
	}

	// Save poll options section - updated
	if len(pollOptionsIDs) > 0 {
		saveWg.Add(1)
		go func(ids []int) {
			defer saveWg.Done()
			pollOptions, err := d.pollOptionService.FetchMultiple(ctx, ids)
			if err != nil {
				log.Printf("Error fetching POLL OPTIONS: %v", err)
				return
			}
			if len(pollOptions) != 0 {
				if err := kafka.NewPollOptionProducer("PollOptionsTopic", pollOptions); err != nil {
					log.Printf("Error sending POLL OPTIONS to Kafka: %v", err)
				} else {
					log.Printf("Sent %d poll option objects to Kafka", len(pollOptions))
					redis.CacheIDs(ctx, updatesCacheKey, ids, 120*time.Second)
					log.Printf("---------------Cached %d POLL OPTIONS to Redis---------------", len(ids))
				}
			}
		}(pollOptionsIDs)
	}

	// Save users section - updated
	if len(userIDs) > 0 {
		saveWg.Add(1)
		go func(ids []string) {
			defer saveWg.Done()
			users, err := d.userService.FetchMultipleByUsernames(ctx, ids)
			if err != nil {
				log.Printf("Error fetching USERS: %v", err)
				return
			}
			if len(users) != 0 {
				if err := kafka.NewUserProducer("UsersTopic", users); err != nil {
					log.Printf("Error sending USERS to Kafka: %v", err)
				} else {
					log.Printf("Sent %d user objects to Kafka", len(users))
					redis.CacheUserIDs(ctx, updatesUserCacheKey, ids, 120*time.Second)
					log.Printf("---------------Cached %d USERS to Redis---------------", len(ids))
				}
			}
		}(userIDs)
	}

	saveWg.Wait()

	log.Printf("Update sync completed - Stories: %d, Asks: %d, Comments: %d, Jobs: %d, Polls: %d, Poll Options: %d, Users: %d",
		len(storiesIDs), len(asksIDs), len(commentsIDs), len(jobsIDs), len(pollsIDs), len(pollOptionsIDs), len(userIDs))
}
