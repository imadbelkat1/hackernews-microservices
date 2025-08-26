package services

import (
	"context"
	"sync"

	api "news-service/internal/api"
	"news-service/internal/models"
)

// JobApiService implements JobApiFetcher
type JobApiService struct {
	client *api.HackerNewsApiClient
}

func NewJobApiService(client *api.HackerNewsApiClient) *JobApiService {
	return &JobApiService{client: client}
}

func (s *JobApiService) FetchByID(ctx context.Context, id int) (*models.Job, error) {
	var job models.Job
	err := s.client.GetItem(ctx, id, &job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *JobApiService) FetchMultiple(ctx context.Context, ids []int) ([]*models.Job, error) {
	var wg sync.WaitGroup
	results := make([]*models.Job, len(ids))
	errors := make([]error, len(ids))

	for i, id := range ids {
		wg.Add(1)
		go func(index, jobID int) {
			defer wg.Done()
			job, err := s.FetchByID(ctx, jobID)
			results[index] = job
			errors[index] = err
		}(i, id)
	}

	wg.Wait()

	var validJobs []*models.Job
	for i, job := range results {
		if errors[i] == nil && job != nil {
			validJobs = append(validJobs, job)
		}
	}

	return validJobs, nil
}

func (s *JobApiService) FetchTopItems(ctx context.Context) ([]int, error) {
	return s.FetchJobStories(ctx)
}

func (s *JobApiService) FetchJobStories(ctx context.Context) ([]int, error) {
	return s.client.GetItemList(ctx, "/jobstories.json")
}

