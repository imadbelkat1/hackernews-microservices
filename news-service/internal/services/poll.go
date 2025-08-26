package services

import (
	"context"
	"sync"

	api "news-service/internal/api"
	"news-service/internal/models"
)

// PollApiService implements PollApiFetcher
type PollApiService struct {
	client *api.HackerNewsApiClient
}

func NewPollApiService(client *api.HackerNewsApiClient) *PollApiService {
	return &PollApiService{client: client}
}

func (s *PollApiService) FetchByID(ctx context.Context, id int) (*models.Poll, error) {
	var poll models.Poll
	err := s.client.GetItem(ctx, id, &poll)
	if err != nil {
		return nil, err
	}
	return &poll, nil
}

func (s *PollApiService) FetchMultiple(ctx context.Context, ids []int) ([]*models.Poll, error) {
	var wg sync.WaitGroup
	results := make([]*models.Poll, len(ids))
	errors := make([]error, len(ids))

	for i, id := range ids {
		wg.Add(1)
		go func(index, pollID int) {
			defer wg.Done()
			poll, err := s.FetchByID(ctx, pollID)
			results[index] = poll
			errors[index] = err
		}(i, id)
	}

	wg.Wait()

	var validPolls []*models.Poll
	for i, poll := range results {
		if errors[i] == nil && poll != nil {
			validPolls = append(validPolls, poll)
		}
	}

	return validPolls, nil
}

func (s *PollApiService) FetchTopItems(ctx context.Context) ([]int, error) {
	return []int{}, nil
}

