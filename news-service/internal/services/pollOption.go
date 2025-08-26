package services

import (
	"context"
	"sync"

	api "news-service/internal/api"
	"news-service/internal/models"
)

// PollOptionApiService implements PollOptionApiFetcher
type PollOptionApiService struct {
	client *api.HackerNewsApiClient
}

func NewPollOptionApiService(client *api.HackerNewsApiClient) *PollOptionApiService {
	return &PollOptionApiService{client: client}
}

func (s *PollOptionApiService) FetchByID(ctx context.Context, id int) (*models.PollOption, error) {
	var pollOption models.PollOption
	err := s.client.GetItem(ctx, id, &pollOption)
	if err != nil {
		return nil, err
	}
	return &pollOption, nil
}

func (s *PollOptionApiService) FetchMultiple(ctx context.Context, ids []int) ([]*models.PollOption, error) {
	var wg sync.WaitGroup
	results := make([]*models.PollOption, len(ids))
	errors := make([]error, len(ids))

	for i, id := range ids {
		wg.Add(1)
		go func(index, pollOptionID int) {
			defer wg.Done()
			pollOption, err := s.FetchByID(ctx, pollOptionID)
			results[index] = pollOption
			errors[index] = err
		}(i, id)
	}

	wg.Wait()

	var validPollOptions []*models.PollOption
	for i, pollOption := range results {
		if errors[i] == nil && pollOption != nil {
			validPollOptions = append(validPollOptions, pollOption)
		}
	}

	return validPollOptions, nil
}

func (s *PollOptionApiService) FetchTopItems(ctx context.Context) ([]int, error) {
	return []int{}, nil
}

