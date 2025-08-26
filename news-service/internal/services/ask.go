package services

import (
	"context"
	"sync"

	api "news-service/internal/api"
	"news-service/internal/models"
)

// AskApiService implements AskApiFetcher
type AskApiService struct {
	client *api.HackerNewsApiClient
}

func NewAskApiService(client *api.HackerNewsApiClient) *AskApiService {
	return &AskApiService{client: client}
}

func (s *AskApiService) FetchByID(ctx context.Context, id int) (*models.Ask, error) {
	var ask models.Ask
	err := s.client.GetItem(ctx, id, &ask)
	if err != nil {
		return nil, err
	}
	return &ask, nil
}

func (s *AskApiService) FetchMultiple(ctx context.Context, ids []int) ([]*models.Ask, error) {
	var wg sync.WaitGroup
	results := make([]*models.Ask, len(ids))
	errors := make([]error, len(ids))

	for i, id := range ids {
		wg.Add(1)
		go func(index, askID int) {
			defer wg.Done()
			ask, err := s.FetchByID(ctx, askID)
			results[index] = ask
			errors[index] = err
		}(i, id)
	}

	wg.Wait()

	var validAsks []*models.Ask
	for i, ask := range results {
		if errors[i] == nil && ask != nil {
			validAsks = append(validAsks, ask)
		}
	}

	return validAsks, nil
}

func (s *AskApiService) FetchTopItems(ctx context.Context) ([]int, error) {
	return s.FetchAskStories(ctx)
}

func (s *AskApiService) FetchAskStories(ctx context.Context) ([]int, error) {
	return s.client.GetItemList(ctx, "/askstories.json")
}
