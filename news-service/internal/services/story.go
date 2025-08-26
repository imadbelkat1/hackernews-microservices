package services

import (
	"context"
	"sync"

	api "news-service/internal/api"
	"news-service/internal/models"
)

// StoryApiService implements StoryApiFetcher
type StoryApiService struct {
	client *api.HackerNewsApiClient
}

func NewStoryApiService(client *api.HackerNewsApiClient) *StoryApiService {
	return &StoryApiService{client: client}
}

func (s *StoryApiService) FetchByID(ctx context.Context, id int) (*models.Story, error) {
	var story models.Story
	err := s.client.GetItem(ctx, id, &story)
	if err != nil {
		return nil, err
	}
	return &story, nil
}

func (s *StoryApiService) FetchMultiple(ctx context.Context, ids []int) ([]*models.Story, error) {
	var wg sync.WaitGroup
	results := make([]*models.Story, len(ids))
	errors := make([]error, len(ids))

	for i, id := range ids {
		wg.Add(1)
		go func(index, storyID int) {
			defer wg.Done()
			story, err := s.FetchByID(ctx, storyID)
			results[index] = story
			errors[index] = err
		}(i, id)
	}

	wg.Wait()

	var validStories []*models.Story
	for i, story := range results {
		if errors[i] == nil && story != nil {
			validStories = append(validStories, story)
		}
	}

	return validStories, nil
}

func (s *StoryApiService) FetchTopItems(ctx context.Context) ([]int, error) {
	return s.FetchTopStories(ctx)
}

func (s *StoryApiService) FetchTopStories(ctx context.Context) ([]int, error) {
	return s.client.GetItemList(ctx, "/topstories.json")
}

func (s *StoryApiService) FetchNewStories(ctx context.Context) ([]int, error) {
	return s.client.GetItemList(ctx, "/newstories.json")
}

func (s *StoryApiService) FetchBestStories(ctx context.Context) ([]int, error) {
	return s.client.GetItemList(ctx, "/beststories.json")
}

