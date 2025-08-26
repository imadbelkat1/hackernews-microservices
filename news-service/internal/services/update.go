package services

import (
	"context"
	"log"

	api "news-service/internal/api"
	"news-service/internal/models"
)

type UpdateApiService struct {
	Client *api.HackerNewsApiClient
}

func NewUpdateApiService(client *api.HackerNewsApiClient) *UpdateApiService {
	return &UpdateApiService{Client: client}
}

func (s *UpdateApiService) FetchUpdates(ctx context.Context) (models.Update, error) {
	var update models.Update
	err := s.Client.Get(ctx, "/updates.json", &update)
	if err != nil {
		log.Printf("Error fetching updates: %v", err)
		return models.Update{}, err
	}

	return update, nil
}

