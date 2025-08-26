package services

import (
	"context"
	"fmt"
	"sync"

	api "news-service/internal/api"
	"news-service/internal/models"
)

// UserApiService implements UserApiFetcher
type UserApiService struct {
	client *api.HackerNewsApiClient
}

// NewUserApiService creates a new user API service
func NewUserApiService(client *api.HackerNewsApiClient) *UserApiService {
	return &UserApiService{client: client}
}

func (s *UserApiService) FetchByID(ctx context.Context, id int) (*models.User, error) {
	var user models.User
	err := s.client.GetItem(ctx, id, &user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (s *UserApiService) FetchByUsername(ctx context.Context, username string) (*models.User, error) {
	var user models.User
	endpoint := fmt.Sprintf("/user/%s.json", username)
	err := s.client.Get(ctx, endpoint, &user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (s *UserApiService) FetchMultiple(ctx context.Context, ids []int) ([]*models.User, error) {
	var wg sync.WaitGroup
	results := make([]*models.User, len(ids))
	errors := make([]error, len(ids))

	for i, id := range ids {
		wg.Add(1)
		go func(index, userID int) {
			defer wg.Done()
			user, err := s.FetchByID(ctx, userID)
			results[index] = user
			errors[index] = err
		}(i, id)
	}

	wg.Wait()

	// Filter out nil results and collect valid users
	var validUsers []*models.User
	for i, user := range results {
		if errors[i] == nil && user != nil {
			validUsers = append(validUsers, user)
		}
	}

	return validUsers, nil
}

func (s *UserApiService) FetchMultipleByUsernames(ctx context.Context, usernames []string) ([]*models.User, error) {
	var wg sync.WaitGroup
	results := make([]*models.User, len(usernames))
	errors := make([]error, len(usernames))

	for i, username := range usernames {
		wg.Add(1)
		go func(index int, user string) {
			defer wg.Done()
			userData, err := s.FetchByUsername(ctx, user)
			results[index] = userData
			errors[index] = err
		}(i, username)
	}

	wg.Wait()

	// Filter out nil results and collect valid users
	var validUsers []*models.User
	for i, user := range results {
		if errors[i] == nil && user != nil {
			validUsers = append(validUsers, user)
		}
	}

	return validUsers, nil
}

