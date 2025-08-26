package services

import (
	"context"
	"sync"

	api "news-service/internal/api"
	"news-service/internal/models"
)

// CommentApiService implements CommentApiFetcher
type CommentApiService struct {
	client *api.HackerNewsApiClient
}

func NewCommentApiService(client *api.HackerNewsApiClient) *CommentApiService {
	return &CommentApiService{client: client}
}

func (s *CommentApiService) FetchByID(ctx context.Context, id int) (*models.Comment, error) {
	var comment models.Comment
	err := s.client.GetItem(ctx, id, &comment)
	if err != nil {
		return nil, err
	}
	return &comment, nil
}

func (s *CommentApiService) FetchMultiple(ctx context.Context, ids []int) ([]*models.Comment, error) {
	var wg sync.WaitGroup
	results := make([]*models.Comment, len(ids))
	errors := make([]error, len(ids))

	for i, id := range ids {
		wg.Add(1)
		go func(index, commentID int) {
			defer wg.Done()
			comment, err := s.FetchByID(ctx, commentID)
			results[index] = comment
			errors[index] = err
		}(i, id)
	}

	wg.Wait()

	var validComments []*models.Comment
	for i, comment := range results {
		if errors[i] == nil && comment != nil {
			validComments = append(validComments, comment)
		}
	}

	return validComments, nil
}

func (s *CommentApiService) FetchTopItems(ctx context.Context) ([]int, error) {
	return []int{}, nil
}

