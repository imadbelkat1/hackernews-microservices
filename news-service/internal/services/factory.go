package services

import (
	api "news-service/internal/api"
)

// HackerNewsApiServiceFactory creates all API services
type HackerNewsApiServiceFactory struct {
	client *api.HackerNewsApiClient
}

func NewHackerNewsApiServiceFactory() *HackerNewsApiServiceFactory {
	return &HackerNewsApiServiceFactory{
		client: api.NewHackerNewsApiClient(),
	}
}

func (f *HackerNewsApiServiceFactory) CreateUserService() UserApiFetcher {
	return NewUserApiService(f.client)
}

func (f *HackerNewsApiServiceFactory) CreateStoryService() StoryApiFetcher {
	return NewStoryApiService(f.client)
}

func (f *HackerNewsApiServiceFactory) CreateCommentService() CommentApiFetcher {
	return NewCommentApiService(f.client)
}

func (f *HackerNewsApiServiceFactory) CreateAskService() AskApiFetcher {
	return NewAskApiService(f.client)
}

func (f *HackerNewsApiServiceFactory) CreateJobService() JobApiFetcher {
	return NewJobApiService(f.client)
}

func (f *HackerNewsApiServiceFactory) CreatePollService() PollApiFetcher {
	return NewPollApiService(f.client)
}

func (f *HackerNewsApiServiceFactory) CreatePollOptionService() PollOptionApiFetcher {
	return NewPollOptionApiService(f.client)
}
