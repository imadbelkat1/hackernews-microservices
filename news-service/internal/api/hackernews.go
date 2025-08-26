package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HackerNewsApiClient handles HTTP requests to the Hacker News API
type HackerNewsApiClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewHackerNewsApiClient creates a new API client
func NewHackerNewsApiClient() *HackerNewsApiClient {
	return &HackerNewsApiClient{
		baseURL: "https://hacker-news.firebaseio.com/v0",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Get performs a GET request to the specified endpoint
func (c *HackerNewsApiClient) Get(ctx context.Context, endpoint string, result interface{}) error {
	url := fmt.Sprintf("%s%s", c.baseURL, endpoint)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to perform request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API request failed with status %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

// GetItem fetches a single item by ID
func (c *HackerNewsApiClient) GetItem(ctx context.Context, id int, result interface{}) error {
	endpoint := fmt.Sprintf("/item/%d.json", id)
	return c.Get(ctx, endpoint, result)
}

// GetItemList fetches a list of item IDs from the specified endpoint
func (c *HackerNewsApiClient) GetItemList(ctx context.Context, endpoint string) ([]int, error) {
	var ids []int
	err := c.Get(ctx, endpoint, &ids)
	return ids, err
}

// GetMaxItemID retrieves the maximum item ID from the API
func (c *HackerNewsApiClient) GetMaxItemID() (int, error) {
	var maxItem int
	err := c.Get(context.Background(), "/maxitem.json", &maxItem)
	if err != nil {
		return 0, fmt.Errorf("failed to get max item ID: %w", err)
	}
	return maxItem, nil
}

