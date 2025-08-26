package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"search-service/internal/models"

	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

type OpenSearchService struct {
	client *opensearch.Client
	config OpenSearchConfig
}

func (s *OpenSearchService) GetClient() *opensearch.Client {
	return s.client
}

func NewOpenSearchService() (*OpenSearchService, error) {
	config := GetOpenSearchConfig()

	// Create OpenSearch client
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: config.URLs,
		Username:  config.Username,
		Password:  config.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenSearch client: %w", err)
	}

	service := &OpenSearchService{
		client: client,
		config: config,
	}

	// Test connection
	if err := service.TestConnection(); err != nil {
		return nil, fmt.Errorf("failed to connect to OpenSearch: %w", err)
	}

	log.Println("OpenSearch service initialized successfully")
	return service, nil
}

// TestConnection tests the connection to OpenSearch
func (s *OpenSearchService) TestConnection() error {
	req := opensearchapi.InfoRequest{}
	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("OpenSearch connection failed: %s", res.String())
	}

	log.Printf("Connected to OpenSearch cluster: %s", res.String())
	return nil
}

// indexDocument is a generic function to index any document
func (s *OpenSearchService) indexDocument(indexName, docID string, document interface{}) error {
	fullIndexName := fmt.Sprintf("%s-%s", s.config.Index, indexName)

	// Ensure index exists
	if err := s.ensureIndexExists(fullIndexName, indexName); err != nil {
		return err
	}

	// Convert document to JSON
	docJSON, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	// Index the document
	req := opensearchapi.IndexRequest{
		Index:      fullIndexName,
		DocumentID: docID,
		Body:       strings.NewReader(string(docJSON)),
		Refresh:    "wait_for",
	}

	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to index document: %s", res.String())
	}

	log.Printf("Successfully indexed %s document with ID: %s", indexName, docID)
	return nil
}

// IndexStory indexes a story document
func (s *OpenSearchService) IndexStory(story *models.Story) error {
	return s.indexDocument("stories", strconv.Itoa(story.ID), story)
}

// IndexAsk indexes an ask document
func (s *OpenSearchService) IndexAsk(ask *models.Ask) error {
	return s.indexDocument("asks", strconv.Itoa(ask.ID), ask)
}

// IndexComment indexes a comment document
func (s *OpenSearchService) IndexComment(comment *models.Comment) error {
	return s.indexDocument("comments", strconv.Itoa(comment.ID), comment)
}

// IndexJob indexes a job document
func (s *OpenSearchService) IndexJob(job *models.Job) error {
	return s.indexDocument("jobs", strconv.Itoa(job.ID), job)
}

// IndexPoll indexes a poll document
func (s *OpenSearchService) IndexPoll(poll *models.Poll) error {
	return s.indexDocument("polls", strconv.Itoa(poll.ID), poll)
}

// IndexPollOption indexes a poll option document
func (s *OpenSearchService) IndexPollOption(pollOption *models.PollOption) error {
	return s.indexDocument("polloptions", strconv.Itoa(pollOption.ID), pollOption)
}

// IndexUser indexes a user document
func (s *OpenSearchService) IndexUser(user *models.User) error {
	userDoc := map[string]interface{}{
		"username":      user.Username,
		"karma":         user.Karma,
		"about":         user.About,
		"created_at":    user.Created_At,
		"submitted_ids": user.Submitted,
		"type":          "user",
	}

	return s.indexDocument("users", user.Username, userDoc)
}

// CreateIndex creates an index with mapping for a specific type
func (s *OpenSearchService) CreateIndex(indexName string, mapping map[string]interface{}) error {
	req := opensearchapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(fmt.Sprintf(`{"mappings": %s}`, toJSON(mapping))),
	}

	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() && !strings.Contains(res.String(), "resource_already_exists_exception") {
		return fmt.Errorf("failed to create index %s: %s", indexName, res.String())
	}

	log.Printf("Index %s created or already exists", indexName)
	return nil
}

// ensureIndexExists creates the index if it doesn't exist
func (s *OpenSearchService) ensureIndexExists(fullIndexName, docType string) error {
	// Check if index exists
	req := opensearchapi.IndicesExistsRequest{Index: []string{fullIndexName}}
	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// If index doesn't exist (404), create it
	if res.StatusCode == 404 {
		mapping := s.getDefaultMapping(docType)
		return s.CreateIndex(fullIndexName, mapping)
	}

	return nil
}

func (s *OpenSearchService) getDefaultMapping(docType string) map[string]interface{} {
	baseMapping := map[string]interface{}{
		"properties": map[string]interface{}{
			"id": map[string]interface{}{
				"type": "long",
			},
			"type": map[string]interface{}{
				"type": "keyword",
			},
			"time": map[string]interface{}{
				"type":   "date",
				"format": "epoch_second",
			},
			"by": map[string]interface{}{
				"type": "text",
				"fields": map[string]interface{}{
					"keyword": map[string]interface{}{
						"type":         "keyword",
						"ignore_above": 256,
					},
				},
			},
		},
	}

	properties := baseMapping["properties"].(map[string]interface{})

	switch docType {
	case "stories":
		properties["title"] = map[string]interface{}{
			"type":     "text",
			"analyzer": "standard",
		}
		properties["url"] = map[string]interface{}{"type": "keyword"}
		properties["score"] = map[string]interface{}{"type": "integer"}
		properties["descendants"] = map[string]interface{}{"type": "integer"}
		properties["kids"] = map[string]interface{}{"type": "long"}

	case "asks":
		properties["title"] = map[string]interface{}{
			"type":     "text",
			"analyzer": "standard",
		}
		properties["text"] = map[string]interface{}{
			"type":     "text",
			"analyzer": "standard",
		}
		properties["score"] = map[string]interface{}{"type": "integer"}
		properties["descendants"] = map[string]interface{}{"type": "integer"}
		properties["kids"] = map[string]interface{}{"type": "long"}

	case "comments":
		properties["text"] = map[string]interface{}{
			"type":     "text",
			"analyzer": "standard",
		}
		properties["parent"] = map[string]interface{}{"type": "long"}
		properties["kids"] = map[string]interface{}{"type": "long"}

	case "jobs":
		properties["title"] = map[string]interface{}{
			"type":     "text",
			"analyzer": "standard",
		}
		properties["text"] = map[string]interface{}{
			"type":     "text",
			"analyzer": "standard",
		}
		properties["url"] = map[string]interface{}{"type": "keyword"}
		properties["score"] = map[string]interface{}{"type": "integer"}

	case "polls":
		properties["title"] = map[string]interface{}{
			"type":     "text",
			"analyzer": "standard",
		}
		properties["score"] = map[string]interface{}{"type": "integer"}
		properties["parts"] = map[string]interface{}{"type": "long"}
		properties["kids"] = map[string]interface{}{"type": "long"}

	case "polloptions":
		properties["text"] = map[string]interface{}{
			"type":     "text",
			"analyzer": "standard",
		}
		properties["poll"] = map[string]interface{}{"type": "long"}
		properties["score"] = map[string]interface{}{"type": "integer"}

	case "users":
		properties["id"] = map[string]interface{}{
			"type": "keyword",
		}
		properties["karma"] = map[string]interface{}{"type": "integer"}
		properties["about"] = map[string]interface{}{
			"type":     "text",
			"analyzer": "standard",
		}
		properties["created"] = map[string]interface{}{
			"type":   "date",
			"format": "epoch_second",
		}
		properties["submitted"] = map[string]interface{}{"type": "long"}
	}

	return baseMapping
}

func (s *OpenSearchService) BulkIndexStories(stories []*models.Story) error {
	if len(stories) == 0 {
		return nil
	}

	fullIndexName := fmt.Sprintf("%s-stories", s.config.Index)

	// Ensure index exists
	if err := s.ensureIndexExists(fullIndexName, "stories"); err != nil {
		return err
	}

	var body strings.Builder
	for _, story := range stories {
		docID := strconv.Itoa(story.ID)

		// Bulk API format: action line + document line
		actionLine := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, fullIndexName, docID)
		body.WriteString(actionLine + "\n")

		docJSON, err := json.Marshal(story)
		if err != nil {
			log.Printf("Error marshaling story document: %v", err)
			continue
		}
		body.WriteString(string(docJSON) + "\n")
	}

	req := opensearchapi.BulkRequest{
		Body:    strings.NewReader(body.String()),
		Refresh: "true",
	}

	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk index stories failed: %s", res.String())
	}

	log.Printf("Successfully bulk indexed %d stories", len(stories))
	return nil
}

func (s *OpenSearchService) BulkIndexAsks(asks []*models.Ask) error {
	if len(asks) == 0 {
		return nil
	}

	fullIndexName := fmt.Sprintf("%s-asks", s.config.Index)

	// Ensure index exists
	if err := s.ensureIndexExists(fullIndexName, "asks"); err != nil {
		return err
	}

	var body strings.Builder
	for _, ask := range asks {
		docID := strconv.Itoa(ask.ID)

		// Bulk API format: action line + document line
		actionLine := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, fullIndexName, docID)
		body.WriteString(actionLine + "\n")

		docJSON, err := json.Marshal(ask)
		if err != nil {
			log.Printf("Error marshaling ask document: %v", err)
			continue
		}
		body.WriteString(string(docJSON) + "\n")
	}

	req := opensearchapi.BulkRequest{
		Body:    strings.NewReader(body.String()),
		Refresh: "true",
	}

	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk index asks failed: %s", res.String())
	}

	log.Printf("Successfully bulk indexed %d asks", len(asks))
	return nil
}

func (s *OpenSearchService) BulkIndexComments(comments []*models.Comment) error {
	if len(comments) == 0 {
		return nil
	}

	fullIndexName := fmt.Sprintf("%s-comments", s.config.Index)

	// Ensure index exists
	if err := s.ensureIndexExists(fullIndexName, "comments"); err != nil {
		return err
	}

	var body strings.Builder
	for _, comment := range comments {
		docID := strconv.Itoa(comment.ID)

		// Bulk API format: action line + document line
		actionLine := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, fullIndexName, docID)
		body.WriteString(actionLine + "\n")

		docJSON, err := json.Marshal(comment)
		if err != nil {
			log.Printf("Error marshaling comment document: %v", err)
			continue
		}
		body.WriteString(string(docJSON) + "\n")
	}

	req := opensearchapi.BulkRequest{
		Body:    strings.NewReader(body.String()),
		Refresh: "true",
	}

	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk index comments failed: %s", res.String())
	}

	log.Printf("Successfully bulk indexed %d comments", len(comments))
	return nil
}

func (s *OpenSearchService) BulkIndexJobs(jobs []*models.Job) error {
	if len(jobs) == 0 {
		return nil
	}

	fullIndexName := fmt.Sprintf("%s-jobs", s.config.Index)

	// Ensure index exists
	if err := s.ensureIndexExists(fullIndexName, "jobs"); err != nil {
		return err
	}

	var body strings.Builder
	for _, job := range jobs {
		docID := strconv.Itoa(job.ID)

		// Bulk API format: action line + document line
		actionLine := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, fullIndexName, docID)
		body.WriteString(actionLine + "\n")

		docJSON, err := json.Marshal(job)
		if err != nil {
			log.Printf("Error marshaling job document: %v", err)
			continue
		}
		body.WriteString(string(docJSON) + "\n")
	}

	req := opensearchapi.BulkRequest{
		Body:    strings.NewReader(body.String()),
		Refresh: "true",
	}

	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk index jobs failed: %s", res.String())
	}

	log.Printf("Successfully bulk indexed %d jobs", len(jobs))
	return nil
}

func (s *OpenSearchService) BulkIndexPolls(polls []*models.Poll) error {
	if len(polls) == 0 {
		return nil
	}

	fullIndexName := fmt.Sprintf("%s-polls", s.config.Index)

	// Ensure index exists
	if err := s.ensureIndexExists(fullIndexName, "polls"); err != nil {
		return err
	}

	var body strings.Builder
	for _, poll := range polls {
		docID := strconv.Itoa(poll.ID)

		// Bulk API format: action line + document line
		actionLine := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, fullIndexName, docID)
		body.WriteString(actionLine + "\n")

		docJSON, err := json.Marshal(poll)
		if err != nil {
			log.Printf("Error marshaling poll document: %v", err)
			continue
		}
		body.WriteString(string(docJSON) + "\n")
	}

	req := opensearchapi.BulkRequest{
		Body:    strings.NewReader(body.String()),
		Refresh: "true",
	}

	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk index polls failed: %s", res.String())
	}

	log.Printf("Successfully bulk indexed %d polls", len(polls))
	return nil
}

func (s *OpenSearchService) BulkIndexPollOptions(pollOptions []*models.PollOption) error {
	if len(pollOptions) == 0 {
		return nil
	}

	fullIndexName := fmt.Sprintf("%s-polloptions", s.config.Index)

	// Ensure index exists
	if err := s.ensureIndexExists(fullIndexName, "polloptions"); err != nil {
		return err
	}

	var body strings.Builder
	for _, pollOption := range pollOptions {
		docID := strconv.Itoa(pollOption.ID)

		// Bulk API format: action line + document line
		actionLine := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, fullIndexName, docID)
		body.WriteString(actionLine + "\n")

		docJSON, err := json.Marshal(pollOption)
		if err != nil {
			log.Printf("Error marshaling poll option document: %v", err)
			continue
		}
		body.WriteString(string(docJSON) + "\n")
	}

	req := opensearchapi.BulkRequest{
		Body:    strings.NewReader(body.String()),
		Refresh: "true",
	}

	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk index poll options failed: %s", res.String())
	}

	log.Printf("Successfully bulk indexed %d poll options", len(pollOptions))
	return nil
}

func (s *OpenSearchService) BulkIndexUsers(users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	fullIndexName := fmt.Sprintf("%s-users", s.config.Index)

	// Ensure index exists
	if err := s.ensureIndexExists(fullIndexName, "users"); err != nil {
		return err
	}

	var body strings.Builder
	for _, user := range users {
		docID := user.Username

		// Bulk API format: action line + document line
		actionLine := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, fullIndexName, docID)
		body.WriteString(actionLine + "\n")

		docJSON, err := json.Marshal(user)
		if err != nil {
			log.Printf("Error marshaling user document: %v", err)
			continue
		}
		body.WriteString(string(docJSON) + "\n")
	}

	req := opensearchapi.BulkRequest{
		Body:    strings.NewReader(body.String()),
		Refresh: "true",
	}

	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk index users failed: %s", res.String())
	}

	log.Printf("Successfully bulk indexed %d users", len(users))
	return nil
}

// Helper function to convert to JSON string
func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
