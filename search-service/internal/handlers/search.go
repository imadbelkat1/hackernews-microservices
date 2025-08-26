package handlers

import (
"encoding/json"
"net/http"
"strconv"

"search-service/internal/opensearch"

"github.com/gorilla/mux"
)

type SearchHandler struct {
openSearchService *opensearch.OpenSearchService
}

func NewSearchHandler(openSearchService *opensearch.OpenSearchService) *SearchHandler {
return &SearchHandler{
openSearchService: openSearchService,
}
}

func (h *SearchHandler) SearchStories(w http.ResponseWriter, r *http.Request) {
query := r.URL.Query().Get("q")
if query == "" {
http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
return
}

limit := 20
if l := r.URL.Query().Get("limit"); l != "" {
if parsed, err := strconv.Atoi(l); err == nil {
limit = parsed
}
}

// TODO: Implement actual OpenSearch query
results := map[string]interface{}{
"query":   query,
"type":    "stories",
"results": []interface{}{},
"total":   0,
"limit":   limit,
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(results)
}

func (h *SearchHandler) SearchUsers(w http.ResponseWriter, r *http.Request) {
query := r.URL.Query().Get("q")
if query == "" {
http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
return
}

results := map[string]interface{}{
"query":   query,
"type":    "users",
"results": []interface{}{},
"total":   0,
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(results)
}

func (h *SearchHandler) SearchComments(w http.ResponseWriter, r *http.Request) {
query := r.URL.Query().Get("q")
if query == "" {
http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
return
}

results := map[string]interface{}{
"query":   query,
"type":    "comments",
"results": []interface{}{},
"total":   0,
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(results)
}

func (h *SearchHandler) SearchJobs(w http.ResponseWriter, r *http.Request) {
query := r.URL.Query().Get("q")
if query == "" {
http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
return
}

results := map[string]interface{}{
"query":   query,
"type":    "jobs",
"results": []interface{}{},
"total":   0,
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(results)
}

func (h *SearchHandler) SearchAll(w http.ResponseWriter, r *http.Request) {
query := r.URL.Query().Get("q")
if query == "" {
http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
return
}

results := map[string]interface{}{
"query":   query,
"type":    "all",
"results": map[string]interface{}{
"stories":  []interface{}{},
"users":    []interface{}{},
"comments": []interface{}{},
"jobs":     []interface{}{},
},
"total": 0,
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(results)
}
