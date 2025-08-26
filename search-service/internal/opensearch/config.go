package opensearch

import (
	"hackernews-services/config"
)

type OpenSearchConfig struct {
	URLs     []string `yaml:"urls"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	Index    string   `yaml:"index"`
}

func GetOpenSearchConfig() OpenSearchConfig {
	return OpenSearchConfig{
		URLs:     []string{config.GetEnv("OPENSEARCH_URLS", "http://localhost:9200")},
		Username: config.GetEnv("OPENSEARCH_USERNAME", "admin"),
		Password: config.GetEnv("OPENSEARCH_PASSWORD", "admin"),
		Index:    config.GetEnv("OPENSEARCH_INDEX", "hackernews"),
	}
}
