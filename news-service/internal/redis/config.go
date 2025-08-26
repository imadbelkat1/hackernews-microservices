package redis

import (
	"news-service/config"
)

// RedisConfig holds the configuration for Redis
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// GetRedisConfig returns the Redis configuration from environment variables
func GetRedisConfig() RedisConfig {
	return RedisConfig{
		Addr:     config.GetEnv("REDIS_ADDR", "localhost:6379"),
		Password: config.GetEnv("REDIS_PASSWORD", ""),
		DB:       config.GetEnvInt("REDIS_DB", 0),
	}
}
