package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func IsItemCached(ctx context.Context, key string, id int) (bool, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     GetRedisConfig().Addr,
		Password: GetRedisConfig().Password,
		DB:       GetRedisConfig().DB,
	})

	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil // Key does not exist
		}
		return false, fmt.Errorf("failed to get value from Redis: %w", err)
	}

	var cachedIDs []int
	if err := json.Unmarshal([]byte(val), &cachedIDs); err != nil {
		return false, fmt.Errorf("failed to unmarshal IDs: %w", err)
	}

	for _, cachedID := range cachedIDs {
		if cachedID == id {
			return true, nil // ID is cached
		}
	}

	return false, nil // ID is not cached
}

func IsUserIDCached(ctx context.Context, key string, id string) (bool, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     GetRedisConfig().Addr,
		Password: GetRedisConfig().Password,
		DB:       GetRedisConfig().DB,
	})

	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil // Key does not exist
		}
		return false, fmt.Errorf("failed to get value from Redis: %w", err)
	}

	var cachedIDs []string
	if err := json.Unmarshal([]byte(val), &cachedIDs); err != nil {
		return false, fmt.Errorf("failed to unmarshal IDs: %w", err)
	}

	for _, cachedID := range cachedIDs {
		if cachedID == id {
			return true, nil // ID is cached
		}
	}

	return false, nil // ID is not cached
}

func GetUncachedItems(ctx context.Context, key string, targetIDs []int) ([]int, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     GetRedisConfig().Addr,
		Password: GetRedisConfig().Password,
		DB:       GetRedisConfig().DB,
	})

	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return targetIDs, nil // Key does not exist, all IDs are uncached
		}
		return nil, fmt.Errorf("failed to get value from Redis: %w", err)
	}

	var cachedIDs []int
	if err := json.Unmarshal([]byte(val), &cachedIDs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal IDs: %w", err)
	}

	var uncachedIDs []int
	for _, targetID := range targetIDs {
		found := false
		for _, id := range cachedIDs {
			if id == targetID {
				found = true
				break
			}
		}
		if !found {
			uncachedIDs = append(uncachedIDs, targetID)
		}
	}

	return uncachedIDs, nil
}

// UncachedUserIDs retrieves user IDs that are not cached in Redis.
func GetUncachedUserIDs(ctx context.Context, key string, targetIDs []string) ([]string, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     GetRedisConfig().Addr,
		Password: GetRedisConfig().Password,
		DB:       GetRedisConfig().DB,
	})

	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return targetIDs, nil // Key does not exist, all IDs are uncached
		}
		return nil, fmt.Errorf("failed to get value from Redis: %w", err)
	}

	var cachedIDs []string
	if err := json.Unmarshal([]byte(val), &cachedIDs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal IDs: %w", err)
	}

	var uncachedIDs []string
	for _, targetID := range targetIDs {
		found := false
		for _, id := range cachedIDs {
			if id == targetID {
				found = true
				break
			}
		}
		if !found {
			uncachedIDs = append(uncachedIDs, targetID)
		}
	}

	return uncachedIDs, nil
}

