package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func CacheIDs(ctx context.Context, key string, ids []int, expirationTime time.Duration) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:     GetRedisConfig().Addr,
		Password: GetRedisConfig().Password,
		DB:       GetRedisConfig().DB,
	})

	idsJSON, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("failed to marshal IDs: %w", err)
	}

	err = rdb.Set(ctx, key, string(idsJSON), expirationTime).Err()
	if err != nil {
		return fmt.Errorf("failed to set user IDs in Redis: %w", err)
	}

	return nil
}

func CacheUserIDs(ctx context.Context, key string, ids []string, expirationTime time.Duration) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:     GetRedisConfig().Addr,
		Password: GetRedisConfig().Password,
		DB:       GetRedisConfig().DB,
	})

	idsJSON, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("failed to marshal user IDs: %w", err)
	}

	err = rdb.Set(ctx, key, string(idsJSON), expirationTime).Err()
	if err != nil {
		return fmt.Errorf("failed to set user IDs in Redis: %w", err)
	}

	return nil
}

