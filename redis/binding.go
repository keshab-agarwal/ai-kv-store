package redis

import (
	"context"
	"fmt"
	"time"

	"ai-kv-store/redis/ycsb"

	goredis "github.com/redis/go-redis/v9"
)

// RedisConfig holds connection and pool parameters for the Redis binding.
type RedisConfig struct {
	Addrs        []string
	Password     string
	DB           int
	PoolSize     int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	OpTimeout    time.Duration
	ClusterMode  bool
	TLSEnabled   bool
}

func DefaultRedisConfig() RedisConfig {
	return RedisConfig{
		Addrs:        []string{"localhost:6379"},
		Password:     "",
		DB:           0,
		PoolSize:     128,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		OpTimeout:    5 * time.Second,
	}
}

// RedisBinding implements ycsb.DB backed by Redis.
type RedisBinding struct {
	client  goredis.UniversalClient
	timeout time.Duration
}

var _ ycsb.DB = (*RedisBinding)(nil)

func NewRedisBinding(cfg RedisConfig) (*RedisBinding, error) {
	var client goredis.UniversalClient

	if cfg.ClusterMode {
		client = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:        cfg.Addrs,
			Password:     cfg.Password,
			PoolSize:     cfg.PoolSize,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		})
	} else {
		addr := "localhost:6379"
		if len(cfg.Addrs) > 0 {
			addr = cfg.Addrs[0]
		}
		client = goredis.NewClient(&goredis.Options{
			Addr:         addr,
			Password:     cfg.Password,
			DB:           cfg.DB,
			PoolSize:     cfg.PoolSize,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.OpTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &RedisBinding{
		client:  client,
		timeout: cfg.OpTimeout,
	}, nil
}

func (b *RedisBinding) Read(key string) (ycsb.Status, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	val, err := b.client.Get(ctx, key).Bytes()
	if err == goredis.Nil {
		return ycsb.StatusNotFound, nil, nil
	}
	if err != nil {
		return ycsb.StatusError, nil, err
	}
	return ycsb.StatusFound, val, nil
}

func (b *RedisBinding) Insert(key string, value []byte) (ycsb.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	if err := b.client.Set(ctx, key, value, 0).Err(); err != nil {
		return ycsb.StatusError, err
	}
	return ycsb.StatusOK, nil
}

func (b *RedisBinding) Update(key string, value []byte) (ycsb.Status, error) {
	return b.Insert(key, value)
}

func (b *RedisBinding) Delete(key string) (ycsb.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	if err := b.client.Del(ctx, key).Err(); err != nil {
		return ycsb.StatusError, err
	}
	return ycsb.StatusOK, nil
}

func (b *RedisBinding) Scan(_ string, _ int) (ycsb.Status, error) {
	return ycsb.StatusOK, nil
}

// Close releases the underlying connection pool.
func (b *RedisBinding) Close() error {
	return b.client.Close()
}

// FlushDB removes all keys in the current database — call before benchmark runs
// to ensure a clean baseline.
func (b *RedisBinding) FlushDB() error {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	return b.client.FlushDB(ctx).Err()
}

// DBSize returns the number of keys in the current database.
func (b *RedisBinding) DBSize() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	return b.client.DBSize(ctx).Result()
}

// Info returns Redis server info for the given section (e.g. "server", "memory").
func (b *RedisBinding) Info(section string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	return b.client.Info(ctx, section).Result()
}
