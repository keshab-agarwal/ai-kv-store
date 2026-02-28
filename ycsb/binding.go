package ycsb

import (
	"crypto/md5"

	"ai-kv-store/kvstore"
	"ai-kv-store/kvstore/client"
)

// DB is the YCSB-compatible database interface.
type DB interface {
	Read(key string) (kvstore.Status, []byte, error)
	Insert(key string, value []byte) (kvstore.Status, error)
	Update(key string, value []byte) (kvstore.Status, error)
	Delete(key string) (kvstore.Status, error)
	Scan(startKey string, count int) (kvstore.Status, error)
}

// KVBinding maps YCSB DB operations to the KV store client.
type KVBinding struct {
	client *client.Client
}

func NewKVBinding(nodes []string) *KVBinding {
	return &KVBinding{client: client.New(nodes)}
}

func NewKVBindingWithClient(c *client.Client) *KVBinding {
	return &KVBinding{client: c}
}

func (b *KVBinding) Read(key string) (kvstore.Status, []byte, error) {
	k := stringToKey(key)
	result := b.client.Get(k)
	return result.Status, result.Value, nil
}

func (b *KVBinding) Insert(key string, value []byte) (kvstore.Status, error) {
	k := stringToKey(key)
	result := b.client.Put(k, value)
	return result.Status, nil
}

func (b *KVBinding) Update(key string, value []byte) (kvstore.Status, error) {
	k := stringToKey(key)
	result := b.client.Put(k, value)
	return result.Status, nil
}

func (b *KVBinding) Delete(key string) (kvstore.Status, error) {
	k := stringToKey(key)
	result := b.client.Delete(k)
	return result.Status, nil
}

func (b *KVBinding) Scan(startKey string, count int) (kvstore.Status, error) {
	return kvstore.StatusOK, nil
}

func stringToKey(s string) kvstore.Key {
	hash := md5.Sum([]byte(s))
	return kvstore.Key(hash)
}
