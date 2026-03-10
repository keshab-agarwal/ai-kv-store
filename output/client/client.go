package client

import (
	"context"

	"google.golang.org/grpc"
	"kvstore/proto/kvpb"
)

type KVClient struct {
	client kvpb.KVServiceClient
}

func NewKVClient(conn *grpc.ClientConn) *KVClient {
	return &KVClient{client: kvpb.NewKVServiceClient(conn)}
}

func (c *KVClient) Get(ctx context.Context, key []byte) ([]byte, error) {
	resp, err := c.client.Get(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

func (c *KVClient) Put(ctx context.Context, key, value []byte) error {
	_, err := c.client.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
	return err
}

func (c *KVClient) Delete(ctx context.Context, key []byte) error {
	_, err := c.client.Delete(ctx, &kvpb.DeleteRequest{Key: key})
	return err
}
