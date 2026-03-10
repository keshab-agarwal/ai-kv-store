package server

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"kvstore/proto/kvpb"
	"kvstore/storage"
)

type KVServer struct {
	kvpb.UnimplementedKVServiceServer
	engine *storage.Engine
}

func NewKVServer(engine *storage.Engine) *KVServer {
	return &KVServer{engine: engine}
}

func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	value, err := s.engine.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &kvpb.GetResponse{Value: value}, nil
}

func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	err := s.engine.Put(req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	return &kvpb.PutResponse{}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	err := s.engine.Delete(req.Key)
	if err != nil {
		return nil, err
	}
	return &kvpb.DeleteResponse{}, nil
}

func StartServer(engine *storage.Engine, address string) (net.Listener, *grpc.Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, err
	}
	grpcServer := grpc.NewServer()
	kvpb.RegisterKVServiceServer(grpcServer, NewKVServer(engine))
	go grpcServer.Serve(listener)
	return listener, grpcServer, nil
}
