package network

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type Request struct {
	Type  string
	Key   []byte
	Value []byte
}

type Response struct {
	Status string
	Data   []byte
}

func SerializeRequest(req *Request) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(req); err != nil {
		return nil, fmt.Errorf("failed to serialize request: %w", err)
	}
	return buf.Bytes(), nil
}

func DeserializeRequest(data []byte) (*Request, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	var req Request
	if err := decoder.Decode(&req); err != nil {
		return nil, fmt.Errorf("failed to deserialize request: %w", err)
	}
	return &req, nil
}

func SerializeResponse(resp *Response) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(resp); err != nil {
		return nil, fmt.Errorf("failed to serialize response: %w", err)
	}
	return buf.Bytes(), nil
}

func DeserializeResponse(data []byte) (*Response, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	var resp Response
	if err := decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to deserialize response: %w", err)
	}
	return &resp, nil
}
