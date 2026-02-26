package node

import (
	"kv-store/interfaces"
	"kv-store/internal/version"
)

type PutReq struct {
	Key     interfaces.Key
	Value   interfaces.Value
	ClientVV version.Vector
}

type PutResp struct {
	VV  version.Vector
	Err string
}

type GetReq struct {
	Key     interfaces.Key
	ClientVV version.Vector
}

type GetResp struct {
	Value interfaces.Value
	VV    version.Vector
	Found bool
	Err   string
}

type ReplicateReq struct {
	Key   interfaces.Key
	Value interfaces.Value
	VV    version.Vector
}

type ReplicateResp struct {
	Err string
}
