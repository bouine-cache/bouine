// Copyright 2022 Théotime Levêque
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/Jille/raft-grpc-leader-rpc/rafterrors"
	"github.com/hashicorp/raft"
	pb "github.com/thylong/bouine/pkg/backend/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SetupgRPCConn setups a gRPC connection listening on given address.
func SetupgRPCConn(RaftAddress string) (net.Listener, error) {
	_, _, err := net.SplitHostPort(RaftAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to parse local address (%q): %v", RaftAddress, err)
	}
	return net.Listen("tcp", RaftAddress)
}

// RPCInterface holds cache server and Raft reference.
// This is useful to forward cache write attempts to the leader.
type RPCInterface struct {
	pb.UnimplementedCacheServer
	Raft *raft.Raft
}

// AddCacheEntry writes concurrently the entry in local cache and apply in raft.
// note: only leaders will be able to process this call
func (r RPCInterface) AddCacheEntry(ctx context.Context, req *pb.AddCacheEntryRequest) (*pb.AddCacheEntryResponse, error) {
	if len(req.GetCacheKey()) < 1 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid cache key %v", req.GetCacheKey())
	}
	if len(req.GetCacheEntry()) < 1 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid cache entry %v", req.GetCacheEntry())
	}
	exp, _ := time.ParseDuration(fmt.Sprintf("%ds", req.GetCacheExpiration()))

	jsonMsg := struct {
		Key string        `json:"key"`
		Val []byte        `json:"val"`
		Exp time.Duration `json:"exp"`
	}{
		Key: req.GetCacheKey(), Val: req.GetCacheEntry(), Exp: exp,
	}
	b, err := json.Marshal(jsonMsg)
	if err != nil {
		return nil, rafterrors.MarkUnretriable(err)
	}

	f := r.Raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		return nil, rafterrors.MarkRetriable(err)
	}

	return &pb.AddCacheEntryResponse{
		CommitIndex: f.Index(),
	}, nil
}

// GetCacheEntry get unique cache entry if exists otherwise return a zero-value.
func (r RPCInterface) GetCacheEntry(ctx context.Context, req *pb.GetCacheEntryRequest) (*pb.GetCacheEntryResponse, error) {
	return &pb.GetCacheEntryResponse{
		CacheEntry: "",
	}, nil
}
