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
	"log"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/thylong/bouine/pkg/backend/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func dialer(listener *bufconn.Listener, r *raft.Raft) func(context.Context, string) (net.Conn, error) {
	server := grpc.NewServer()
	pb.RegisterCacheServer(server, &RPCInterface{Raft: r})

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}

func TestRPCInterface_AddCacheEntry(t *testing.T) {
	raftDir := "/tmp/"
	BoltdbFilesCleanup(raftDir)

	// NewRaft() with bootstrap
	r, _, err := NewRaft(context.Background(), "/tmp/", "1", "localhost:4766", true, &RaftedRistretto{})
	if err != nil {
		t.Errorf("NewRaft() unexpected error = %v", err)
		return
	}

	time.Sleep(2 * time.Second)

	ctx := context.Background()
	listener := bufconn.Listen(1024 * 1024)
	conn, err := grpc.DialContext(ctx, "", grpc.WithInsecure(), grpc.WithContextDialer(dialer(listener, r)))
	if err != nil {
		log.Fatal(err)
	}
	// tm.Register(s)
	defer conn.Close()

	client := pb.NewCacheClient(conn)

	tests := []struct {
		name            string
		cacheEntry      *pb.AddCacheEntryRequest
		wantCommitIndex uint64
		wantErr         bool
	}{
		{name: "invalid request with empty cache param", cacheEntry: &pb.AddCacheEntryRequest{CacheKey: "", CacheEntry: ""}, wantErr: true, wantCommitIndex: 0},
		{name: "invalid request with empty cache key", cacheEntry: &pb.AddCacheEntryRequest{CacheKey: "", CacheEntry: "bar"}, wantErr: true, wantCommitIndex: 0},
		{name: "invalid request with empty cache entry", cacheEntry: &pb.AddCacheEntryRequest{CacheKey: "foo", CacheEntry: ""}, wantErr: true, wantCommitIndex: 0},
		{name: "valid request", cacheEntry: &pb.AddCacheEntryRequest{CacheKey: "foo", CacheEntry: "bar"}, wantErr: false, wantCommitIndex: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := client.AddCacheEntry(ctx, tt.cacheEntry)
			if (err != nil) != tt.wantErr {
				t.Errorf("AddCacheEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if commitIndex := res.GetCommitIndex(); commitIndex != tt.wantCommitIndex {
				t.Errorf("AddCacheEntry() commitIndex = %v, wantCommitIndex %v", commitIndex, tt.wantCommitIndex)
				return
			}
		})
	}
}
