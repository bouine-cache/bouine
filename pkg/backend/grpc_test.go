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
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/outcaste-io/badger/v3"
	pb "github.com/thylong/bouine/pkg/backend/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

var client pb.CacheClient

func TestMain(m *testing.M) {
	raftDir := "/tmp/bouine/grpc"

	// Delete potential orphans from previous test runs
	BoltdbFilesCleanup(raftDir)
	BadgerFilesCleanup(raftDir)
	defer BoltdbFilesCleanup(raftDir)
	defer BadgerFilesCleanup(raftDir)

	// Open and reset database
	db, err := badger.Open(badger.DefaultOptions(raftDir))
	if err != nil {
		panic(err)
	}
	if err := db.DropAll(); err != nil {
		panic(err)
	}
	defer db.Close()

	// NewRaft() with bootstrap
	r, _, err := NewRaft(RaftConfig{RaftDir: raftDir, RaftNodeID: "1", HostAddress: "localhost:4769", RaftBootstrap: true, FSM: &RaftedBadger{BadgerKV: db, Logger: zap.NewExample()}})
	if err != nil {
		panic(err)
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

	client = pb.NewCacheClient(conn)

	exitVal := m.Run()

	os.Exit(exitVal)
}

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
	ctx := context.Background()

	tests := []struct {
		name            string
		cacheEntry      *pb.AddCacheEntryRequest
		wantCommitIndex uint64
		wantErr         bool
	}{
		{name: "invalid request with empty cache param", cacheEntry: &pb.AddCacheEntryRequest{CacheKey: "", CacheEntry: []byte("")}, wantErr: true, wantCommitIndex: 0},
		{name: "invalid request with empty cache key", cacheEntry: &pb.AddCacheEntryRequest{CacheKey: "", CacheEntry: []byte("bar")}, wantErr: true, wantCommitIndex: 0},
		{name: "invalid request with empty cache entry", cacheEntry: &pb.AddCacheEntryRequest{CacheKey: "foo", CacheEntry: []byte("")}, wantErr: true, wantCommitIndex: 0},
		{name: "valid request with Exp", cacheEntry: &pb.AddCacheEntryRequest{CacheKey: "foo", CacheEntry: []byte("bar"), CacheExpiration: 10}, wantErr: false, wantCommitIndex: 3},
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
