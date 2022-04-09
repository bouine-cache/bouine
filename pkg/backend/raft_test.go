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
	"testing"
	"time"

	"github.com/outcaste-io/badger/v3"
	pb "github.com/thylong/bouine/pkg/serializer/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNewRaft(t *testing.T) {
	// Open and reset database
	db, err := badger.Open(badger.DefaultOptions("/tmp/bouine/raft"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DropAll(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		name          string
		raftConfig    RaftConfig
		wantRaftStats map[string]string
		wantErr       bool
	}{
		{name: "missing-config", raftConfig: RaftConfig{}, wantRaftStats: map[string]string{}, wantErr: true},
		{name: "successful-leader-start", raftConfig: RaftConfig{RaftDir: "/tmp/bouine/raft", RaftNodeID: "1", RaftBootstrap: true, HostAddress: "localhost:4596", FSM: &RaftedBadger{BadgerKV: db, Logger: zap.NewExample()}}, wantRaftStats: map[string]string{}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			TmpDircleanup(tt.raftConfig.RaftDir)
			defer TmpDircleanup(tt.raftConfig.RaftDir)
			got, _, err := NewRaft(tt.raftConfig)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("NewRaft() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			time.Sleep(2 * time.Second)

			// Apply should not fail on single node leader
			// note: we don't test the response as a mocked FSM is used.
			entry := &pb.AddCacheEntryRequest{
				CacheKey:        "foo",
				CacheEntry:      []byte("bar"),
				CacheExpiration: durationpb.New(10 * time.Second),
			}
			any, err := anypb.New(entry)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			msg, err := proto.Marshal(any)
			if err != nil {
				t.Fatalf("err: %v", err)
			}

			future := got.Apply(msg, 3*time.Second)
			if err = future.Error(); err != nil {
				t.Errorf("NewRaft() unexpected error = %v", err)
				return
			}
		})
	}
}
