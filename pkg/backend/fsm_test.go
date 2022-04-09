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
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/outcaste-io/badger/v3"
	pb "github.com/thylong/bouine/pkg/serializer/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRaft_SnapshotAndLiveRestore(t *testing.T) {
	TmpDircleanup("/tmp/bouine/fsm")
	defer TmpDircleanup("/tmp/bouine/fsm")

	// Open and reset database
	db, _ := badger.Open(badger.DefaultOptions("/tmp/bouine/fsm"))
	defer db.Close()

	// Make the cluster
	r, _, err := NewRaft(
		RaftConfig{RaftDir: "/tmp/bouine/fsm", RaftNodeID: "1", RaftBootstrap: true, HostAddress: "localhost:4597", FSM: &RaftedBadger{BadgerKV: db, Logger: zap.NewExample()}},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the leader
	time.Sleep(2 * time.Second)

	// Commit a lot of things
	var future raft.Future
	for i := 0; i < 10; i++ {
		entry := &pb.AddCacheEntryRequest{
			CacheKey:        fmt.Sprintf("key%d", i),
			CacheEntry:      bytes.NewBufferString(fmt.Sprintf("val%d", i)).Bytes(),
			CacheExpiration: durationpb.New(10 * time.Second),
		}
		any, _ := anypb.New(entry)
		msg, _ := proto.Marshal(any)
		future = r.Apply(msg, 0)
	}
	// Wait for the last future to apply
	_ = future.Error()

	// Take a snapshot
	snapFuture := r.Snapshot()
	if err := snapFuture.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Commit some more things.
	for i := 0; i < 10; i++ {
		entry := &pb.AddCacheEntryRequest{
			CacheKey:        fmt.Sprintf("key%d", i),
			CacheEntry:      bytes.NewBufferString(fmt.Sprintf("val%d", i)).Bytes(),
			CacheExpiration: durationpb.New(10 * time.Second),
		}
		any, _ := anypb.New(entry)
		msg, _ := proto.Marshal(any)
		future = r.Apply(msg, 0)
	}
	// Wait for the last future to apply.
	_ = future.Error()
	preIndex := r.LastIndex()

	// Restore the snapshot, twiddling the index with the offset.
	meta, reader, err := snapFuture.Open()
	meta.Index += 2
	if err != nil {
		t.Fatalf("Snapshot open failed: %v", err)
	}
	defer reader.Close()
	if err := r.Restore(meta, reader, 5*time.Second); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Make sure the index was updated correctly. We add 2 because we burn
	// an index to create a hole, and then we apply a no-op after the
	// restore.
	var expected uint64
	if meta.Index < preIndex {
		expected = preIndex + 2
	} else {
		expected = meta.Index + 2
	}
	lastIndex := r.LastIndex()
	if lastIndex != expected {
		t.Fatalf("Index was not updated correctly: %d vs. %d", lastIndex, expected)
	}
}
