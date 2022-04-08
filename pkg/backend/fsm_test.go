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

func TestRaft_SnapshotRestore(t *testing.T) {
	BoltdbFilesCleanup("/tmp/bouine/fsm")
	BadgerFilesCleanup("/tmp/bouine/fsm")
	defer BoltdbFilesCleanup("/tmp/bouine/fsm")
	defer BadgerFilesCleanup("/tmp/bouine/fsm")

	// Open and reset database
	db, err := badger.Open(badger.DefaultOptions("/tmp/bouine/fsm"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DropAll(); err != nil {
		t.Fatal(err)
	}
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
	for i := 0; i < 20; i++ {
		entry := &pb.AddCacheEntryRequest{
			CacheKey:        fmt.Sprintf("key%d", i),
			CacheEntry:      bytes.NewBufferString(fmt.Sprintf("val%d", i)).Bytes(),
			CacheExpiration: durationpb.New(10 * time.Second),
		}
		any, _ := anypb.New(entry)

		msg, err := proto.Marshal(any)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		future = r.Apply(msg, 0)
	}
	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Take a snapshot
	snapFuture := r.Snapshot()
	if err := snapFuture.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Shutdown
	shutdown := r.Shutdown()
	if err := shutdown.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Restart the Raft
	// _, _, err = NewRaft(
	// 	RaftConfig{RaftDir: "/tmp/bouine/fsm_restore", RaftNodeID: "1", RaftBootstrap: false, HostAddress: "localhost:4597", FSM: &RaftedBadger{BadgerKV: db, Logger: zap.NewExample()}},
	// )
	// if err != nil {
	// 	t.Fatalf("err: %v", err)
	// }
	// We should have restored from the snapshot!
	// if last := r.getLastApplied(); last != snap.Index {
	// 	t.Fatalf("bad last index: %d, expecting %d", last, snap.Index)
	// }
}
