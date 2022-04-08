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
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/outcaste-io/badger/v3"
	pb "github.com/thylong/bouine/pkg/serializer/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// RaftedBadger is the FSM implemented in Bouine to make use of the replicated logs.
type RaftedBadger struct {
	BadgerKV *badger.DB
	Logger   *zap.Logger
	mutex    sync.Mutex
}

// This variable declaration verifies interface compliance at build time.
var _ raft.FSM = &RaftedBadger{}

// fsmSnapshot is used by Raft library to save a point-in-time snapshot of the FSM
// https://godoc.org/github.com/hashicorp/raft#FSMSnapshot
type fsmSnapshot struct {
	badgerKV *badger.DB
}

// Apply is called once a log entry is committed by a majority of the cluster.
// The returned value is returned to the client as the ApplyFuture.Response.
func (rr *RaftedBadger) Apply(l *raft.Log) interface{} {
	rr.Logger.Debug("new Apply", zap.String("component", "raft"))

	var anything anypb.Any
	err := proto.Unmarshal(l.Data, &anything)
	if err != nil {
		rr.Logger.Debug("Apply err", zap.String("component", "raft"), zap.Error(fmt.Errorf("anypb.New error: %s", err)))
		return nil
	}

	msg, err := anything.UnmarshalNew()
	if err != nil {
		rr.Logger.Debug("Apply err", zap.String("component", "raft"), zap.Error(fmt.Errorf("proto.UnmarshalNew error: %s", err)))
		return nil
	}

	switch msgType := msg.(type) {
	case *pb.AddCacheEntryRequest:
		return rr.applyCacheEntryRequest(msg.(*pb.AddCacheEntryRequest))
	case *pb.PurgeCacheRequest:
		return rr.applyPurgeCacheRequest()
	default:
		fmt.Printf("%v\n", msgType)
		return nil
	}
}

// Snapshot is not implemented yet.
func (rr *RaftedBadger) Snapshot() (raft.FSMSnapshot, error) {
	rr.Logger.Debug("new Snapshot", zap.String("component", "raft"))
	rr.mutex.Lock()
	defer rr.mutex.Unlock()
	return &fsmSnapshot{badgerKV: rr.BadgerKV}, nil
}

// Restore is not implemented yet.
func (rr *RaftedBadger) Restore(snapshot io.ReadCloser) error {
	rr.Logger.Debug("new Restore", zap.String("component", "raft"))
	return rr.BadgerKV.Load(snapshot, 128)
}

// Persist dumps state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
// https://godoc.org/github.com/hashicorp/raft#FSMSnapshot
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := f.badgerKV.Backup(sink, 0)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	err = sink.Close()
	if err != nil {
		_ = sink.Cancel()
		return err
	}

	return nil
}

// Release is invoked when the Raft library is finished with the snapshot.
func (f *fsmSnapshot) Release() {
	log.Println("release Snapshot")
}
