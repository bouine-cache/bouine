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

	"github.com/gofiber/utils"
	"github.com/hashicorp/raft"
	"github.com/outcaste-io/badger/v3"
	pb "github.com/thylong/bouine/pkg/serializer/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// RaftedBadger is the FSM implemented in Bouine to make use of the replicated logs.
type RaftedBadger struct {
	BadgerKV *badger.DB
	Logger   *zap.Logger
}

// This variable declaration verifies interface compliance at build time.
var _ raft.FSM = &RaftedBadger{}

// Apply is called once a log entry is committed by a majority of the cluster.
// The returned value is returned to the client as the ApplyFuture.Response.
func (rr *RaftedBadger) Apply(l *raft.Log) interface{} {
	rr.Logger.Debug("new Apply", zap.String("component", "raft"))
	var req pb.AddCacheEntryRequest
	err := proto.Unmarshal(l.Data, &req)
	if err != nil {
		rr.Logger.Debug("Apply err", zap.String("component", "raft"), zap.Error(fmt.Errorf("proto.Unmarshal error: %s", err)))
		return nil
	}

	rr.Logger.Debug("FSM.Set", zap.String("component", "raft"),
		zap.String("key", req.GetCacheKey()),
		zap.ByteString("val", req.GetCacheEntry()),
		zap.String("CacheExp", req.GetCacheExpiration().AsDuration().String()),
	)

	entry := badger.NewEntry(utils.UnsafeBytes(req.GetCacheKey()), req.GetCacheEntry())
	if req.GetCacheExpiration().GetSeconds() != 0 {
		entry.WithTTL(req.GetCacheExpiration().AsDuration())
	}
	err = rr.BadgerKV.Update(func(tx *badger.Txn) error {
		return tx.SetEntry(entry)
	})
	if err != nil {
		rr.Logger.Debug("Apply err", zap.String("component", "raft"), zap.Error(fmt.Errorf("fail to apply on FSM: %s", err)))
		return err
	}
	return nil
}

// Snapshot is not implemented yet.
func (rr *RaftedBadger) Snapshot() (raft.FSMSnapshot, error) {
	rr.Logger.Debug("new Snapshot", zap.String("component", "raft"))
	return nil, nil
}

// Restore is not implemented yet.
func (rr *RaftedBadger) Restore(snapshot io.ReadCloser) error {
	rr.Logger.Debug("new Restore", zap.String("component", "raft"))
	return nil
}
