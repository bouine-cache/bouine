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
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/gofiber/utils"
	"github.com/hashicorp/raft"
	"github.com/outcaste-io/badger/v3"
	pb "github.com/thylong/bouine/pkg/backend/proto"
	"go.uber.org/zap"
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
	rr.Logger.Debug("new Apply", zap.String("component", "raft"), zap.Any("log", l.Data))
	var cacheEntry pb.AddCacheEntryRequest
	err := json.Unmarshal(l.Data, &cacheEntry)
	if err != nil {
		rr.Logger.Debug("Apply err", zap.String("component", "raft"), zap.Error(err))
		return nil
	}
	if len(cacheEntry.CacheKey) <= 0 || len(cacheEntry.CacheEntry) <= 0 {
		rr.Logger.Debug("Apply err", zap.String("component", "raft"), zap.NamedError("empty cache key/entry", err))
		return nil
	}

	exp, err := time.ParseDuration(fmt.Sprintf("%ds", cacheEntry.CacheExpiration))
	if err != nil {
		rr.Logger.Debug("Apply err", zap.String("component", "raft"), zap.NamedError("empty cache key/entry", err))
		return nil
	}

	rr.Logger.Debug("FSM.Set", zap.String("component", "raft"), zap.String("BadgerKV", fmt.Sprintf("%#v", rr.BadgerKV)))

	entry := badger.NewEntry(utils.UnsafeBytes(cacheEntry.CacheKey), []byte(cacheEntry.CacheEntry))
	if exp != 0 {
		entry.WithTTL(exp)
	}
	// err = rr.BadgerKV.Update(func(tx *badger.Txn) error {
	// 	return tx.SetEntry(entry)
	// })
	if err != nil {
		rr.Logger.Debug("Apply err", zap.String("component", "raft"), zap.String("error", "Badger not saved !"))
		return nil
	}
	return nil
}

// Snapshot is not implemented, as Badger is an in-memory cache.
func (rr *RaftedBadger) Snapshot() (raft.FSMSnapshot, error) {
	rr.Logger.Debug("new Snapshot", zap.String("component", "raft"))
	return nil, nil
}

// Restore is not implemented, as Badger is an in-memory cache.
func (rr *RaftedBadger) Restore(snapshot io.ReadCloser) error {
	rr.Logger.Debug("new Restore", zap.String("component", "raft"))
	return nil
}
