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

	"github.com/gofiber/utils"
	"github.com/outcaste-io/badger/v3"
	pb "github.com/thylong/bouine/pkg/serializer/proto"
	"go.uber.org/zap"
)

// applyCacheEntryRequest applies AddCacheEntryRequest on FSM.
func (rr *RaftedBadger) applyCacheEntry(req *pb.AddCacheEntryRequest) error {
	rr.Logger.Debug("FSM.ApplyCacheEntryRequest", zap.String("component", "raft"),
		zap.String("key", req.GetCacheKey()),
		zap.ByteString("val", req.GetCacheEntry()),
		zap.String("CacheExp", req.GetCacheExpiration().AsDuration().String()),
	)

	entry := badger.NewEntry(utils.UnsafeBytes(req.GetCacheKey()), req.GetCacheEntry())
	if req.GetCacheExpiration().GetSeconds() != 0 {
		entry.WithTTL(req.GetCacheExpiration().AsDuration())
	}
	err := rr.BadgerKV.Update(func(tx *badger.Txn) error {
		return tx.SetEntry(entry)
	})
	if err != nil {
		rr.Logger.Debug("applyCacheEntry err", zap.String("component", "raft"), zap.Error(fmt.Errorf("fail to apply on FSM: %s", err)))
		return err
	}
	// msg.(*pb.AddCacheEntryRequest).Get
	return nil
}

// applyCacheEntryRequest applies PurgeCacheRequest on FSM.
func (rr *RaftedBadger) applyPurgeCache() error {
	rr.Logger.Debug("FSM.applyPurgeCache", zap.String("component", "raft"))
	// TODO: Prevent Purge to threaten quorum stability
	err := rr.BadgerKV.DropAll()
	if err != nil {
		rr.Logger.Error("applyPurgeCache err", zap.String("component", "raft"), zap.Error(fmt.Errorf("fail to apply on FSM: %s", err)))
	}
	return err
}

// applyInvalidateCacheEntry applies PurgeCacheRequest on FSM.
func (rr *RaftedBadger) applyInvalidateCacheEntry(req *pb.InvalidateCacheEntryRequest) error {
	rr.Logger.Debug("FSM.applyInvalidateCacheEntry", zap.String("component", "raft"),
		zap.ByteString("key", req.GetCacheKey()),
	)

	// req.GetCacheKey() can be either a prefix or a
	err := rr.BadgerKV.DropPrefixBlocking(req.GetCacheKey())
	if err != nil {
		rr.Logger.Error("applyInvalidateCacheEntry err", zap.String("component", "raft"), zap.Error(fmt.Errorf("fail to apply on FSM: %s", err)))
	}
	return err
}
