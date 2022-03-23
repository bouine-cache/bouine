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

	"github.com/dgraph-io/ristretto"
	"github.com/hashicorp/raft"
	pb "github.com/thylong/bouine/pkg/backend/proto"
	"go.uber.org/zap"
)

// RaftedRistretto is the FSM implemented in Bouine to make use of the replicated logs.
type RaftedRistretto struct {
	RistrettoCache *ristretto.Cache
	Logger         zap.Logger
}

// This variable declaration verifies interface compliance at build time.
var _ raft.FSM = &RaftedRistretto{}

// Apply is called once a log entry is committed by a majority of the cluster.
// The returned value is returned to the client as the ApplyFuture.Response.
func (rr *RaftedRistretto) Apply(l *raft.Log) interface{} {
	rr.Logger.Debug("new Apply", zap.String("component", "raft"))
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
	saved := rr.RistrettoCache.SetWithTTL(cacheEntry.CacheKey, cacheEntry.CacheEntry, 0, exp)
	if !saved {
		rr.Logger.Debug("Apply err", zap.String("component", "raft"), zap.String("error", "Ristretto not saved !"))
		return nil
	}
	return nil
}

// Snapshot is not implemented, as Ristretto is an in-memory cache.
func (rr *RaftedRistretto) Snapshot() (raft.FSMSnapshot, error) {
	rr.Logger.Debug("new Snapshot", zap.String("component", "raft"))
	return nil, nil
}

// Restore is not implemented, as Ristretto is an in-memory cache.
func (rr *RaftedRistretto) Restore(snapshot io.ReadCloser) error {
	rr.Logger.Debug("new Restore", zap.String("component", "raft"))
	return nil
}
