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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/hashicorp/raft"
	pb "github.com/thylong/bouine/pkg/backend/proto"
)

// RaftedRistretto is the FSM implemented in Bouine to make use of the replicated logs.
type RaftedRistretto struct {
	RistrettoCache *ristretto.Cache
}

// This variable declaration verifies interface compliance at build time.
var _ raft.FSM = &RaftedRistretto{}

// ErrForwardToLeaderAsLeader is returned when trying to send a write call to the leader of the quorum as the leader.
var ErrForwardToLeaderAsLeader = errors.New("cannot forward to leader as leader")

// Apply is called once a log entry is committed by a majority of the cluster.
// The returned value is returned to the client as the ApplyFuture.Response.
func (rr *RaftedRistretto) Apply(l *raft.Log) interface{} {
	var cacheEntry pb.AddCacheEntryRequest
	err := json.Unmarshal(l.Data, &cacheEntry)
	if err != nil {
		return nil
	}
	if len(cacheEntry.CacheKey) <= 0 || len(cacheEntry.CacheEntry) <= 0 {
		return nil
	}

	exp, err := time.ParseDuration(fmt.Sprintf("%ds", cacheEntry.CacheExpiration))
	if err != nil {
		return nil
	}
	saved := rr.RistrettoCache.SetWithTTL(cacheEntry.CacheKey, cacheEntry.CacheEntry, 1, exp)
	if !saved {
		return nil
	}
	return nil
}

// Snapshot is not implemented, as Ristretto is an in-memory cache.
func (rr *RaftedRistretto) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

// Restore is not implemented, as Ristretto is an in-memory cache.
func (rr *RaftedRistretto) Restore(snapshot io.ReadCloser) error {
	return nil
}
