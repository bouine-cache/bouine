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
	pb "github.com/thylong/bouine/internal/backend/proto"
)

type RaftedRistretto struct {
	RistrettoCache *ristretto.Cache
}

var _ raft.FSM = &RaftedRistretto{}

var ErrForwardToLeaderAsLeader = errors.New("cannot forward to leader as leader")

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

func (rr *RaftedRistretto) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (rr *RaftedRistretto) Restore(snapshot io.ReadCloser) error {
	return nil
}
