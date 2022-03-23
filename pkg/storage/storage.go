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

// Package storage provides a custom storage driver that implements the Storage interface.
// It combines the Ristretto premade driver with the Hashicorp Raft library to offer a distributed Ristretto K/V storage for cache entries.
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	"github.com/dgraph-io/ristretto"
	"github.com/hashicorp/raft"
	"github.com/thylong/bouine/pkg/backend"
	pb "github.com/thylong/bouine/pkg/backend/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Config defines the config for storage.
type Config struct {
	// NumCounters number of keys to track frequency of (10M).
	NumCounters int64
	// MaxCost maximum cost of cache (1GB).
	MaxCost int64
	// BufferItems number of keys per Get buffer.
	BufferItems int64
	DefaultCost int64
	// raftID ID of the present raft node
	RaftID string
	// RaftDir path to raft persistence directory
	RaftDir       string
	RaftBootstrap bool
	RaftAddress   string
	Logger        zap.Logger
}

var ConfigDefault = Config{
	NumCounters: 1e7,
	MaxCost:     1 << 30,
	BufferItems: 64,
	DefaultCost: 1,
}

// ErrForwardToLeaderAsLeader is returned when trying to send a write call to the leader of the quorum as the leader.
var ErrForwardToLeaderAsLeader = errors.New("cannot forward to leader as leader")

// ErrEstablishingConnToLeader is returned when a gRPC connection to the leader cannot be established.
var ErrEstablishingConnToLeader = errors.New("cannot forward to leader as leader")

func configDefault(config ...Config) Config {
	if len(config) < 1 {
		return ConfigDefault
	}
	cfg := config[0]

	if cfg.NumCounters < 1 {
		cfg.NumCounters = ConfigDefault.NumCounters
	}

	if cfg.MaxCost < 1 {
		cfg.MaxCost = ConfigDefault.MaxCost
	}

	if cfg.BufferItems < 1 {
		cfg.BufferItems = ConfigDefault.BufferItems
	}

	if cfg.DefaultCost == 0 {
		cfg.DefaultCost = ConfigDefault.DefaultCost
	}

	return cfg
}

// Storage interface that is implemented by storage providers.
type Storage struct {
	cache       *ristretto.Cache
	defaultCost int64
	logger      zap.Logger
	r           *raft.Raft
	s           *grpc.Server
}

// New creates a new storage.
func New(config ...Config) *Storage {
	cfg := configDefault(config...)

	// setup local Ristretto cache
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: cfg.NumCounters,
		MaxCost:     cfg.MaxCost,
		BufferItems: cfg.BufferItems,
	})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	// start raft
	fsm := &backend.RaftedRistretto{RistrettoCache: cache, Logger: cfg.Logger}
	r, tm, err := backend.NewRaft(
		ctx, cfg.RaftDir, cfg.RaftID, cfg.RaftAddress, cfg.RaftBootstrap, fsm,
	)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
	}

	// register gRPC interface
	s := grpc.NewServer()
	pb.RegisterCacheServer(s, &backend.RPCInterface{Raft: r})
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"bouine"})
	raftadmin.Register(s, r)
	reflection.Register(s)

	return &Storage{
		cache:       cache,
		defaultCost: cfg.DefaultCost,
		logger:      cfg.Logger,
		r:           r,
		s:           s,
	}
}

// ListengRPCServer listen over TCP connection for gRPC requests.
func (s *Storage) ListengRPCServer(raftAddress string) error {
	// setup TCP conn for gRPC
	sock, err := backend.SetupgRPCConn(raftAddress)
	if err != nil {
		return fmt.Errorf("failed to listen: %s", err)
	}
	if err := s.s.Serve(sock); err != nil {
		return fmt.Errorf("failed to start gRPC server: %s", err)
	}
	return nil
}

// Get gets the value for the given key.
// `nil, nil` is returned when the key does not exist.
func (s *Storage) Get(key string) ([]byte, error) {
	if len(key) <= 0 {
		return nil, nil
	}

	item, found := s.cache.Get(key)
	if !found {
		return nil, nil
	}

	buf, asserted := item.([]byte)
	if !asserted {
		return nil, nil
	}

	return buf, nil
}

func (s *Storage) Set(key string, val []byte, exp time.Duration) error {
	err := s.forwardToLeader(key, val, exp)
	if (err != nil) && err != ErrForwardToLeaderAsLeader {
		s.logger.Info("Set err",
			zap.String("component", "storage"), zap.String("key", key), zap.String("error_msg", err.Error()),
		)
		// Opportunistic local cache copy
		if len(key) <= 0 || len(val) <= 0 {
			return nil
		}
		saved := s.cache.SetWithTTL(key, val, s.defaultCost, exp)
		if !saved {
			return nil
		}
		return fmt.Errorf("cannot forward to leader: %s", err)
	}

	s.cache.SetWithTTL(key, val, s.defaultCost, exp)

	s.logger.Debug("new storage.Set",
		zap.String("component", "storage"),
		zap.String("key", key),
		zap.Duration("exp", exp),
	)

	// TODO: extract into separated method
	// (duplicated code with func (r RPCInterface) AddCacheEntry)
	req := &pb.AddCacheEntryRequest{CacheKey: "foo", CacheEntry: "bar", CacheExpiration: uint64(exp.Seconds())}
	if len(req.GetCacheKey()) < 1 {
		s.logger.Debug("storage.Set err: invalid cache key",
			zap.String("component", "storage"), zap.String("key", key),
		)
		return fmt.Errorf("invalid cache key %v", req.GetCacheKey())
	}
	if len(req.GetCacheEntry()) < 1 {
		s.logger.Debug("storage.Set err: invalid cache entry",
			zap.String("component", "storage"), zap.String("key", key),
		)
		return fmt.Errorf("invalid cache entry %v", req.GetCacheEntry())
	}
	if _, err := time.ParseDuration(fmt.Sprintf("%ds", req.GetCacheExpiration())); err != nil {
		s.logger.Debug("storage.Set err",
			zap.String("component", "storage"), zap.NamedError("invalid cache expiration", err), zap.String("key", key), zap.Uint64("exp", req.GetCacheExpiration()),
		)
		return fmt.Errorf("invalid cache expiration %v", req.GetCacheExpiration())
	}
	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("cannot Marshal cache entry: %s", err)
	}
	s.r.Apply(b, time.Second)

	return nil
}

func (s *Storage) Delete(key string) error {
	// TODO: handle BAN (both raft leaders and followers) here

	if len(key) <= 0 {
		return nil
	}
	s.cache.Del(key)
	return nil
}

func (s *Storage) Reset() error {
	// TODO: handle PURGE (both raft leaders and followers) here

	s.cache.Clear()
	return nil
}

func (s *Storage) Close() error {
	s.cache.Close()
	return nil
}

// forwardToLeader forwards request to leader and return leader response.
// TODO: migrate to persistent connection.
func (s *Storage) forwardToLeader(key string, val []byte, exp time.Duration) error {
	if s.IsLeader() {
		return ErrForwardToLeaderAsLeader
	}

	s.logger.Debug("new forwardToLeader",
		zap.String("component", "raft"), zap.Any("current leader", s.r.Leader()), zap.String("raft_key", key),
	)

	// FIXME: nil pointer dereference (when never joined a quorum)
	conn, err := grpc.Dial(string(s.r.Leader()), grpc.WithInsecure())
	if err != nil {
		s.logger.Debug("forwardToLeader err",
			zap.String("component", "raft"), zap.String("raft_key", key), zap.String("error_msg", err.Error()),
		)
		return ErrEstablishingConnToLeader
	}
	defer conn.Close()

	client := pb.NewCacheClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.AddCacheEntry(ctx, &pb.AddCacheEntryRequest{CacheKey: key, CacheEntry: string(val), CacheExpiration: uint64(exp.Seconds())}, nil)
	if err != nil {
		s.logger.Debug("forwardToLeader err",
			zap.String("component", "raft"), zap.String("raft_key", key), zap.String("error_msg", err.Error()),
		)
		return fmt.Errorf("failed to forward to the leader: %s", err)
	}

	return nil
}

// IsLeader returns true if the current node is the cluster leader.
func (s *Storage) IsLeader() bool {
	s.logger.Debug("new IsLeader",
		zap.String("component", "raft"), zap.String("raft-role", s.r.State().String()),
	)
	return strings.Compare(s.r.State().String(), "Leader") == 0
}
