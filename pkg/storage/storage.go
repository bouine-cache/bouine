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
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/hashicorp/raft"
	"github.com/thylong/bouine/pkg/backend"
	pb "github.com/thylong/bouine/pkg/backend/proto"
	"google.golang.org/grpc"
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
}

var ConfigDefault = Config{
	NumCounters: 1e7,
	MaxCost:     1 << 30,
	BufferItems: 64,
	DefaultCost: 1,
}

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
	fsm := &backend.RaftedRistretto{RistrettoCache: cache}
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

	return &Storage{
		cache:       cache,
		defaultCost: cfg.DefaultCost,
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
	err := backend.ForwardToLeader(backend.Config{
		Leader:  "",
		ID:      "",
		Address: "",
	})
	if (err != nil) && err != backend.ErrForwardToLeaderAsLeader {
		return fmt.Errorf("cannot forward to leader: %s", err)
	}

	if len(key) <= 0 || len(val) <= 0 {
		return nil
	}
	saved := s.cache.SetWithTTL(key, val, s.defaultCost, exp)
	if !saved {
		return nil
	}
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
