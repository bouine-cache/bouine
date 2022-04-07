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
// It combines the Badger premade driver with the Hashicorp Raft library to offer a distributed Badger K/V storage for cache entries.
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
	"github.com/gofiber/utils"
	"github.com/hashicorp/raft"
	"github.com/outcaste-io/badger/v3"
	"github.com/thylong/bouine/pkg/backend"
	"github.com/thylong/bouine/pkg/serializer"
	pb "github.com/thylong/bouine/pkg/serializer/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Storage interface that is implemented by storage providers.
type Storage struct {
	gcInterval time.Duration
	done       chan struct{}
	fsm        *backend.RaftedBadger
	logger     *zap.Logger
	r          *raft.Raft
	s          *grpc.Server
}

// Config defines the config for storage.
type Config struct {
	// Database name
	// Optional. Default is "./fiber.badger"
	Database string

	// Reset clears any existing keys in existing Table
	// Optional. Default is false
	Reset bool

	// Time before deleting expired keys
	// Optional. Default is 10 * time.Second
	GCInterval time.Duration

	RaftHeartbeatTimeout   time.Duration
	RaftElectionTimeout    time.Duration
	RaftCommitTimeout      time.Duration
	RaftSnapshotInterval   time.Duration
	RaftLeaderLeaseTimeout time.Duration

	// BadgerOptions is a way to set options in badger
	// Optional. Default is badger.DefaultOptions("./fiber.badger")
	BadgerOptions badger.Options

	// UseLogger define if any logger will be used
	// Optional. Default is false
	UseLogger bool
	// raftID ID of the present raft node
	RaftID string
	// RaftDir path to raft persistence directory
	RaftDir       string
	RaftBootstrap bool
	RaftAddress   string
	Logger        *zap.Logger
}

const defaultDatabase = "./fiber.badger"

var ConfigDefault = Config{
	Database:      defaultDatabase,
	Reset:         false,
	GCInterval:    10 * time.Second,
	BadgerOptions: badger.DefaultOptions(defaultDatabase).WithLogger(nil),
	Logger:        zap.NewExample(),
	UseLogger:     false,
}

// ErrEmptyKey is returned when trying to get/set on FSM with an empty key.
var ErrEmptyKey = errors.New("invalid empty key parameter")

// ErrEmptyVal is returned when trying to get/set on FSM with an empty value.
var ErrEmptyVal = errors.New("invalid empty value parameter")

// ErrKeyNotFound is returned when a key is not found in the K/V store.
var ErrKeyNotFound = errors.New("key not found")

// ErrEstablishingConnToLeader is returned when a gRPC connection to the leader cannot be established.
var ErrEstablishingConnToLeader = errors.New("cannot forward to leader as leader")

func configDefault(config ...Config) Config {
	// Return default config if nothing provided
	if len(config) < 1 {
		return ConfigDefault
	}

	// Override default config
	cfg := config[0]

	cfg.Logger = zap.NewExample()

	// Set default values
	if cfg.Database == "" {
		cfg.Database = ConfigDefault.Database
	}
	if int(cfg.GCInterval.Seconds()) <= 0 {
		cfg.GCInterval = ConfigDefault.GCInterval
	}
	// Detecting if no default Badger option was given
	// Also detects when a default badger option is given with a custom database name
	if cfg.BadgerOptions.ValueLogFileSize <= 0 || cfg.BadgerOptions.Dir == "" || cfg.BadgerOptions.ValueDir == "" ||
		(cfg.BadgerOptions.Dir == defaultDatabase && cfg.BadgerOptions.Dir != cfg.Database) {
		cfg.BadgerOptions = badger.DefaultOptions(cfg.Database)
	}

	return cfg
}

// New creates a new storage.
func New(config ...Config) *Storage {
	cfg := configDefault(config...)

	// Open database
	db, err := badger.Open(badger.DefaultOptions(cfg.RaftDir))
	if err != nil {
		panic(err)
	}

	if cfg.Reset {
		if err := db.DropAll(); err != nil {
			panic(err)
		}
	}

	// start raft
	cfg.Logger.Debug("Start raft", zap.String("component", "storage"),
		zap.String("RaftDir", cfg.RaftDir), zap.String("RaftNodeID", cfg.RaftID), zap.String("HostAddress", cfg.RaftAddress), zap.Bool("RaftBootstrap", cfg.RaftBootstrap),
	)
	fsm := &backend.RaftedBadger{BadgerKV: db, Logger: cfg.Logger}
	r, tm, err := backend.NewRaft(
		backend.RaftConfig{RaftDir: cfg.RaftDir, RaftNodeID: cfg.RaftID, HostAddress: cfg.RaftAddress, RaftBootstrap: cfg.RaftBootstrap, FSM: fsm},
	)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
	}

	// register gRPC interface
	s := grpc.NewServer()
	pb.RegisterCacheServer(s, &serializer.RPCInterface{Raft: r})
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"bouine"})
	raftadmin.Register(s, r)
	reflection.Register(s)

	store := &Storage{
		fsm:        fsm,
		logger:     cfg.Logger,
		gcInterval: cfg.GCInterval,
		r:          r,
		s:          s,
	}

	// Start garbage collector
	go store.gc()

	return store
}

// ListengRPCServer listen over TCP connection for gRPC requests.
func (s *Storage) ListengRPCServer(raftAddress string) error {
	// setup TCP conn for gRPC
	sock, err := serializer.SetupgRPCConn(raftAddress)
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
	s.logger.Debug("new storage.Get", zap.String("component", "storage"),
		zap.String("key", key),
	)

	if len(key) <= 0 {
		s.logger.Debug("storage.Get err:",
			zap.String("component", "storage"),
			zap.Error(ErrEmptyKey),
		)
		return nil, ErrEmptyKey
	}
	var data []byte

	err := s.fsm.BadgerKV.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		// item.Value() is only valid within the transaction.
		// We can either copy it ourselves or use the ValueCopy() method.
		// TODO: Benchmark if it's faster to copy + close tx,
		// or to keep the tx open until unmarshalling is done.
		data, err = item.ValueCopy(nil)
		return err
	})
	// If no value was found return false
	if err == badger.ErrKeyNotFound {
		s.logger.Debug("storage.Get err:",
			zap.String("component", "storage"),
			zap.String("key", key),
			zap.Error(ErrKeyNotFound),
		)
		return nil, ErrKeyNotFound
	}

	s.logger.Debug("storage.Get result",
		zap.String("component", "storage"),
		zap.ByteString("data", data),
		zap.Error(err),
	)
	return data, err
}

func (s *Storage) Set(key string, val []byte, exp time.Duration) error {
	s.logger.Debug("new storage.Set", zap.String("component", "storage"),
		zap.String("key", key),
		zap.ByteString("val", val),
		zap.Duration("exp", exp),
	)
	// Ain't Nobody Got Time For That
	if len(key) <= 0 {
		s.logger.Debug("storage.Set err:", zap.String("component", "storage"),
			zap.Error(ErrEmptyKey),
		)
		return ErrEmptyKey
	}
	if len(val) <= 0 {
		s.logger.Debug("storage.Set err:", zap.String("component", "storage"),
			zap.Error(ErrEmptyVal),
		)
		return ErrEmptyVal
	}

	if s.IsLeader() {
		req := struct {
			Key string        `json:"key"`
			Val []byte        `json:"val"`
			Exp time.Duration `json:"exp"`
		}{Key: key, Val: val, Exp: exp}

		b, err := json.Marshal(req)
		if err != nil {
			return fmt.Errorf("cannot Marshal cache entry: %s", err)
		}
		s.r.Apply(b, time.Second)
		return nil
	}

	err := s.forwardToLeader(key, val, exp)
	// Opportunistic local cache copy
	// note: This increases drastically the resiliency of the raft cluster
	if err != nil {
		s.logger.Info("Set err", zap.String("component", "storage"),
			zap.String("key", key), zap.String("error_msg", err.Error()),
		)
		entry := badger.NewEntry(utils.UnsafeBytes(key), val)
		if exp != 0 {
			entry.WithTTL(exp)
		}
		err := s.fsm.BadgerKV.Update(func(tx *badger.Txn) error {
			return tx.SetEntry(entry)
		})
		if err != nil {
			return fmt.Errorf("failed opportunistic write to badger: %s", err)
		}
	}
	return nil
}

func (s *Storage) Delete(key string) error {
	s.logger.Debug("new storage.Delete", zap.String("component", "storage"),
		zap.String("key", key),
	)

	// Ain't Nobody Got Time For That
	if len(key) <= 0 {
		return ErrEmptyKey
	}
	return s.fsm.BadgerKV.Update(func(tx *badger.Txn) error {
		return tx.Delete(utils.UnsafeBytes(key))
	})
}

func (s *Storage) Reset() error {
	s.logger.Info("new storage.Reset", zap.String("component", "storage"))

	// TODO: handle PURGE (both raft leaders and followers) here

	return s.fsm.BadgerKV.DropAll()
}

func (s *Storage) Close() error {
	s.logger.Warn("new storage.Close", zap.String("component", "storage"))

	// Stop gRPC server
	s.s.GracefulStop()

	// Leave Raft quorum
	s.r.Shutdown()

	// FIXME: times out.
	// s.done <- struct{}{}
	return s.fsm.BadgerKV.Close()
}

func (s *Storage) gc() {
	ticker := time.NewTicker(s.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			_ = s.fsm.BadgerKV.RunValueLogGC(0.7)
		}
	}
}

// forwardToLeader forwards request to leader and return leader response.
// TODO: migrate to persistent connection.
func (s *Storage) forwardToLeader(key string, val []byte, exp time.Duration) error {
	s.logger.Debug("new forwardToLeader",
		zap.String("component", "storage"), zap.Any("current leader", s.r.Leader()), zap.String("raft_key", key),
	)

	// s.r.
	conn, err := grpc.Dial(string(s.r.Leader()), grpc.WithInsecure())
	if err != nil {
		s.logger.Debug("forwardToLeader err", zap.String("component", "storage"),
			zap.String("raft_key", key),
			zap.Any("state", s.r.State()),
			zap.Error(fmt.Errorf("failed to connect to leader: %s", err)),
		)
		return ErrEstablishingConnToLeader
	}
	defer conn.Close()

	client := pb.NewCacheClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = client.AddCacheEntry(ctx, &pb.AddCacheEntryRequest{CacheKey: key, CacheEntry: val, CacheExpiration: uint64(exp.Seconds())})
	if err != nil {
		err = fmt.Errorf("failed to forward to the leader: %s", err)
		s.logger.Debug("forwardToLeader err",
			zap.String("component", "storage"), zap.String("raft-role", s.r.State().String()), zap.String("raft_key", key), zap.Error(err),
		)
		return err
	}
	// TODO: check gRPC response (potential retry, etc).

	return nil
}

// IsLeader returns true if the current node is the cluster leader.
func (s *Storage) IsLeader() bool {
	s.logger.Debug("new IsLeader",
		zap.String("component", "storage"), zap.String("raft-role", s.r.State().String()),
	)
	return strings.Compare(s.r.State().String(), "Leader") == 0
}
