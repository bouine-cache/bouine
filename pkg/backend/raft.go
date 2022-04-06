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
	"os"
	"path/filepath"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
)

// RaftConfig holds the configuration to create a Raft cluster.
type RaftConfig struct {
	RaftDir            string
	RaftNodeID         string
	HostAddress        string
	RaftBootstrap      bool
	FSM                raft.FSM
	LogLevel           string
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	SnapshotInterval   time.Duration
}

func overrideDefaultConfig(raftConfig *raft.Config, customConfig *RaftConfig) {
	raftConfig.LocalID = raft.ServerID(customConfig.RaftNodeID)

	if len(customConfig.LogLevel) > 0 {
		raftConfig.LogLevel = customConfig.LogLevel
	}

	if customConfig.HeartbeatTimeout.Seconds() > 0 {
		raftConfig.HeartbeatTimeout = customConfig.HeartbeatTimeout
	}

	if customConfig.ElectionTimeout.Seconds() > 0 {
		raftConfig.ElectionTimeout = customConfig.ElectionTimeout
	}

	if customConfig.LeaderLeaseTimeout.Seconds() > 0 {
		raftConfig.LeaderLeaseTimeout = customConfig.LeaderLeaseTimeout
	}

	if customConfig.SnapshotInterval.Seconds() > 0 {
		raftConfig.SnapshotInterval = customConfig.SnapshotInterval
	}
}

// NewRaft instantiate a new Raft node based on provided parameters.
// This will create local boltdb log files, create a gRPC service & a Raft transport.
func NewRaft(config RaftConfig) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	overrideDefaultConfig(c, &config)

	ldb, err := boltdb.NewBoltStore(filepath.Join(config.RaftDir, "logs.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(config.RaftDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(config.RaftDir, "stable.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(config.RaftDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(config.RaftDir, 3, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf(`cannot create snapshot store (%q, ...): %v`, config.RaftDir, err)
	}

	tm := transport.New(raft.ServerAddress(config.HostAddress), []grpc.DialOption{grpc.WithInsecure()})

	r, err := raft.NewRaft(c, config.FSM, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create new raft: %v", err)
	}

	if config.RaftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(config.RaftNodeID),
					Address:  raft.ServerAddress(config.HostAddress),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, nil, fmt.Errorf("cannot bootstrap cluster: %v", err)
		}
	}

	return r, tm, nil
}

// BoltdbFilesCleanup deletes logs/stable/snapshots relicates.
//
// NOTE: This is exposed for testing purposes and is not a stable API.
func BoltdbFilesCleanup(raftDir string) {
	for _, path := range []string{
		filepath.Join(raftDir, "logs.dat"),
		filepath.Join(raftDir, "stable.dat"),
		filepath.Join(raftDir, "snapshots"),
	} {
		logFile, err := filepath.Abs(path)
		if err != nil {
			// return errors.New("cannot cleanup /tmp directory from test files")
			return
		}
		os.Remove(logFile)
	}
}

// BadgerFilesCleanup deletes cache store.
//
// NOTE: This is exposed for testing purposes and is not a stable API.
func BadgerFilesCleanup(raftDir string) {
	for _, path := range []string{
		filepath.Join(raftDir, "cache_store"),
	} {
		logFile, err := filepath.Abs(path)
		if err != nil {
			// return errors.New("cannot cleanup /tmp directory from test files")
			return
		}
		os.Remove(logFile)
	}
}
