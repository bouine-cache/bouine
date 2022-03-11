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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
)

type Config struct {
	Leader  string
	ID      string
	Address string
}

// type raftedRistretto struct{}

var ErrForwardToLeaderAsLeader = errors.New("cannot forward to leader as leader")

// func (rr *raftedRistretto) Apply(*raft.Log) interface{} {
// 	return nil
// }

// func (rr *raftedRistretto) Snapshot() (raft.FSMSnapshot, error) {
// 	return nil, nil
// }

// func (rr *raftedRistretto) Restore(snapshot io.ReadCloser) error {
// 	return nil
// }

// ForwardToLeader forwards request to leader and return leader response.
func ForwardToLeader(config Config) error {
	if IsLeader(config) {
		return ErrForwardToLeaderAsLeader
	}
	return nil
}

// IsLeader returns true if the current node is cluster leader.
func IsLeader(config Config) bool {
	return config.Leader == "leader" // Not implemented yet
}

func NewRaft(ctx context.Context, raftDir, myID, myAddress string, raftBootstrap bool, fsm raft.FSM) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

	baseDir := filepath.Join(raftDir, myID)

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf(`cannot create snapshot store (%q, ...): %v`, baseDir, err)
	}

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{grpc.WithInsecure()})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create new raft: %v", err)
	}

	if raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(myID),
					Address:  raft.ServerAddress(myAddress),
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
