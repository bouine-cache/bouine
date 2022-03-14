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
	"testing"
	"time"

	"github.com/gofiber/fiber/v2/utils"
	"github.com/hashicorp/raft"
)

func Test_ForwardToLeader_AsLeader(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "leader",
	}

	utils.AssertEqual(t, ForwardToLeader(config), ErrForwardToLeaderAsLeader)
}
func Test_ForwardToLeader_AsFollower(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "follower",
	}

	utils.AssertEqual(t, ForwardToLeader(config), nil)
}

func Test_ForwardToLeader_AsCandidate(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "candidate",
	}

	utils.AssertEqual(t, ForwardToLeader(config), nil)
}

func Test_IsLeader_AsLeader(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "leader",
	}

	utils.AssertEqual(t, IsLeader(config), true)
}

func Test_IsLeader_AsFollower(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "follower",
	}

	utils.AssertEqual(t, IsLeader(config), false)
}

func Test_IsLeader_AsCandidate(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "candidate",
	}

	utils.AssertEqual(t, IsLeader(config), false)
}

func TestNewRaft(t *testing.T) {
	type args struct {
		ctx           context.Context
		raftDir       string
		raftNodeID    string
		hostAddress   string
		raftBootstrap bool
		fsm           raft.FSM
	}
	tests := []struct {
		name          string
		args          args
		wantRaftStats map[string]string
		wantErr       bool
	}{
		{name: "missing-config", args: args{ctx: context.Background()}, wantRaftStats: map[string]string{}, wantErr: true},
		{name: "missing-valid-raftDir", args: args{ctx: context.Background(), raftDir: "/foobar12345678910/", raftNodeID: "1", raftBootstrap: false, hostAddress: "localhost:4566", fsm: &raft.MockFSM{}}, wantRaftStats: map[string]string{}, wantErr: true},
		{name: "successful-leader-start", args: args{ctx: context.Background(), raftDir: "/tmp/", raftNodeID: "1", raftBootstrap: true, hostAddress: "localhost:4596", fsm: &raft.MockFSM{}}, wantRaftStats: map[string]string{}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			BoltdbFilesCleanup(tt.args.raftDir)
			got, _, err := NewRaft(tt.args.ctx, tt.args.raftDir, tt.args.raftNodeID, tt.args.hostAddress, tt.args.raftBootstrap, tt.args.fsm)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRaft() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// at this point, boltdb files have been created
			defer BoltdbFilesCleanup(tt.args.raftDir)

			// prevent further execution of "wantErr: true" cases
			if err != nil {
				return
			}

			time.Sleep(2 * time.Second)

			// Apply should not fail on single node leader
			// note: we don't test the response as a mocked FSM is used.
			var timeout time.Duration = 1
			future := got.Apply([]byte("foobar"), timeout)
			if err = future.Error(); err != nil {
				t.Errorf("NewRaft() unexpected error = %v", err)
				return
			}
		})
	}
}
