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
	"reflect"
	"testing"

	transport "github.com/Jille/raft-grpc-transport"
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
		myID          string
		myAddress     string
		raftBootstrap bool
		fsm           raft.FSM
	}
	tests := []struct {
		name    string
		args    args
		want    *raft.Raft
		want1   *transport.Manager
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := NewRaft(tt.args.ctx, tt.args.raftDir, tt.args.myID, tt.args.myAddress, tt.args.raftBootstrap, tt.args.fsm)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRaft() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewRaft() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("NewRaft() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
