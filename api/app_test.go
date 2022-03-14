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
package main

import (
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/thylong/bouine/internal/backend"
)

func Test_createFiberApp(t *testing.T) {
	type args struct {
		httpTimeout   int64
		raftAddress   string
		prod          bool
		upstream      string
		raftID        string
		raftDir       string
		raftBootstrap bool
	}
	tests := []struct {
		name string
		args args
		want *fiber.App
	}{
		{name: "flags with default values except raftBootrap(true)", args: args{
			httpTimeout:   3000,
			raftAddress:   "localhost:50051",
			prod:          false,
			upstream:      "http://mockingjay:8084",
			raftID:        "1",
			raftDir:       "/tmp/",
			raftBootstrap: true,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend.BoltdbFilesCleanup(tt.args.raftDir)

			_, _ = createFiberApp(tt.args.httpTimeout, tt.args.raftAddress, tt.args.prod, tt.args.upstream, tt.args.raftID, tt.args.raftDir, tt.args.raftBootstrap)
			// if !reflect.DeepEqual(app, tt.want) {
			// 	t.Errorf("createFiberApp() = %v, want %v", app, tt.want)
			// }
		})
	}
}
