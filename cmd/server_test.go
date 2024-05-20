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
package main

import (
	"testing"

	fiber "github.com/gofiber/fiber/v2"
)

func Test_createApp(t *testing.T) {
	type args struct {
		httpTimeout  int64
		prod         bool
		upstream     string
		loggingLevel string
	}
	tests := []struct {
		name string
		args args
		want *fiber.App
	}{
		{name: "flags with default values", args: args{
			httpTimeout:  3000,
			prod:         false,
			upstream:     "http://mockingjay:8084",
			loggingLevel: "debug",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _ = createApp(tt.args.httpTimeout, tt.args.prod, tt.args.upstream, tt.args.loggingLevel)
			// if !reflect.DeepEqual(app, tt.want) {
			// 	t.Errorf("createApp() = %v, want %v", app, tt.want)
			// }
		})
	}
}
