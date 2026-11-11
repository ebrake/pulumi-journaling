// Copyright 2026, Pulumi Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package diy

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGCSBackendAPI(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, input, api, remaining string
		invalid                     bool
	}{
		{name: "ordinary", input: "gs://bucket/prefix", remaining: "gs://bucket/prefix"},
		{name: "ordinary options", input: "gs://bucket?anonymous=true", remaining: "gs://bucket?anonymous=true"},
		{name: "grpc", input: "gs://bucket/prefix?gcs_api=grpc", api: "grpc", remaining: "gs://bucket/prefix"},
		{name: "rapid", input: "gs://bucket/prefix?gcs_api=rapid", api: "rapid", remaining: "gs://bucket/prefix"},
		{name: "unknown", input: "gs://bucket?gcs_api=unknown", invalid: true},
		{name: "repeated", input: "gs://bucket?gcs_api=grpc&gcs_api=rapid", invalid: true},
		{name: "incompatible", input: "gs://bucket?gcs_api=rapid&anonymous=true", invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			u, err := url.Parse(test.input)
			require.NoError(t, err)
			api, err := gcsBackendAPI(u)
			if test.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.api, api)
			assert.Equal(t, test.remaining, u.String())
		})
	}
}
