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

package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	"github.com/pulumi/pulumi/sdk/v3/go/common/tokens"
	"github.com/pulumi/pulumi/sdk/v3/go/common/util/rpcutil/rpcerror"
	"github.com/pulumi/pulumi/sdk/v3/go/property"
	pulumirpc "github.com/pulumi/pulumi/sdk/v3/proto/go"
)

type recordedHint struct {
	hint    Hint
	message string
}

type testRecorder struct {
	mu   sync.Mutex
	seen []recordedHint
}

func (r *testRecorder) RecordHint(hint Hint, message string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, recordedHint{hint, message})
}

var escHint = recordedHint{HintDynamicCredentials, "use ESC"}

func TestHintOf(t *testing.T) {
	t.Parallel()

	credsErr := WithHint(rpcerror.New(codes.Unauthenticated, "expired token"), HintDynamicCredentials, "use ESC")

	tests := []struct {
		name string
		err  error
		want recordedHint
	}{
		{"nil", nil, recordedHint{}},
		{"plain error", errors.New("boom"), recordedHint{}},
		{"status without detail", rpcerror.New(codes.Unauthenticated, "boom"), recordedHint{}},
		{"status with detail", credsErr, escHint},
		{"converted and wrapped", fmt.Errorf("configure: %w", rpcerror.Convert(credsErr)), escHint},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			hint, message := HintOf(tt.err)
			assert.Equal(t, tt.want, recordedHint{hint, message})
		})
	}
}

// newHintTestContext returns a plugin context that reports hints to rec.
func newHintTestContext(t *testing.T, rec HintRecorder) *Context {
	pctx := newTestContext(t)
	pctx.HintRecorder = rec
	return pctx
}

func TestProviderRecordsCheckConfigFailureHint(t *testing.T) {
	t.Parallel()

	client := &stubClient{
		CheckConfigF: func(*pulumirpc.CheckRequest) (*pulumirpc.CheckResponse, error) {
			return &pulumirpc.CheckResponse{Failures: []*pulumirpc.CheckFailure{
				{Reason: "unrelated"},
				{Reason: "no credentials", Hint: pulumirpc.Hint_HINT_DYNAMIC_CREDENTIALS, HintMessage: "use ESC"},
			}}, nil
		},
	}
	rec := &testRecorder{}
	p := NewProviderWithClient(newHintTestContext(t, rec), client, false /* disablePreview */)

	resp, err := p.CheckConfig(t.Context(), CheckConfigRequest{
		URN:  resource.NewURN("stack", "proj", "", "pulumi:providers:aws", "default"),
		News: property.Map{},
	})
	require.NoError(t, err)
	require.Len(t, resp.Failures, 2)
	assert.Equal(t, HintUnspecified, resp.Failures[0].Hint)
	assert.Equal(t, HintDynamicCredentials, resp.Failures[1].Hint)
	assert.Equal(t, "use ESC", resp.Failures[1].HintMessage)
	assert.Equal(t, []recordedHint{escHint}, rec.seen)
}

func TestProviderRecordsConfigureErrorHint(t *testing.T) {
	t.Parallel()

	client := &stubClient{
		ConfigureF: func(*pulumirpc.ConfigureRequest) (*pulumirpc.ConfigureResponse, error) {
			return nil, WithHint(rpcerror.New(codes.Unauthenticated, "expired token"), HintDynamicCredentials, "use ESC")
		},
	}
	rec := &testRecorder{}
	p := NewProviderWithClient(newHintTestContext(t, rec), client, false /* disablePreview */)

	_, err := p.Configure(t.Context(), ConfigureRequest{Type: new(tokens.Type("pulumi:providers:gcp"))})
	require.ErrorContains(t, err, "expired token")
	assert.Equal(t, []recordedHint{escHint}, rec.seen)
}

func TestProviderRecordsCreateErrorHint(t *testing.T) {
	t.Parallel()

	client := &stubClient{
		ConfigureF: func(*pulumirpc.ConfigureRequest) (*pulumirpc.ConfigureResponse, error) {
			return &pulumirpc.ConfigureResponse{}, nil
		},
		CreateF: func(*pulumirpc.CreateRequest) (*pulumirpc.CreateResponse, error) {
			return nil, WithHint(rpcerror.New(codes.Unauthenticated, "expired token"), HintDynamicCredentials, "use ESC")
		},
	}
	rec := &testRecorder{}
	p := NewProviderWithClient(newHintTestContext(t, rec), client, false /* disablePreview */)
	_, err := p.Configure(t.Context(), ConfigureRequest{Type: new(tokens.Type("pulumi:providers:aws"))})
	require.NoError(t, err)

	_, err = p.Create(t.Context(), CreateRequest{
		URN:        resource.NewURN("stack", "proj", "", "aws:s3/bucket:Bucket", "b"),
		Properties: property.Map{},
	})
	require.ErrorContains(t, err, "expired token")
	assert.Equal(t, []recordedHint{escHint}, rec.seen)
}

func TestProviderServerSendsCheckFailureHint(t *testing.T) {
	t.Parallel()

	server := NewProviderServer(&MockProvider{
		CheckConfigF: func(context.Context, CheckConfigRequest) (CheckConfigResponse, error) {
			return CheckConfigResponse{Failures: []CheckFailure{
				{Reason: "no credentials", Hint: HintDynamicCredentials, HintMessage: "use ESC"},
			}}, nil
		},
	})
	resp, err := server.CheckConfig(t.Context(), &pulumirpc.CheckRequest{
		Urn: "urn:pulumi:stack::proj::pulumi:providers:aws::default",
	})
	require.NoError(t, err)
	require.Len(t, resp.GetFailures(), 1)
	assert.Equal(t, pulumirpc.Hint_HINT_DYNAMIC_CREDENTIALS, resp.GetFailures()[0].GetHint())
	assert.Equal(t, "use ESC", resp.GetFailures()[0].GetHintMessage())
}
