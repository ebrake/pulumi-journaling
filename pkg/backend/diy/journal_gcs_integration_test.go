//go:build integration

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

// Modified by E Brake for pulumi-journaling: durable DIY journaling and recovery.

package diy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"golang.org/x/sync/errgroup"

	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/env"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	"github.com/pulumi/pulumi/sdk/v3/go/common/testing/diagtest"
	"github.com/pulumi/pulumi/sdk/v3/go/common/tokens"
	"github.com/pulumi/pulumi/sdk/v3/go/common/workspace"
)

type journalMetricBucket struct {
	Bucket
	uploaded   atomic.Int64
	downloaded atomic.Int64
	puts       atomic.Int64
}

func (b *journalMetricBucket) WriteAll(ctx context.Context, key string, data []byte, opts *blob.WriterOptions) error {
	b.uploaded.Add(int64(len(data)))
	b.puts.Add(1)
	return b.Bucket.WriteAll(ctx, key, data, opts)
}

func (b *journalMetricBucket) ReadAll(ctx context.Context, key string) ([]byte, error) {
	data, err := b.Bucket.ReadAll(ctx, key)
	b.downloaded.Add(int64(len(data)))
	return data, err
}

func (b *journalMetricBucket) NewReader(
	ctx context.Context, key string, opts *blob.ReaderOptions,
) (*blob.Reader, error) {
	reader, err := b.Bucket.NewReader(ctx, key, opts)
	if err == nil {
		b.downloaded.Add(reader.Size())
	}
	return reader, err
}

type journalMeasurement struct {
	Mode            string  `json:"mode"`
	Resources       int     `json:"resources"`
	Seconds         float64 `json:"seconds"`
	UploadedBytes   int64   `json:"uploadedBytes"`
	DownloadedBytes int64   `json:"downloadedBytes"`
	ObjectPuts      int64   `json:"objectPuts"`
}

// TestDIYJournalGCSPerformance requires an explicit integration build and a dedicated test prefix.
//
//nolint:paralleltest // Sequential samples compare persistence modes without competing storage traffic.
func TestDIYJournalGCSPerformance(t *testing.T) {
	prefix := os.Getenv("PULUMI_DIY_JOURNAL_GCS_TEST_PREFIX")
	if prefix == "" {
		t.Skip("set PULUMI_DIY_JOURNAL_GCS_TEST_PREFIX to opt into hosted storage")
	}
	require.True(t, strings.HasPrefix(prefix, "gs://") && strings.Contains(prefix, "/journal-canary/"))
	var measurements []journalMeasurement
	for _, count := range []int{25, 100} {
		states := make([]apitype.ResourceV3, count)
		for i := range states {
			var payload strings.Builder
			for j := range 16 {
				sum := sha256.Sum256([]byte(fmt.Sprintf("resource-%d-%d", i, j)))
				payload.WriteString(hex.EncodeToString(sum[:]))
			}
			states[i] = apitype.ResourceV3{
				URN:  resource.URN(fmt.Sprintf("urn:pulumi:teststack::journal-bench::test:index:Item::item-%d", i)),
				Type: tokens.Type("test:index:Item"), Outputs: map[string]any{"data": payload.String()},
			}
		}
		for _, journaled := range []bool{false, true} {
			mode := "checkpoint"
			if journaled {
				mode = "journal"
			}
			t.Run(fmt.Sprintf("%d/%s", count, mode), func(t *testing.T) {
				store := env.MapStore{env.DIYBackendGzip.Var().Name(): "true", env.DIYBackendParallel.Var().Name(): "16"}
				b, err := newDIYBackend(t.Context(), diagtest.LogSink(t),
					fmt.Sprintf("%s/%d/%s", prefix, count, mode),
					&workspace.Project{Name: tokens.PackageName("journal-bench")}, &diyBackendOptions{Env: env.NewEnv(store)})
				require.NoError(t, err)
				ref, err := b.store.ParseReference("organization/journal-bench/teststack")
				require.NoError(t, err)
				_, err = b.CreateStack(t.Context(), ref, "", nil, nil)
				require.NoError(t, err)
				metrics := &journalMetricBucket{Bucket: b.bucket}
				b.bucket = metrics
				started := time.Now()
				if journaled {
					m, err := b.startJournal(t.Context(), ref)
					require.NoError(t, err)
					var group errgroup.Group
					group.SetLimit(16)
					for i := range states {
						group.Go(func() error {
							e := apitype.JournalEntry{
								Version: 1, SequenceID: int64(i + 1), OperationID: int64(i + 1),
								Kind: apitype.JournalEntryKindSuccess, State: &states[i],
							}
							data, err := json.Marshal(e)
							if err != nil {
								return err
							}
							key := path.Join(b.journalDir(ref), m.Epoch, "entries", fmt.Sprintf("%020d.journal.json", e.SequenceID))
							return b.bucket.WriteAll(t.Context(), key, data, &blob.WriterOptions{IfNotExist: true})
						})
					}
					require.NoError(t, group.Wait())
					require.NoError(t, b.consolidateJournal(t.Context(), ref, ""))
				} else {
					manifest := apitype.ManifestV1{}
					manifest.Magic = manifest.NewMagic()
					for i := range states {
						_, err := b.saveStack(t.Context(), ref, apitype.TypedDeployment{
							Version: 3, Deployment: &apitype.DeploymentV3{Manifest: manifest, Resources: states[:i+1]},
						})
						require.NoError(t, err)
					}
				}
				measurements = append(measurements, journalMeasurement{
					Mode: mode, Resources: count,
					Seconds: time.Since(started).Seconds(), UploadedBytes: metrics.uploaded.Load(),
					DownloadedBytes: metrics.downloaded.Load(), ObjectPuts: metrics.puts.Load(),
				})
				cp, _, _, err := b.getCheckpoint(t.Context(), ref)
				require.NoError(t, err)
				require.Equal(t, states, cp.Latest.Resources)
			})
		}
	}
	data, err := json.MarshalIndent(measurements, "", "  ")
	require.NoError(t, err)
	t.Log(string(data))
	if output := os.Getenv("PULUMI_DIY_JOURNAL_BENCHMARK_OUTPUT"); output != "" {
		require.NoError(t, os.WriteFile(output, data, 0o600))
	}
}

// TestDIYJournalGCSLargeStateSingleChange measures the fixed consolidation cost for one mutation of a large state.
//
//nolint:paralleltest // Sequential storage samples avoid competing traffic.
func TestDIYJournalGCSLargeStateSingleChange(t *testing.T) {
	prefix := os.Getenv("PULUMI_DIY_JOURNAL_GCS_TEST_PREFIX")
	if prefix == "" {
		t.Skip("set PULUMI_DIY_JOURNAL_GCS_TEST_PREFIX to opt into hosted storage")
	}
	require.True(t, strings.HasPrefix(prefix, "gs://") && strings.Contains(prefix, "/journal-canary/"))
	const count = 5000
	states := make([]apitype.ResourceV3, count)
	for i := range states {
		var payload strings.Builder
		for j := range 16 {
			sum := sha256.Sum256([]byte(fmt.Sprintf("large-resource-%d-%d", i, j)))
			payload.WriteString(hex.EncodeToString(sum[:]))
		}
		states[i] = apitype.ResourceV3{
			URN:  resource.URN(fmt.Sprintf("urn:pulumi:teststack::journal-bench::test:index:Item::item-%d", i)),
			Type: tokens.Type("test:index:Item"), Outputs: map[string]any{"data": payload.String()},
		}
	}
	updated := states[0]
	updated.Outputs = map[string]any{"data": "changed"}
	expected := append([]apitype.ResourceV3{}, states...)
	expected[0] = updated
	var measurements []journalMeasurement
	for _, journaled := range []bool{false, true} {
		mode := "checkpoint"
		if journaled {
			mode = "journal"
		}
		t.Run(mode, func(t *testing.T) {
			b, err := newDIYBackend(t.Context(), diagtest.LogSink(t), prefix+"/single-change/"+mode,
				&workspace.Project{Name: "journal-bench"}, &diyBackendOptions{Env: env.NewEnv(env.MapStore{
					env.DIYBackendGzip.Var().Name(): "true", env.DIYBackendParallel.Var().Name(): "16",
				})})
			require.NoError(t, err)
			ref, err := b.store.ParseReference("organization/journal-bench/teststack")
			require.NoError(t, err)
			_, err = b.CreateStack(t.Context(), ref, "", nil, nil)
			require.NoError(t, err)
			manifest := apitype.ManifestV1{}
			manifest.Magic = manifest.NewMagic()
			_, err = b.saveStack(t.Context(), ref, apitype.TypedDeployment{
				Version: 3, Deployment: &apitype.DeploymentV3{Manifest: manifest, Resources: states},
			})
			require.NoError(t, err)
			metrics := &journalMetricBucket{Bucket: b.bucket}
			b.bucket = metrics
			started := time.Now()
			if journaled {
				m, err := b.startJournal(t.Context(), ref)
				require.NoError(t, err)
				oldIndex := int64(0)
				writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
					Version: 1, SequenceID: 1, OperationID: 1, Kind: apitype.JournalEntryKindSuccess,
					State: &updated, RemoveOld: &oldIndex,
				})
				require.NoError(t, b.consolidateJournal(t.Context(), ref, m.Epoch))
			} else {
				_, err := b.saveStack(t.Context(), ref, apitype.TypedDeployment{
					Version: 3, Deployment: &apitype.DeploymentV3{Manifest: manifest, Resources: expected},
				})
				require.NoError(t, err)
			}
			measurements = append(measurements, journalMeasurement{
				Mode: mode, Resources: count, Seconds: time.Since(started).Seconds(),
				UploadedBytes: metrics.uploaded.Load(), DownloadedBytes: metrics.downloaded.Load(), ObjectPuts: metrics.puts.Load(),
			})
			cp, _, _, err := b.getCheckpoint(t.Context(), ref)
			require.NoError(t, err)
			require.Equal(t, expected, cp.Latest.Resources)
		})
	}
	data, err := json.MarshalIndent(measurements, "", "  ")
	require.NoError(t, err)
	t.Log(string(data))
	if output := os.Getenv("PULUMI_DIY_JOURNAL_BENCHMARK_OUTPUT"); output != "" {
		require.NoError(t, os.WriteFile(output, data, 0o600))
	}
}
