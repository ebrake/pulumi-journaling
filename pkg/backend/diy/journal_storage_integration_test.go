// Copyright 2026, E Brake.
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
	"context"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"

	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/env"
	"github.com/pulumi/pulumi/sdk/v3/go/common/testing/diagtest"
	"github.com/pulumi/pulumi/sdk/v3/go/common/workspace"
)

// journalRacingBucket injects a competing write after the checkpoint's version was read.
type journalRacingBucket struct {
	Bucket
	key         string
	replacement []byte
}

func (b *journalRacingBucket) WriteAll(ctx context.Context, key string, data []byte, opts *blob.WriterOptions) error {
	if key == b.key {
		if err := b.Bucket.WriteAll(ctx, key, b.replacement, nil); err != nil {
			return err
		}
	}
	return b.Bucket.WriteAll(ctx, key, data, opts)
}

// TestDIYJournalStorageBackends exercises the actual drivers against dedicated test prefixes.
// Hosted cases are opt-in; CI supplies local emulators, never cloud credentials.
//
//nolint:paralleltest // Competing writes within each backend fixture must run sequentially.
func TestDIYJournalStorageBackends(t *testing.T) {
	t.Parallel()
	for _, scheme := range []string{"file", "gs", "s3", "azblob", "postgres"} {
		t.Run(scheme, func(t *testing.T) {
			t.Parallel()
			backendURL := os.Getenv("PULUMI_JOURNAL_TEST_" + strings.ToUpper(scheme) + "_URL")
			if scheme == "file" {
				backendURL = "file://" + filepath.ToSlash(t.TempDir())
			} else if backendURL == "" {
				t.Skip("set PULUMI_JOURNAL_TEST_" + strings.ToUpper(scheme) + "_URL to a dedicated test prefix")
			}
			parsed, err := url.Parse(backendURL)
			require.NoError(t, err)
			require.Equal(t, scheme, parsed.Scheme)
			if scheme == "postgres" {
				require.True(t, strings.HasPrefix(parsed.Query().Get("table"), "journal_canary_"))
			} else if scheme != "file" {
				require.Contains(t, parsed.Path, "/journal-canary/", "remote tests require a dedicated journal-canary prefix")
			}
			for _, compressed := range []bool{false, true} {
				t.Run(strconv.FormatBool(compressed), func(t *testing.T) {
					t.Parallel()
					location := *parsed
					if scheme == "postgres" {
						query := location.Query()
						query.Set("table", query.Get("table")+strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""))
						location.RawQuery = query.Encode()
					} else {
						location.Path = path.Join(location.Path, uuid.Must(uuid.NewV4()).String())
					}
					if scheme == "file" {
						require.NoError(t, os.MkdirAll(location.Path, 0o700))
					}
					b, err := newDIYBackend(t.Context(), diagtest.LogSink(t), location.String(),
						&workspace.Project{Name: "testproject"}, &diyBackendOptions{Env: env.NewEnv(env.MapStore{
							env.DIYBackendJournaling.Var().Name(): "true",
							env.DIYBackendGzip.Var().Name():       strconv.FormatBool(compressed),
						})})
					require.NoError(t, err)
					ref, err := b.store.ParseReference("organization/testproject/teststack")
					require.NoError(t, err)
					_, err = b.CreateStack(t.Context(), ref, "", nil, nil)
					require.NoError(t, err)
					m, err := b.startJournal(t.Context(), ref)
					require.NoError(t, err)
					writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
						Version: 1, SequenceID: 1, OperationID: 1,
						Kind: apitype.JournalEntryKindSuccess, State: journalResource(),
					})
					// A reader sees the base; recovery commits the durable entry exactly once.
					checkpoint, _, _, err := b.getCheckpoint(t.Context(), ref)
					require.NoError(t, err)
					if checkpoint.Latest != nil {
						require.Empty(t, checkpoint.Latest.Resources)
					}
					require.NoError(t, b.RecoverJournal(t.Context(), ref))
					require.NoError(t, b.RecoverJournal(t.Context(), ref))
					checkpoint, _, features, err := b.getCleanCheckpoint(t.Context(), ref)
					require.NoError(t, err)
					require.NotContains(t, features, diyJournalFeature)
					require.Len(t, checkpoint.Latest.Resources, 1)
					require.Equal(t, journalResource().URN, checkpoint.Latest.Resources[0].URN)
					// Immutable writes may only succeed on retry if the stored bytes agree.
					key := path.Join(b.journalDir(ref), "immutable-probe")
					require.NoError(t, b.journalPut(t.Context(), key, []byte("original")))
					require.NoError(t, b.journalPut(t.Context(), key, []byte("original")))
					require.Error(t, b.journalPut(t.Context(), key, []byte("overwrite")))
					// Conditional replacement must preserve a competing object, including multipart checkpoints.
					for _, size := range []int{16, 12 * 1024 * 1024} {
						t.Run(strconv.Itoa(size), func(t *testing.T) {
							original := []byte(strings.Repeat("a", size))
							replacement := []byte(strings.Repeat("b", size))
							require.NoError(t, b.bucket.WriteAll(t.Context(), key, original, nil))
							underlying := b.bucket
							b.bucket = &journalRacingBucket{Bucket: underlying, key: key, replacement: replacement}
							if scheme != "file" {
								require.Error(t, b.journalWrite(t.Context(), key, []byte(strings.Repeat("c", size)), journalHash(original)))
								observed, err := underlying.ReadAll(t.Context(), key)
								require.NoError(t, err)
								require.Equal(t, replacement, observed)
							}
							b.bucket = underlying
						})
					}
				})
			}
		})
	}
}
