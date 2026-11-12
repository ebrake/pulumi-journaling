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

package diy

import (
	"context"
	"net/url"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"

	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/env"
	"github.com/pulumi/pulumi/sdk/v3/go/common/testing/diagtest"
	"github.com/pulumi/pulumi/sdk/v3/go/common/tokens"
	"github.com/pulumi/pulumi/sdk/v3/go/common/workspace"
)

// TestGCSRapidTransport requires a dedicated canary prefix and never calls providers.
//
//nolint:paralleltest // Hosted storage requires explicit opt-in and serial fault scenarios.
func TestGCSRapidTransport(t *testing.T) {
	prefix := os.Getenv("PULUMI_DIY_JOURNAL_RAPID_TEST_PREFIX")
	if prefix == "" {
		t.Skip("set PULUMI_DIY_JOURNAL_RAPID_TEST_PREFIX to opt in")
	}
	require.Contains(t, prefix, "/journal-canary/")
	u, err := url.Parse(prefix)
	require.NoError(t, err)
	require.Equal(t, "rapid", u.Query().Get("gcs_api"))
	u.Path = path.Join(u.Path, strconv.FormatInt(time.Now().UnixNano(), 10))
	project := &workspace.Project{Name: tokens.PackageName("rapid-canary")}
	b, err := newDIYBackend(t.Context(), diagtest.LogSink(t), u.String(), project, nil)
	require.NoError(t, err)
	rapid, ok := b.bucket.(*rapidBucket)
	require.True(t, ok)
	first := []byte("original complete bytes")
	second := []byte("replacement complete bytes")
	key := "probe/checkpoint"
	require.NoError(t, b.bucket.WriteAll(t.Context(), key, first, nil))
	err = b.bucket.WriteAll(t.Context(), key, second, &blob.WriterOptions{IfNotExist: true})
	require.ErrorContains(t, err, "condition")
	actual, err := b.bucket.ReadAll(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, first, actual)
	require.NoError(t, b.journalWrite(t.Context(), key, second, journalHash(first)))
	err = b.journalWrite(t.Context(), key, first, journalHash(first))
	require.ErrorContains(t, err, "checkpoint changed unexpectedly")
	err = b.bucket.WriteAll(t.Context(), key, first, &blob.WriterOptions{BeforeWrite: func(as func(any) bool) error {
		var handle **storage.ObjectHandle
		require.True(t, as(&handle))
		*handle = (*handle).If(storage.Conditions{GenerationMatch: 1})
		return nil
	}})
	require.ErrorContains(t, err, "condition")
	actual, err = b.bucket.ReadAll(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, second, actual)
	require.NoError(t, b.bucket.Copy(t.Context(), "probe/backup", key, nil))
	actual, err = b.bucket.ReadAll(t.Context(), "probe/backup")
	require.NoError(t, err)
	require.Equal(t, second, actual)
	ref, err := b.store.ParseReference("organization/rapid-canary/teststack")
	require.NoError(t, err)
	_, err = b.CreateStack(t.Context(), ref, "", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.Lock(t.Context(), ref))
	b2, err := newDIYBackend(t.Context(), diagtest.LogSink(t), u.String(), project, nil)
	require.NoError(t, err)
	ref2, err := b2.store.ParseReference("organization/rapid-canary/teststack")
	require.NoError(t, err)
	require.ErrorContains(t, b2.Lock(t.Context(), ref2), "the stack is currently locked")
	b.Unlock(t.Context(), ref)
	require.NoError(t, b2.Lock(t.Context(), ref2))
	b2.Unlock(t.Context(), ref2)
	// An interrupted append stream is never a state object and cannot overwrite the published checkpoint.
	ctx, cancel := context.WithCancel(t.Context())
	unfinished := rapid.client.Bucket(rapid.name).Object(rapid.prefix + ".pulumi/rapid-uploads/interrupted")
	w := unfinished.NewWriter(ctx)
	w.Append = true
	w.FinalizeOnClose = false
	_, err = w.Write([]byte("incomplete"))
	require.NoError(t, err)
	_, err = w.Flush()
	require.NoError(t, err)
	cancel()
	_ = w.Close()
	actual, err = b.bucket.ReadAll(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, second, actual)
	require.NoError(t, unfinished.Delete(t.Context()))
}

//nolint:paralleltest // Explicit opt-in hosted recovery canaries are serial.
func TestGCSRapidJournalRecovery(t *testing.T) {
	prefix := os.Getenv("PULUMI_DIY_JOURNAL_RAPID_TEST_PREFIX")
	if prefix == "" {
		t.Skip("set PULUMI_DIY_JOURNAL_RAPID_TEST_PREFIX to opt in")
	}
	require.Contains(t, prefix, "/journal-canary/")
	for _, pending := range []bool{false, true} {
		t.Run(strconv.FormatBool(pending), func(t *testing.T) {
			u, err := url.Parse(prefix)
			require.NoError(t, err)
			u.Path = path.Join(u.Path, strconv.FormatInt(time.Now().UnixNano(), 10))
			project := &workspace.Project{Name: tokens.PackageName("testproject")}
			options := &diyBackendOptions{Env: env.NewEnv(env.MapStore{env.DIYBackendGzip.Var().Name(): "true"})}
			b, err := newDIYBackend(t.Context(), diagtest.LogSink(t), u.String(), project, options)
			require.NoError(t, err)
			ref, err := b.store.ParseReference("organization/testproject/teststack")
			require.NoError(t, err)
			_, err = b.CreateStack(t.Context(), ref, "", nil, nil)
			require.NoError(t, err)
			m, err := b.startJournal(t.Context(), ref)
			require.NoError(t, err)
			entry := apitype.JournalEntry{
				Version: 1, SequenceID: 1, OperationID: 1, Kind: apitype.JournalEntryKindSuccess, State: journalResource(),
			}
			if pending {
				entry.Kind = apitype.JournalEntryKindBegin
				entry.State = nil
				entry.Operation = &apitype.OperationV2{Resource: *journalResource(), Type: apitype.OperationTypeCreating}
			}
			frame, err := encodeRapidJournalFrame(entry)
			require.NoError(t, err)
			rapid := b.rapid
			key := path.Join(b.journalDir(ref), m.Epoch, "journal.stream")
			object := rapid.client.Bucket(rapid.name).Object(rapid.prefix + key)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			writer := object.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
			t.Cleanup(func() { _ = writer.Close() })
			writer.Append = true
			writer.FinalizeOnClose = false
			durable := append([]byte(rapidJournalMagic), frame...)
			_, err = writer.Write(durable)
			require.NoError(t, err)
			_, err = writer.Flush()
			require.NoError(t, err)
			// A partially persisted next frame is unacknowledged and must not enter recovered state.
			_, err = writer.Write(frame[:len(frame)/2])
			require.NoError(t, err)
			persisted, err := writer.Flush()
			require.NoError(t, err)
			require.Equal(t, int64(len(durable)+len(frame)/2), persisted)
			// A crashed writer cannot Close to refresh object metadata. Persisted bytes must remain readable even
			// when the unfinalized object's metadata size is stale or zero.
			cancel()
			actual, err := rapid.readStream(t.Context(), key)
			require.NoError(t, err)
			require.Equal(t, append(durable, frame[:len(frame)/2]...), actual)
			// Recovery opens a fresh backend, so no live journal manager or local cache can supply missing state.
			recovered, err := newDIYBackend(t.Context(), diagtest.LogSink(t), u.String(), project, options)
			require.NoError(t, err)
			recoveredRef, err := recovered.store.ParseReference("organization/testproject/teststack")
			require.NoError(t, err)
			require.NoError(t, recovered.RecoverJournal(t.Context(), recoveredRef))
			require.NoError(t, recovered.RecoverJournal(t.Context(), recoveredRef))
			cp, _, features, err := recovered.getCheckpoint(t.Context(), recoveredRef)
			require.NoError(t, err)
			require.NotContains(t, features, diyJournalFeature)
			if pending {
				require.Empty(t, cp.Latest.Resources)
				require.Len(t, cp.Latest.PendingOperations, 1)
				require.Equal(t, apitype.OperationTypeCreating, cp.Latest.PendingOperations[0].Type)
			} else {
				require.Len(t, cp.Latest.Resources, 1)
				require.Equal(t, journalResource().URN, cp.Latest.Resources[0].URN)
			}
			require.NoError(t, recovered.requireCleanJournal(t.Context(), recoveredRef))
		})
	}
}
