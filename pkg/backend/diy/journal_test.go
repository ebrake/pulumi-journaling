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
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"

	"github.com/pulumi/pulumi/pkg/v3/engine"
	"github.com/pulumi/pulumi/pkg/v3/secrets/b64"
	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/encoding"
	"github.com/pulumi/pulumi/sdk/v3/go/common/env"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	"github.com/pulumi/pulumi/sdk/v3/go/common/testing/diagtest"
	"github.com/pulumi/pulumi/sdk/v3/go/common/tokens"
	"github.com/pulumi/pulumi/sdk/v3/go/common/workspace"
)

func newDIYJournalTest(t *testing.T, gzip bool) (*diyBackend, *diyBackendReference) {
	t.Helper()
	store := env.MapStore{
		env.DIYBackendJournaling.Var().Name(): "true",
		env.DIYBackendGzip.Var().Name():       strconv.FormatBool(gzip),
		env.DIYBackendParallel.Var().Name():   "1",
	}
	b, err := newDIYBackend(t.Context(), diagtest.LogSink(t), "file://"+filepath.ToSlash(t.TempDir()),
		&workspace.Project{Name: tokens.PackageName("testproject")}, &diyBackendOptions{Env: env.NewEnv(store)})
	require.NoError(t, err)
	ref, err := b.store.ParseReference("organization/testproject/teststack")
	require.NoError(t, err)
	_, err = b.CreateStack(t.Context(), ref, "", nil, nil)
	require.NoError(t, err)
	return b, ref
}

func journalResource() *apitype.ResourceV3 {
	return &apitype.ResourceV3{
		URN:  resource.URN("urn:pulumi:teststack::testproject::test:index:Resource::one"),
		Type: tokens.Type("test:index:Resource"), Outputs: map[string]any{"answer": 42.0},
	}
}

func writeDIYTestEntry(
	t *testing.T, b *diyBackend, ref *diyBackendReference, m *journalManifest, e apitype.JournalEntry,
) {
	t.Helper()
	data, err := json.Marshal(e)
	require.NoError(t, err)
	key := path.Join(b.journalDir(ref), m.Epoch, "entries", fmt.Sprintf("%020d.journal.json", e.SequenceID))
	require.NoError(t, b.bucket.WriteAll(t.Context(), key, data, &blob.WriterOptions{IfNotExist: true}))
}

func TestDIYJournalRecovery(t *testing.T) {
	t.Parallel()
	for _, gzip := range []bool{false, true} {
		t.Run(strconv.FormatBool(gzip), func(t *testing.T) {
			t.Parallel()
			b, ref := newDIYJournalTest(t, gzip)
			m, err := b.startJournal(t.Context(), ref)
			require.NoError(t, err)
			require.ErrorContains(t, b.requireCleanJournal(t.Context(), ref), "journal recover")
			_, _, _, err = b.getCheckpoint(t.Context(), ref)
			require.NoError(t, err)
			guardBytes, err := b.bucket.ReadAll(t.Context(), m.Checkpoint)
			require.NoError(t, err)
			guard, err := decodeJournalCheckpoint(guardBytes)
			require.NoError(t, err)
			require.Contains(t, guard.Features, diyJournalFeature)
			writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
				Version: 1, SequenceID: 1, OperationID: 1,
				Kind: apitype.JournalEntryKindSuccess, State: journalResource(),
			})
			require.NoError(t, b.RecoverJournal(t.Context(), ref))
			require.NoError(t, b.RecoverJournal(t.Context(), ref))
			cp, _, features, err := b.getCheckpoint(t.Context(), ref)
			require.NoError(t, err)
			require.NotContains(t, features, diyJournalFeature)
			require.Len(t, cp.Latest.Resources, 1)
			require.Equal(t, journalResource().URN, cp.Latest.Resources[0].URN)
			raw, err := b.bucket.ReadAll(t.Context(), b.stackPath(t.Context(), ref))
			require.NoError(t, err)
			require.Equal(t, gzip, encoding.DetectCompression(raw) == encoding.CompressionGzip)
			require.NoError(t, b.requireCleanJournal(t.Context(), ref))
		})
	}
}

type journalCrashBucket struct {
	Bucket
	mu        sync.Mutex
	remaining int
	after     bool
}

func (b *journalCrashBucket) step(after bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.after != after {
		return false
	}
	b.remaining--
	return b.remaining == 0
}

func (b *journalCrashBucket) WriteAll(ctx context.Context, key string, data []byte, opts *blob.WriterOptions) error {
	if b.step(false) {
		panic("simulated process termination")
	}
	err := b.Bucket.WriteAll(ctx, key, data, opts)
	if b.step(true) {
		panic("simulated process termination")
	}
	return err
}

func (b *journalCrashBucket) Copy(ctx context.Context, dst, src string, opts *blob.CopyOptions) error {
	if b.step(false) {
		panic("simulated process termination")
	}
	err := b.Bucket.Copy(ctx, dst, src, opts)
	if b.step(true) {
		panic("simulated process termination")
	}
	return err
}

func (b *journalCrashBucket) Delete(ctx context.Context, key string) error {
	if b.step(false) {
		return errors.New("simulated cleanup interruption")
	}
	err := b.Bucket.Delete(ctx, key)
	if b.step(true) {
		return errors.New("simulated cleanup interruption")
	}
	return err
}

func TestDIYJournalCrashBoundaries(t *testing.T) {
	t.Parallel()
	for _, after := range []bool{false, true} {
		for boundary := 1; boundary <= 14; boundary++ {
			t.Run(fmt.Sprintf("after=%v/boundary=%d", after, boundary), func(t *testing.T) {
				t.Parallel()
				b, ref := newDIYJournalTest(t, true)
				original := b.bucket
				faults := &journalCrashBucket{Bucket: original, remaining: boundary, after: after}
				b.bucket = faults
				func() {
					defer func() {
						if failure := recover(); failure != nil {
							require.Equal(t, "simulated process termination", failure)
						}
					}()
					m, err := b.startJournal(t.Context(), ref)
					require.NoError(t, err)
					writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
						Version: 1, SequenceID: 1, OperationID: 1,
						Kind: apitype.JournalEntryKindSuccess, State: journalResource(),
					})
					if err := b.consolidateJournal(t.Context(), ref, ""); err != nil {
						require.ErrorContains(t, err, "simulated cleanup interruption")
					}
				}()
				b.bucket = original
				require.NoError(t, b.RecoverJournal(t.Context(), ref))
				require.NoError(t, b.RecoverJournal(t.Context(), ref))
				require.NoError(t, b.requireCleanJournal(t.Context(), ref))
				cp, _, features, err := b.getCheckpoint(t.Context(), ref)
				require.NoError(t, err)
				require.NotContains(t, features, diyJournalFeature)
				if cp.Latest != nil {
					require.LessOrEqual(t, len(cp.Latest.Resources), 1)
					if len(cp.Latest.Resources) > 0 {
						require.Equal(t, journalResource().URN, cp.Latest.Resources[0].URN)
					}
				}
			})
		}
	}
}

func TestDIYJournalPreservesUnknownState(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"checkpoint", "manifest", "entry", "reference", "version"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			b, ref := newDIYJournalTest(t, false)
			m, err := b.startJournal(t.Context(), ref)
			require.NoError(t, err)
			switch kind {
			case "checkpoint":
				require.NoError(t, b.bucket.WriteAll(t.Context(), m.Checkpoint, []byte("unexpected"), nil))
			case "manifest":
				require.NoError(t, b.bucket.WriteAll(t.Context(), path.Join(b.journalDir(ref), "manifest.json"), []byte("{"), nil))
			case "entry":
				key := path.Join(b.journalDir(ref), m.Epoch, "entries", "bad.journal.json")
				require.NoError(t, b.bucket.WriteAll(t.Context(), key, []byte("{"), nil))
			case "reference":
				missing := int64(100)
				writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
					Version: 1, SequenceID: 1, Kind: apitype.JournalEntryKindOutputs,
					RemoveNew: &missing, State: journalResource(),
				})
			case "version":
				writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
					Version: 999, SequenceID: 1, Kind: apitype.JournalEntryKindBegin,
				})
			}
			before, err := b.bucket.ReadAll(t.Context(), m.Checkpoint)
			require.NoError(t, err)
			require.Error(t, b.RecoverJournal(t.Context(), ref))
			after, err := b.bucket.ReadAll(t.Context(), m.Checkpoint)
			require.NoError(t, err)
			require.Equal(t, before, after)
			exists, err := b.bucket.Exists(t.Context(), path.Join(b.journalDir(ref), "manifest.json"))
			require.NoError(t, err)
			require.True(t, exists)
		})
	}
}

type journalFailBucket struct {
	Bucket
	mu   sync.Mutex
	fail bool
}

func (b *journalFailBucket) WriteAll(ctx context.Context, key string, data []byte, opts *blob.WriterOptions) error {
	b.mu.Lock()
	fail := b.fail
	b.mu.Unlock()
	if fail && strings.HasSuffix(key, ".journal.json") {
		return errors.New("injected entry failure")
	}
	return b.Bucket.WriteAll(ctx, key, data, opts)
}

func TestDIYJournalerAsyncFailure(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	faults := &journalFailBucket{Bucket: b.bucket, fail: true}
	b.bucket = faults
	j := b.newDIYJournaler(t.Context(), ref, m, b64.NewBase64SecretsManager())
	require.NoError(t, j.AddJournalEntry(engine.JournalEntry{
		Kind: engine.JournalEntryBegin, SequenceID: 1, OperationID: 1, ElideWrite: true,
	}))
	require.ErrorContains(t, j.Close(), "injected entry failure")
	require.ErrorContains(t, j.Close(), "injected entry failure")
	require.Error(t, j.AddJournalEntry(engine.JournalEntry{}))
}

func TestDIYJournalPendingOperation(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
		Version: 1, SequenceID: 1, OperationID: 1, Kind: apitype.JournalEntryKindBegin,
		Operation: &apitype.OperationV2{Resource: *journalResource(), Type: apitype.OperationTypeCreating},
	})
	require.NoError(t, b.RecoverJournal(t.Context(), ref))
	cp, _, _, err := b.getCheckpoint(t.Context(), ref)
	require.NoError(t, err)
	require.Len(t, cp.Latest.PendingOperations, 1)
}

func TestDIYJournalConditionalWrite(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	key := b.stackPath(t.Context(), ref)
	before, err := b.bucket.ReadAll(t.Context(), key)
	require.NoError(t, err)
	require.ErrorContains(t, b.journalWrite(t.Context(), key, []byte("bad"), "wrong"), "changed unexpectedly")
	after, err := b.bucket.ReadAll(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestDIYJournalMissingManifestRejectsGuardedCheckpoint(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	_, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	require.NoError(t, b.bucket.Delete(t.Context(), path.Join(b.journalDir(ref), "manifest.json")))
	_, _, _, err = b.getCheckpoint(t.Context(), ref)
	require.ErrorContains(t, err, diyJournalFeature)
}

func TestDIYJournalConcurrentEntries(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	b.Env = env.NewEnv(env.MapStore{env.DIYBackendParallel.Var().Name(): "16"})
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	j := b.newDIYJournaler(t.Context(), ref, m, b64.NewBase64SecretsManager())
	var writes sync.WaitGroup
	for i := int64(1); i <= 120; i++ {
		writes.Go(func() {
			require.NoError(t, j.AddJournalEntry(engine.JournalEntry{
				Kind: engine.JournalEntryBegin, SequenceID: i, OperationID: i, ElideWrite: true,
			}))
		})
	}
	writes.Wait()
	require.NoError(t, j.Close())
	entries, err := b.readJournalEntries(t.Context(), j.dir)
	require.NoError(t, err)
	require.Len(t, entries, 120)
	for i, entry := range entries {
		require.Equal(t, int64(i+1), entry.SequenceID)
	}
	require.NoError(t, b.RecoverJournal(t.Context(), ref))
}
