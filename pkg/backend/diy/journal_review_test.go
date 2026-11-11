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
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/pulumi/pulumi/pkg/v3/backend"
	"github.com/pulumi/pulumi/pkg/v3/engine"
	"github.com/pulumi/pulumi/pkg/v3/secrets/b64"
	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/env"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
)

func TestDIYJournalRecoveryIgnoresTailAfterGap(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	child := journalResource()
	child.Parent = resource.URN("urn:pulumi:teststack::testproject::test:index:Resource::missing")
	writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
		Version: 1, SequenceID: 2, OperationID: 1, Kind: apitype.JournalEntryKindSuccess, State: child,
	})
	require.NoError(t, b.RecoverJournal(t.Context(), ref))
	cp, _, _, err := b.getCheckpoint(t.Context(), ref)
	require.NoError(t, err)
	require.Empty(t, cp.Latest.Resources)
}

func TestDIYJournalReadersSeeBase(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	_, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	_, _, _, err = b.getCheckpoint(t.Context(), ref)
	require.NoError(t, err)
	s := newStack(ref, b)
	_, err = s.Snapshot(t.Context(), nil)
	require.NoError(t, err)
	_, err = s.SnapshotStackOutputs(t.Context(), nil)
	require.NoError(t, err)
	summaries, _, err := b.ListStacks(t.Context(), backend.ListStacksFilter{}, nil)
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	_, err = b.ExportDeployment(t.Context(), s)
	require.ErrorContains(t, err, "journal recover")
}

func TestDIYJournalIntegrityFailureStillCommits(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	child := journalResource()
	child.Parent = resource.URN("urn:pulumi:teststack::testproject::test:index:Resource::missing")
	writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
		Version: 1, SequenceID: 1, OperationID: 1, Kind: apitype.JournalEntryKindSuccess, State: child,
	})
	require.Error(t, b.RecoverJournal(t.Context(), ref))
	require.NoError(t, b.requireCleanJournal(t.Context(), ref))
	cp, _, _, err := b.getCheckpoint(t.Context(), ref)
	require.NoError(t, err)
	require.NotNil(t, cp.Latest.Metadata.IntegrityErrorMetadata)
}

func TestDIYJournalStartBacksUpBase(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, true)
	key := b.stackPath(t.Context(), ref)
	base, err := b.bucket.ReadAll(t.Context(), key)
	require.NoError(t, err)
	_, err = b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	backup, err := b.bucket.ReadAll(t.Context(), key+".bak")
	require.NoError(t, err)
	require.Equal(t, base, backup)
}

func TestDIYJournalV1RequiresPreviousCLI(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	m.Version = 1
	data, err := journalJSON(m)
	require.NoError(t, err)
	require.NoError(t, b.bucket.WriteAll(t.Context(), path.Join(b.journalDir(ref), "manifest.json"), data, nil))
	require.ErrorContains(t, b.RecoverJournal(t.Context(), ref), "the CLI that created this legacy epoch")
}

type journalGateBucket struct {
	Bucket
	first chan struct{}
	later chan struct{}
}

func (b *journalGateBucket) WriteAll(ctx context.Context, key string, data []byte, opts *blob.WriterOptions) error {
	if strings.HasSuffix(key, "/00000000000000000001.journal.json") {
		select {
		case <-b.first:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := b.Bucket.WriteAll(ctx, key, data, opts)
	if strings.HasSuffix(key, "/00000000000000000002.journal.json") {
		close(b.later)
	}
	return err
}

func TestDIYJournalRequiredEntryWaitsForSubmittedPrefix(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	gate := &journalGateBucket{Bucket: b.bucket, first: make(chan struct{}), later: make(chan struct{})}
	b.bucket = gate
	j := b.newDIYJournaler(t.Context(), ref, m, b64.NewBase64SecretsManager())
	// Reuse operation ID zero and reverse engine sequence IDs: neither determines submission order.
	require.NoError(t, j.AddJournalEntry(engine.JournalEntry{
		Kind: engine.JournalEntryBegin, SequenceID: 2, ElideWrite: true,
	}))
	returned := make(chan error, 1)
	go func() {
		returned <- j.AddJournalEntry(engine.JournalEntry{Kind: engine.JournalEntryBegin, SequenceID: 1})
	}()
	select {
	case <-gate.later:
	case <-time.After(5 * time.Second):
		t.Fatal("later write never reached storage")
	}
	select {
	case err := <-returned:
		t.Fatalf("acknowledged past a durability gap: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	entries, err := b.readJournalEntries(t.Context(), j.dir)
	require.NoError(t, err)
	require.Empty(t, entries)
	close(gate.first)
	require.NoError(t, <-returned)
	require.NoError(t, j.Close())
	entries, err = b.readJournalEntries(t.Context(), j.dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, int64(1), entries[0].SequenceID)
}

func TestDIYJournalWaiterCancellation(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	gate := &journalGateBucket{Bucket: b.bucket, first: make(chan struct{}), later: make(chan struct{})}
	b.bucket = gate
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	j := b.newDIYJournaler(ctx, ref, m, b64.NewBase64SecretsManager())
	require.NoError(t, j.AddJournalEntry(engine.JournalEntry{
		Kind: engine.JournalEntryBegin, SequenceID: 1, ElideWrite: true,
	}))
	returned := make(chan error, 1)
	go func() {
		returned <- j.AddJournalEntry(engine.JournalEntry{Kind: engine.JournalEntryBegin, SequenceID: 2})
	}()
	<-gate.later
	cancel()
	require.ErrorIs(t, <-returned, context.Canceled)
	require.ErrorIs(t, j.Close(), context.Canceled)
}

func TestDIYJournalCompletionBindsEpoch(t *testing.T) {
	t.Parallel()
	for _, missing := range []bool{false, true} {
		t.Run(strconv.FormatBool(missing), func(t *testing.T) {
			t.Parallel()
			b, ref := newDIYJournalTest(t, false)
			m, err := b.startJournal(t.Context(), ref)
			require.NoError(t, err)
			key := path.Join(b.journalDir(ref), "manifest.json")
			if missing {
				require.NoError(t, b.bucket.Delete(t.Context(), key))
			} else {
				foreign := *m
				foreign.Epoch = uuid.Must(uuid.NewV4()).String()
				data, err := journalJSON(foreign)
				require.NoError(t, err)
				require.NoError(t, b.bucket.WriteAll(t.Context(), key, data, nil))
			}
			require.ErrorContains(t, b.consolidateJournal(t.Context(), ref, m.Epoch), "recovered or replaced")
			exists, err := b.bucket.Exists(t.Context(), key)
			require.NoError(t, err)
			require.Equal(t, !missing, exists)
		})
	}
}

type journalReadCountBucket struct {
	Bucket
	manifests atomic.Int64
}

func (b *journalReadCountBucket) ReadAll(ctx context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, "/manifest.json") {
		b.manifests.Add(1)
	}
	return b.Bucket.ReadAll(ctx, key)
}

func TestDIYJournalUnguardedReadersSkipManifest(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	counter := &journalReadCountBucket{Bucket: b.bucket}
	b.bucket = counter
	_, _, _, err := b.getCheckpoint(t.Context(), ref)
	require.NoError(t, err)
	s := newStack(ref, b)
	_, err = s.Snapshot(t.Context(), nil)
	require.NoError(t, err)
	_, err = s.Snapshot(t.Context(), nil)
	require.NoError(t, err)
	_, err = s.SnapshotStackOutputs(t.Context(), nil)
	require.NoError(t, err)
	require.Zero(t, counter.manifests.Load())
}

func TestDIYJournalDiscardPreservesEvidence(t *testing.T) {
	t.Parallel()
	for _, corrupt := range []bool{false, true} {
		t.Run(strconv.FormatBool(corrupt), func(t *testing.T) {
			t.Parallel()
			b, ref := newDIYJournalTest(t, true)
			m, err := b.startJournal(t.Context(), ref)
			require.NoError(t, err)
			before, err := b.bucket.ReadAll(t.Context(), m.Checkpoint)
			require.NoError(t, err)
			if corrupt {
				require.NoError(t, b.bucket.WriteAll(t.Context(), path.Join(b.journalDir(ref), "manifest.json"), []byte("{"), nil))
			}
			require.NoError(t, b.DiscardJournal(t.Context(), ref))
			require.NoError(t, b.DiscardJournal(t.Context(), ref))
			require.NoError(t, b.requireCleanJournal(t.Context(), ref))
			after, err := b.bucket.ReadAll(t.Context(), m.Checkpoint)
			require.NoError(t, err)
			old, err := decodeJournalCheckpoint(before)
			require.NoError(t, err)
			clean, err := decodeJournalCheckpoint(after)
			require.NoError(t, err)
			require.NotContains(t, clean.Features, diyJournalFeature)
			require.Equal(t, old.Checkpoint, clean.Checkpoint)
			files, err := listBucket(t.Context(), b.bucket, path.Join(ref.BackupDir(), "journal-discarded"))
			require.NoError(t, err)
			require.NotEmpty(t, files)
		})
	}
}

//nolint:paralleltest // Tests the process-global integrity override.
func TestDIYJournalIntegrityOverride(t *testing.T) {
	backend.DisableIntegrityChecking = true
	t.Cleanup(func() { backend.DisableIntegrityChecking = false })
	b, ref := newDIYJournalTest(t, false)
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	child := journalResource()
	child.Parent = resource.URN("urn:pulumi:teststack::testproject::test:index:Resource::missing")
	writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
		Version: 1, SequenceID: 1, OperationID: 1, Kind: apitype.JournalEntryKindSuccess, State: child,
	})
	require.NoError(t, b.RecoverJournal(t.Context(), ref))
	cp, _, _, err := b.getCheckpoint(t.Context(), ref)
	require.NoError(t, err)
	require.NotNil(t, cp.Latest.Metadata.IntegrityErrorMetadata)
}

func TestDIYJournalRetainsConsolidatedCheckpoint(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	b.Env = env.NewEnv(env.MapStore{env.DIYBackendRetainCheckpoints.Var().Name(): "true"})
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	require.NoError(t, b.consolidateJournal(t.Context(), ref, m.Epoch))
	files, err := listBucket(t.Context(), b.bucket, path.Dir(m.Checkpoint))
	require.NoError(t, err)
	retained := 0
	for _, f := range files {
		suffix := strings.TrimPrefix(f.Key, m.Checkpoint+".")
		if _, err := strconv.ParseInt(suffix, 10, 64); err == nil {
			retained++
		}
	}
	require.Equal(t, 1, retained)
}

func TestDIYJournalDiscardOrphanGuard(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	_, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	require.NoError(t, b.bucket.Delete(t.Context(), path.Join(b.journalDir(ref), "manifest.json")))
	require.NoError(t, b.DiscardJournal(t.Context(), ref))
	_, _, _, err = b.getCleanCheckpoint(t.Context(), ref)
	require.NoError(t, err)
}

func TestDIYJournalDiscardInterruptedCleanup(t *testing.T) {
	t.Parallel()
	for boundary := 1; boundary <= 8; boundary++ {
		t.Run(strconv.Itoa(boundary), func(t *testing.T) {
			t.Parallel()
			b, ref := newDIYJournalTest(t, false)
			m, err := b.startJournal(t.Context(), ref)
			require.NoError(t, err)
			writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
				Version: 1, SequenceID: 1, OperationID: 1, Kind: apitype.JournalEntryKindSuccess, State: journalResource(),
			})
			original := b.bucket
			b.bucket = &journalCrashBucket{Bucket: original, remaining: boundary}
			func() {
				defer func() { _ = recover() }()
				_ = b.DiscardJournal(t.Context(), ref)
			}()
			b.bucket = original
			require.NoError(t, b.DiscardJournal(t.Context(), ref))
			require.NoError(t, b.requireCleanJournal(t.Context(), ref))
			_, _, _, err = b.getCleanCheckpoint(t.Context(), ref)
			require.NoError(t, err)
		})
	}
}

func TestDIYJournalSequenceIDsNeedNotBeContiguous(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	data, err := journalJSON(apitype.JournalEntry{
		Version: 1, SequenceID: 10, OperationID: 1, Kind: apitype.JournalEntryKindSuccess, State: journalResource(),
	})
	require.NoError(t, err)
	key := path.Join(b.journalDir(ref), m.Epoch, "entries", "00000000000000000001.journal.json")
	require.NoError(t, b.bucket.WriteAll(t.Context(), key, data, nil))
	require.NoError(t, b.RecoverJournal(t.Context(), ref))
	cp, _, _, err := b.getCheckpoint(t.Context(), ref)
	require.NoError(t, err)
	require.Len(t, cp.Latest.Resources, 1)
}
