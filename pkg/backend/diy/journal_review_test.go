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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
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
	require.ErrorContains(t, b.RecoverJournal(t.Context(), ref), "snapshot integrity failure")
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

func journalMigrationEntry(sequence int64, baseRetained []int64, inserted ...string) apitype.JournalEntry {
	entry := apitype.JournalEntry{Version: 2, SequenceID: sequence, Kind: apitype.JournalEntryKindStateMigration}
	for i := range baseRetained {
		entry.Layout = append(entry.Layout, apitype.JournalLayoutItem{BaseIndex: &baseRetained[i]})
	}
	for i, name := range inserted {
		index := int64(i)
		entry.Layout = append(entry.Layout, apitype.JournalLayoutItem{StateIndex: &index})
		state := *journalResource()
		state.URN = resource.URN("urn:pulumi:teststack::testproject::test:index:Resource::" + name)
		entry.States = append(entry.States, state)
	}
	return entry
}

func TestDIYJournalStateMigrationUpdatesBaseCount(t *testing.T) {
	t.Parallel()
	removeOld := func(sequence, index int64) apitype.JournalEntry {
		return apitype.JournalEntry{
			Version: 1, SequenceID: sequence, OperationID: sequence, Kind: apitype.JournalEntryKindSuccess,
			RemoveOld: &index,
		}
	}

	t.Run("growth", func(t *testing.T) {
		t.Parallel()
		b, ref := newDIYJournalTest(t, false)
		m, err := b.startJournal(t.Context(), ref)
		require.NoError(t, err)
		writeDIYTestEntry(t, b, ref, m, journalMigrationEntry(1, nil, "a", "b"))
		// Index 1 only exists in the base produced by the migration.
		writeDIYTestEntry(t, b, ref, m, removeOld(2, 1))
		require.NoError(t, b.RecoverJournal(t.Context(), ref))
		cp, _, _, err := b.getCheckpoint(t.Context(), ref)
		require.NoError(t, err)
		require.Len(t, cp.Latest.Resources, 1)
		require.Contains(t, string(cp.Latest.Resources[0].URN), "::a")
	})

	t.Run("shrink", func(t *testing.T) {
		t.Parallel()
		b, ref := newDIYJournalTest(t, false)
		m, err := b.startJournal(t.Context(), ref)
		require.NoError(t, err)
		writeDIYTestEntry(t, b, ref, m, journalMigrationEntry(1, nil, "a", "b", "c"))
		// The second migration keeps base 0 and inserts one state, so the base shrinks from 3 to 2 resources.
		writeDIYTestEntry(t, b, ref, m, journalMigrationEntry(2, []int64{0}, "d"))
		writeDIYTestEntry(t, b, ref, m, removeOld(3, 2))
		require.ErrorContains(t, b.RecoverJournal(t.Context(), ref), "invalid base index")
	})
}

const journalV0Checkpoint = `{"stack":"organization/testproject/teststack","latest":{` +
	`"manifest":{"time":"2020-01-01T00:00:00Z","magic":"","version":""},` +
	`"resources":[{"urn":"urn:pulumi:teststack::testproject::test:index:Resource::legacy",` +
	`"type":"test:index:Resource","custom":false}]}}`

func TestDIYJournalPreV3CheckpointsSurviveStartDiscardAndRecover(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"discard", "recover"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b, ref := newDIYJournalTest(t, false)
			key := b.stackPath(t.Context(), ref)
			// A checkpoint from before versioned wrappers have no "version" or "checkpoint" fields.
			require.NoError(t, b.bucket.WriteAll(t.Context(), key, []byte(journalV0Checkpoint), nil))

			_, err := b.startJournal(t.Context(), ref)
			require.NoError(t, err)
			if name == "discard" {
				require.NoError(t, b.DiscardJournal(t.Context(), ref))
			} else {
				require.NoError(t, b.RecoverJournal(t.Context(), ref))
			}

			cp, _, _, err := b.getCleanCheckpoint(t.Context(), ref)
			require.NoError(t, err)
			require.Len(t, cp.Latest.Resources, 1)
			require.Contains(t, string(cp.Latest.Resources[0].URN), "::legacy")
		})
	}
}

func TestDIYJournalImportAfterRecoveryDoesNotOverwrite(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	m, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
		Version: 1, SequenceID: 1, OperationID: 1, Kind: apitype.JournalEntryKindSuccess, State: journalResource(),
	})

	// A state edit reads the non-strict base, which is missing the crashed run's resource.
	s := newStack(ref, b)
	stale, err := s.Snapshot(t.Context(), nil)
	require.NoError(t, err)
	// The base has no deployment yet, so there is no snapshot and certainly no journaled resource.
	require.Nil(t, stale)
	staleDeployment := &apitype.UntypedDeployment{
		Version: 3,
		Deployment: json.RawMessage(
			`{"manifest":{"time":"2020-01-01T00:00:00Z","magic":"","version":""},"resources":[]}`),
	}

	err = b.ImportDeployment(t.Context(), s, staleDeployment)
	require.ErrorContains(t, err, "was recovered and its state changed")
	cp, _, _, err := b.getCleanCheckpoint(t.Context(), ref)
	require.NoError(t, err)
	require.Len(t, cp.Latest.Resources, 1, "the recovered resource must survive the refused import")

	// With nothing left to recover, the same import now applies.
	require.NoError(t, b.ImportDeployment(t.Context(), s, staleDeployment))
	cp, _, _, err = b.getCleanCheckpoint(t.Context(), ref)
	require.NoError(t, err)
	require.Empty(t, cp.Latest.Resources)
}

func TestDIYJournalRecoveryRefusesStaleCompressionVariant(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"guarded", "base"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b, ref := newDIYJournalTest(t, false)
			plain := b.stackPath(t.Context(), ref)
			baseBytes, err := b.bucket.ReadAll(t.Context(), plain)
			require.NoError(t, err)

			m, err := b.startJournal(t.Context(), ref)
			require.NoError(t, err)
			require.Equal(t, plain, m.Checkpoint)
			if name == "guarded" {
				writeDIYTestEntry(t, b, ref, m, apitype.JournalEntry{
					Version: 1, SequenceID: 1, OperationID: 1, Kind: apitype.JournalEntryKindSuccess,
					State: journalResource(),
				})
			} else {
				// An epoch whose checkpoint still equals its base takes the clear-journal branch of consolidation.
				require.NoError(t, b.bucket.WriteAll(t.Context(), plain, baseBytes, nil))
			}

			// Another writer publishes a newer compressed checkpoint and leaves the older object behind.
			var compressed bytes.Buffer
			zw := gzip.NewWriter(&compressed)
			_, err = zw.Write(baseBytes)
			require.NoError(t, err)
			require.NoError(t, zw.Close())
			newer := plain + ".gz"
			require.NoError(t, b.bucket.WriteAll(t.Context(), newer, compressed.Bytes(), nil))
			future := time.Now().Add(time.Hour)
			root, _, _ := strings.Cut(strings.TrimPrefix(b.url, "file://"), "?")
			require.NoError(t, os.Chtimes(filepath.Join(root, filepath.FromSlash(newer)), future, future))
			require.Equal(t, newer, b.stackPath(t.Context(), ref))

			err = b.RecoverJournal(t.Context(), ref)
			require.ErrorContains(t, err, "is not the current checkpoint")
			err = b.ImportDeployment(t.Context(), newStack(ref, b), &apitype.UntypedDeployment{
				Version: 3,
				Deployment: json.RawMessage(
					`{"manifest":{"time":"2020-01-01T00:00:00Z","magic":"","version":""},"resources":[]}`),
			})
			require.ErrorContains(t, err, "is not the current checkpoint")

			after, err := b.bucket.ReadAll(t.Context(), newer)
			require.NoError(t, err)
			require.Equal(t, compressed.Bytes(), after, "the newer checkpoint must be left alone")
			require.Equal(t, newer, b.stackPath(t.Context(), ref))
			kept, err := b.journalManifest(t.Context(), ref)
			require.NoError(t, err)
			require.NotNil(t, kept, "the journal must be preserved")
		})
	}
}

func TestDIYJournalGuardWithoutManifestPointsToDiscard(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	_, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)
	require.NoError(t, b.bucket.Delete(t.Context(), path.Join(b.journalDir(ref), "manifest.json")))

	// With no manifest RecoverJournal has nothing to replay and says nothing, but the guard remains.
	require.NoError(t, b.RecoverJournal(t.Context(), ref))
	for name, err := range map[string]error{
		"verify": b.VerifyJournalRecovered(t.Context(), ref),
		"export": func() error { _, err := b.ExportDeployment(t.Context(), newStack(ref, b)); return err }(),
		"start":  func() error { _, err := b.startJournal(t.Context(), ref); return err }(),
	} {
		require.ErrorContains(t, err, "journal discard", name)
	}
}

func TestDIYJournalStrictReadDuringLiveOperationSaysWait(t *testing.T) {
	t.Parallel()
	b, ref := newDIYJournalTest(t, false)
	_, err := b.startJournal(t.Context(), ref)
	require.NoError(t, err)

	// Another process holds the stack: its lock file is not ours.
	lock, err := json.Marshal(lockContent{Pid: 1, Username: "other", Hostname: "elsewhere"})
	require.NoError(t, err)
	lockKey := path.Join(stackLockDir(ref.FullyQualifiedName()), "other.json")
	require.NoError(t, b.bucket.WriteAll(t.Context(), lockKey, lock, nil))

	_, err = b.ExportDeployment(t.Context(), newStack(ref, b))
	require.ErrorContains(t, err, "wait for the running operation to finish")
	require.ErrorContains(t, err, "journal recover")
	require.ErrorContains(t, err, "locked by 1 lock")

	// Once the other process is gone the ordinary advice applies.
	require.NoError(t, b.bucket.Delete(t.Context(), lockKey))
	_, err = b.ExportDeployment(t.Context(), newStack(ref, b))
	require.ErrorContains(t, err, "journal recover")
	require.NotContains(t, err.Error(), "wait for the running operation")
}

// journalSubsetBucket persists only the writes whose role is in allowed and fails the rest as if the process had died
// before issuing them. startJournal runs its backup and its base-then-manifest chain concurrently, so a crash can leave
// any subset that respects those dependencies; this lets a test construct each one exactly.
type journalSubsetBucket struct {
	Bucket
	allowed map[string]bool
}

func journalWriteRole(key string) string {
	switch {
	case strings.HasSuffix(key, ".bak"):
		return "backup"
	case strings.HasSuffix(key, "/base"):
		return "base"
	case strings.HasSuffix(key, "manifest.json"):
		return "manifest"
	default:
		return "guard"
	}
}

func (b *journalSubsetBucket) WriteAll(ctx context.Context, key string, data []byte, opts *blob.WriterOptions) error {
	if !b.allowed[journalWriteRole(key)] {
		return errSimulatedCrash
	}
	return b.Bucket.WriteAll(ctx, key, data, opts)
}

func TestDIYJournalStartCrashLeavesRecoverableSubsets(t *testing.T) {
	t.Parallel()
	// Every state reachable by a crash in startJournal: the manifest needs the base, and the guard needs all three.
	subsets := [][]string{
		{},
		{"backup"},
		{"base"},
		{"backup", "base"},
		{"base", "manifest"},
		{"backup", "base", "manifest"},
	}
	for _, subset := range subsets {
		name := strings.Join(subset, "+")
		if name == "" {
			name = "none"
		}
		t.Run(name+"/crash", func(t *testing.T) {
			t.Parallel()
			b, ref := newDIYJournalTest(t, false)
			original := b.bucket
			allowed := map[string]bool{}
			for _, role := range subset {
				allowed[role] = true
			}
			b.bucket = &journalSubsetBucket{Bucket: original, allowed: allowed}
			_, err := b.startJournal(t.Context(), ref)
			require.ErrorIs(t, err, errSimulatedCrash)
			b.bucket = original

			require.NoError(t, b.RecoverJournal(t.Context(), ref))
			require.NoError(t, b.RecoverJournal(t.Context(), ref))
			require.NoError(t, b.requireCleanJournal(t.Context(), ref))
			_, _, features, err := b.getCleanCheckpoint(t.Context(), ref)
			require.NoError(t, err)
			require.NotContains(t, features, diyJournalFeature)
			// The stack must still accept a new journaled run after recovery.
			_, err = b.startJournal(t.Context(), ref)
			require.NoError(t, err)
		})
	}
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
			original, err := b.bucket.ReadAll(t.Context(), b.stackPath(t.Context(), ref))
			require.NoError(t, err)
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
			if corrupt {
				// Without a valid manifest the base is unknown, so only the temporary feature is removed from the guard.
				require.Equal(t, old.Checkpoint, clean.Checkpoint)
			} else {
				// With a valid manifest discard restores the exact pre-run checkpoint.
				require.Equal(t, original, after)
			}
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
