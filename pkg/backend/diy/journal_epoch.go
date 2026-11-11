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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/gofrs/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/encoding"
)

type journalManifest struct {
	Version    int    `json:"version"`
	Epoch      string `json:"epoch"`
	Stack      string `json:"stack"`
	Checkpoint string `json:"checkpoint"`
	BaseHash   string `json:"baseHash"`
	GuardHash  string `json:"guardHash"`
}

func (b *diyBackend) journalManifest(ctx context.Context, ref *diyBackendReference) (*journalManifest, error) {
	data, err := b.journalRead(ctx, path.Join(b.journalDir(ref), "manifest.json"))
	if err != nil || data == nil {
		return nil, err
	}
	var m journalManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("invalid journal manifest: %w", err)
	}
	if m.Version == 1 {
		return nil, errors.New("v1 journal: recover with the CLI that created this legacy epoch first")
	}
	if _, err := uuid.FromString(m.Epoch); err != nil {
		return nil, errors.New("invalid journal epoch")
	}
	validHash := func(hash string) bool {
		decoded, err := hex.DecodeString(hash)
		return err == nil && len(decoded) == 32
	}
	if m.Version != 2 || m.Stack != string(ref.FullyQualifiedName()) ||
		!validHash(m.BaseHash) || !validHash(m.GuardHash) || m.Checkpoint != b.stackPath(ctx, ref) {
		return nil, errors.New("journal manifest identity, version or checkpoint mismatch; preserving journal")
	}
	return &m, nil
}

func (b *diyBackend) requireCleanJournal(ctx context.Context, ref *diyBackendReference) error {
	m, err := b.journalManifest(ctx, ref)
	if err != nil {
		return err
	}
	if m != nil {
		return fmt.Errorf("unfinished journal for %s; run pulumi state journal recover --stack %s", ref, ref)
	}
	return nil
}

func (b *diyBackend) journalBase(ctx context.Context, ref *diyBackendReference, m *journalManifest) ([]byte, error) {
	base, err := b.bucket.ReadAll(ctx, path.Join(b.journalDir(ref), m.Epoch, "base"))
	if err != nil {
		return nil, err
	}
	if journalHash(base) != m.BaseHash {
		return nil, errors.New("journal base hash mismatch; preserving epoch")
	}
	return base, nil
}

// startJournal persists the base and manifest, backs up state, then installs the checkpoint guard.
func (b *diyBackend) startJournal(ctx context.Context, ref *diyBackendReference) (*journalManifest, error) {
	if !strings.HasPrefix(b.url, "gs://") && !strings.HasPrefix(b.url, "file://") &&
		!strings.HasPrefix(b.url, "s3://") && !strings.HasPrefix(b.url, "azblob://") &&
		!strings.HasPrefix(b.url, "postgres://") {
		return nil, errors.New("DIY journaling requires a file, GCS, S3, Azure Blob or PostgreSQL backend")
	}
	if err := b.requireCleanJournal(ctx, ref); err != nil {
		return nil, err
	}
	key := b.stackPath(ctx, ref)
	base, err := b.bucket.ReadAll(ctx, key)
	if err != nil {
		return nil, err
	}
	cp, err := decodeJournalCheckpoint(base)
	if err != nil {
		return nil, err
	}
	if slices.Contains(cp.Features, diyJournalFeature) {
		return nil, errors.New("guarded checkpoint has no journal manifest")
	}
	if encoding.DetectCompression(base) != b.compression {
		if _, _, err := b.saveCheckpoint(ctx, ref, cp); err != nil {
			return nil, err
		}
		key = b.stackPath(ctx, ref)
		base, err = b.bucket.ReadAll(ctx, key)
		if err != nil {
			return nil, err
		}
		cp, err = decodeJournalCheckpoint(base)
		if err != nil {
			return nil, err
		}
	}
	cp.Version = apitype.DeploymentSchemaVersionLatest
	cp.Features = append(cp.Features, diyJournalFeature)
	guard, err := encodeJournalCheckpoint(cp, base)
	if err != nil {
		return nil, err
	}
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	m := &journalManifest{
		Version: 2, Epoch: id.String(), Stack: string(ref.FullyQualifiedName()),
		Checkpoint: key, BaseHash: journalHash(base), GuardHash: journalHash(guard),
	}
	if err := b.journalPut(ctx, path.Join(b.journalDir(ref), m.Epoch, "base"), base); err != nil {
		return nil, err
	}
	data, err := journalJSON(m)
	if err != nil {
		return nil, err
	}
	if err := b.journalPut(ctx, path.Join(b.journalDir(ref), "manifest.json"), data); err != nil {
		return nil, err
	}
	if err := b.bucket.Copy(ctx, key+".bak", key, nil); err != nil {
		return nil, err
	}
	if err := b.journalSync(key + ".bak"); err != nil {
		return nil, err
	}
	if err := b.journalWrite(ctx, key, guard, m.BaseHash); err != nil {
		return nil, err
	}
	return m, nil
}

// readJournalEntries reads the contiguous submitted prefix and sorts it by engine sequence for replay.
func (b *diyBackend) readJournalEntries(ctx context.Context, dir string) ([]apitype.JournalEntry, error) {
	files, err := listBucket(ctx, b.bucket, dir)
	if err != nil {
		return nil, err
	}
	prefix := 0
	for i, f := range files {
		name := path.Base(f.Key)
		index, err := strconv.ParseUint(strings.TrimSuffix(name, ".journal.json"), 10, 64)
		if err != nil || index == 0 || f.IsDir || name != fmt.Sprintf("%020d.journal.json", index) {
			return nil, errors.New("unexpected journal object; preserving epoch")
		}
		if index == uint64(i+1) && prefix == i {
			prefix++
		}
	}
	entries := make([]apitype.JournalEntry, prefix)
	group, readCtx := errgroup.WithContext(ctx)
	group.SetLimit(b.getParallel())
	for i, f := range files[:prefix] {
		group.Go(func() error {
			data, err := b.bucket.ReadAll(readCtx, f.Key)
			if err != nil {
				return err
			}
			var entry apitype.JournalEntry
			if err := json.Unmarshal(data, &entry); err != nil {
				return err
			}
			if entry.SequenceID <= 0 {
				return errors.New("invalid journal sequence")
			}
			entries[i] = entry
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	seen := map[int64]bool{}
	for _, entry := range entries {
		if seen[entry.SequenceID] {
			return nil, errors.New("duplicate journal sequence")
		}
		seen[entry.SequenceID] = true
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].SequenceID < entries[j].SequenceID })
	return entries, nil
}

func validateDIYJournalEntry(e apitype.JournalEntry, baseCount int, newStates map[int64]bool) error {
	if e.Kind == apitype.JournalEntryKindSuccess && e.State != nil {
		newStates[e.OperationID] = true
	}
	for _, index := range []*int64{e.RemoveOld, e.DeleteOld, e.PendingReplacementOld} {
		if index != nil && (*index < 0 || *index >= int64(baseCount)) {
			return errors.New("journal references invalid base index")
		}
	}
	for _, id := range []*int64{e.RemoveNew, e.DeleteNew, e.PendingReplacementNew} {
		if id != nil && !newStates[*id] {
			return errors.New("journal references missing resource operation")
		}
	}
	if e.Kind == apitype.JournalEntryKindSecretsManager && e.SecretsProvider == nil {
		return errors.New("journal secret provider missing")
	}
	if e.Kind == apitype.JournalEntryKindWrite && e.NewSnapshot == nil {
		return errors.New("journal base snapshot missing")
	}
	if e.Kind == apitype.JournalEntryKindExtensionParameterize && (e.Extension == nil || e.ExtensionRef == nil) {
		return errors.New("journal extension missing")
	}
	return nil
}
