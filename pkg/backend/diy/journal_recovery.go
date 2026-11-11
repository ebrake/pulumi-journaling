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
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"

	"gocloud.dev/gcerrors"
	"golang.org/x/sync/errgroup"

	"github.com/pulumi/pulumi/pkg/v3/backend"
	"github.com/pulumi/pulumi/pkg/v3/resource/stack"
	"github.com/pulumi/pulumi/pkg/v3/resource/stack/snapshot"
	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/encoding"
	utilenv "github.com/pulumi/pulumi/sdk/v3/go/common/util/env"
)

func (b *diyBackend) prepareJournal(ctx context.Context, ref *diyBackendReference, m *journalManifest) ([]byte, error) {
	key := path.Join(b.journalDir(ref), m.Epoch, "clean")
	baseBytes, err := b.journalBase(ctx, ref, m)
	if err != nil {
		return nil, err
	}
	base, _, features, err := stack.UnmarshalVersionedCheckpointToLatestCheckpoint(
		encoding.Compress(encoding.JSON, encoding.DetectCompression(baseBytes)), baseBytes)
	if err != nil {
		return nil, err
	}
	if base.Latest == nil {
		base.Latest = &apitype.DeploymentV3{}
	}
	entries, err := b.readJournalEntries(ctx, path.Join(b.journalDir(ref), m.Epoch, "entries"))
	if err != nil {
		return nil, err
	}
	replayer := backend.NewJournalReplayer(base.Latest)
	baseCount := len(base.Latest.Resources)
	newStates := map[int64]bool{}
	for _, e := range entries {
		if err := validateDIYJournalEntry(e, baseCount, newStates); err != nil {
			return nil, err
		}
		if err := replayer.Add(e); err != nil {
			return nil, fmt.Errorf("replaying journal sequence %d: %w", e.SequenceID, err)
		}
		if e.Kind == apitype.JournalEntryKindWrite {
			baseCount = len(e.NewSnapshot.Resources)
		}
		if e.Kind == apitype.JournalEntryKindRebuiltBaseState {
			rebuilt, err := replayer.GenerateDeployment()
			if err != nil {
				return nil, err
			}
			baseCount = len(rebuilt.Deployment.Resources)
			newStates = map[int64]bool{}
		}
	}
	deployment, err := replayer.GenerateDeployment()
	if err != nil {
		return nil, err
	}
	integrityErr := snapshot.VerifyIntegrity(deployment.Deployment)
	deployment.Deployment.Metadata.IntegrityErrorMetadata = nil
	if integrityErr != nil {
		deployment.Deployment.Metadata.IntegrityErrorMetadata = &apitype.SnapshotIntegrityErrorMetadataV1{
			Version: strconv.FormatInt(int64(deployment.Version), 10), Command: strings.Join(os.Args, " "),
			Error: integrityErr.Error(), EnvVars: utilenv.ConfiguredVariables(),
		}
	}
	for _, f := range features {
		if !slices.Contains(deployment.Features, f) {
			deployment.Features = append(deployment.Features, f)
		}
	}
	if len(deployment.Features) > 0 {
		deployment.Version = apitype.DeploymentSchemaVersionLatest
	}
	cp, err := stack.DeploymentV3ToCheckpointWithMarshaler(diyJSONMarshaler, ref.FullyQualifiedName(),
		deployment.Deployment, deployment.Version, deployment.Features)
	if err != nil {
		return nil, err
	}
	clean, err := encodeJournalCheckpoint(cp, baseBytes)
	if err != nil {
		return nil, err
	}
	return clean, b.journalPut(ctx, key, clean)
}

// consolidateJournal commits an active epoch while the native stack lock is held.
// A nonempty wantEpoch binds completion to the current run.
func (b *diyBackend) consolidateJournal(ctx context.Context, ref *diyBackendReference, wantEpoch string) error {
	m, err := b.journalManifest(ctx, ref)
	if err != nil {
		return err
	}
	if wantEpoch != "" && (m == nil || m.Epoch != wantEpoch) {
		return fmt.Errorf(
			"journal epoch %s was recovered or replaced by another process; this run's state may be incomplete",
			wantEpoch)
	}
	if m == nil {
		return nil
	}
	current, err := b.bucket.ReadAll(ctx, m.Checkpoint)
	if err != nil {
		return err
	}
	dir := path.Join(b.journalDir(ref), m.Epoch)
	clean, err := b.journalRead(ctx, path.Join(dir, "clean"))
	if err != nil {
		return err
	}
	currentHash := journalHash(current)
	if currentHash == m.BaseHash && clean == nil {
		files, err := listBucket(ctx, b.bucket, path.Join(dir, "entries"))
		if err != nil {
			return err
		}
		if len(files) > 0 {
			return errors.New("journal entries exist without guarded checkpoint; preserving epoch")
		}
		return b.clearJournal(ctx, ref, m)
	}
	if currentHash != m.GuardHash && (clean == nil || currentHash != journalHash(clean)) {
		return errors.New("unexplained checkpoint change; preserving journal")
	}
	if clean == nil {
		clean, err = b.prepareJournal(ctx, ref, m)
		if err != nil {
			return err
		}
	}
	checkpoint, _, features, err := stack.UnmarshalVersionedCheckpointToLatestCheckpoint(
		encoding.Compress(encoding.JSON, encoding.DetectCompression(clean)), clean)
	if err != nil {
		return err
	}
	if slices.Contains(features, diyJournalFeature) {
		return errors.New("prepared checkpoint is guarded; preserving journal")
	}
	integrityErr := snapshot.VerifyIntegrity(checkpoint.Latest)
	if currentHash == m.GuardHash {
		if err := b.journalWrite(ctx, m.Checkpoint, clean, m.GuardHash); err != nil {
			return err
		}
	}
	if err := b.retainCheckpoint(ctx, m.Checkpoint, clean); err != nil {
		return err
	}
	if err := b.clearJournal(ctx, ref, m); err != nil {
		return err
	}
	if !backend.DisableIntegrityChecking && integrityErr != nil {
		return fmt.Errorf("snapshot integrity failure; it was already written, but is invalid: %w", integrityErr)
	}
	return nil
}

func (b *diyBackend) clearJournal(ctx context.Context, ref *diyBackendReference, m *journalManifest) error {
	dir := path.Join(b.journalDir(ref), m.Epoch)
	files, err := listBucket(ctx, b.bucket, path.Join(dir, "entries"))
	if err != nil {
		return err
	}
	var group errgroup.Group
	group.SetLimit(b.getParallel())
	for _, f := range files {
		group.Go(func() error { return b.journalDelete(ctx, f.Key) })
	}
	if err := group.Wait(); err != nil {
		return err
	}
	// Keep recovery bytes until the active manifest is gone. Later crashes leave only inert artifacts.
	if err := b.journalDelete(ctx, path.Join(b.journalDir(ref), "manifest.json")); err != nil {
		return err
	}
	return errors.Join(b.journalDelete(ctx, path.Join(dir, "base")), b.journalDelete(ctx, path.Join(dir, "clean")))
}

func (b *diyBackend) journalDelete(ctx context.Context, key string) error {
	if err := b.bucket.Delete(ctx, key); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
		return err
	}
	if strings.HasPrefix(b.url, "file://") {
		// Sync the containing directory without requiring the deleted file to exist.
		return b.journalSyncDirectory(key)
	}
	return nil
}

// RecoverJournal consolidates unfinished journal state without invoking providers.
func (b *diyBackend) RecoverJournal(ctx context.Context, stackRef backend.StackReference) error {
	ref, err := b.getReference(stackRef)
	if err != nil {
		return err
	}
	m, err := b.journalManifest(ctx, ref)
	if err != nil || m == nil {
		return err
	}
	if err := b.Lock(ctx, ref); err != nil {
		return err
	}
	defer b.Unlock(ctx, ref)
	return b.consolidateJournal(ctx, ref, "")
}

func (b *diyBackend) recoverJournalForMutation(ctx context.Context, stackRef backend.StackReference) error {
	ref, err := b.getReference(stackRef)
	if err != nil {
		return err
	}
	return b.consolidateJournal(ctx, ref, "")
}
