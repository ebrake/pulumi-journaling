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
	"io"
	"path"
	"slices"
	"strings"

	"github.com/gofrs/uuid"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/pulumi/pulumi/pkg/v3/backend"
)

// DiscardJournal archives journal evidence, removes only the checkpoint guard and clears the active epoch.
// It deliberately does not replay entries and may lose changes from the interrupted operation.
func (b *diyBackend) DiscardJournal(ctx context.Context, stackRef backend.StackReference) error {
	ref, err := b.getReference(stackRef)
	if err != nil {
		return err
	}
	if err := b.Lock(ctx, ref); err != nil {
		return err
	}
	defer b.Unlock(ctx, ref)
	dir := b.journalDir(ref)
	manifestKey := path.Join(dir, "manifest.json")
	manifest, err := b.journalRead(ctx, manifestKey)
	if err != nil {
		return err
	}
	key := b.stackPath(ctx, ref)
	current, err := b.bucket.ReadAll(ctx, key)
	if err != nil {
		if manifest == nil && gcerrors.Code(err) == gcerrors.NotFound {
			return nil
		}
		return err
	}
	cp, decodeErr := decodeJournalCheckpoint(current)
	guarded := decodeErr == nil && slices.Contains(cp.Features, diyJournalFeature)
	if manifest == nil && !guarded {
		return decodeErr
	}
	// Invalid metadata must not prevent an explicit evidence-preserving discard.
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}
	epoch := id.String()
	var m journalManifest
	validEpoch := false
	if json.Unmarshal(manifest, &m) == nil {
		if _, err := uuid.FromString(m.Epoch); err == nil {
			epoch = m.Epoch
			validEpoch = true
		}
	}
	archive := path.Join(ref.BackupDir(), "journal-discarded", epoch)
	if err := b.journalPut(ctx, path.Join(archive, "checkpoints", journalHash(current)), current); err != nil {
		return err
	}
	iterator := b.bucket.List(&blob.ListOptions{Prefix: dir + "/"})
	var keys []string
	for {
		obj, err := iterator.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		data, err := b.readJournalObject(ctx, obj.Key)
		if err != nil {
			return err
		}
		if err := b.journalPut(ctx, path.Join(archive, strings.TrimPrefix(obj.Key, dir+"/")), data); err != nil {
			return err
		}
		keys = append(keys, obj.Key)
	}
	if guarded {
		var clean []byte
		if validEpoch && m.Checkpoint == key {
			// The base is the exact pre-run checkpoint, so restoring it avoids re-encoding a shallow decode of the
			// guard, which cannot represent checkpoints older than v3. journalBase verifies its hash.
			clean, _ = b.journalBase(ctx, ref, &m)
		}
		if clean == nil {
			// Without a verified base, the best remaining option is to drop only the temporary feature.
			cp.Features = slices.DeleteFunc(cp.Features, func(f string) bool { return f == diyJournalFeature })
			clean, err = encodeJournalCheckpoint(cp, current)
			if err != nil {
				return err
			}
		}
		if err := b.journalWrite(ctx, key, clean, journalHash(current)); err != nil {
			return err
		}
	}
	for _, key := range keys {
		if key != manifestKey {
			if err := b.journalDelete(ctx, key); err != nil {
				return err
			}
		}
	}
	if manifest != nil {
		return b.journalDelete(ctx, manifestKey)
	}
	return nil
}
