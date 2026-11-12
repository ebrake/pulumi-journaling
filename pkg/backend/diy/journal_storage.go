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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azureblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/pulumi/pulumi/pkg/v3/backend/diy/postgres"
	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/encoding"
)

const diyJournalFeature = "pulumiJournalingV1"

// isStackCheckpointKey reports whether key is one of the checkpoint paths the stack can have. Validating the shape
// avoids the bucket listing stackPath needs, which would otherwise run on every manifest read. The manifest's
// hashes bind it to the exact bytes, so the listing's tie-breaking adds no safety here.
func isStackCheckpointKey(ref *diyBackendReference, key string) bool {
	plain := filepath.ToSlash(ref.StackBasePath()) + ".json"
	return key == plain || key == plain+encoding.GZIPExt || key == plain+encoding.ZSTDExt
}

func journalHash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func (b *diyBackend) journalDir(ref *diyBackendReference) string {
	relative := strings.TrimPrefix(filepath.ToSlash(ref.BackupDir()), filepath.ToSlash(BackupsDir)+"/")
	return path.Join(".pulumi/journal", relative)
}

// readJournalObject reads any object under the journal prefix. The Rapid stream is an unfinalized appendable object
// that ordinary reads cannot return, so it goes through the ranged reader the rest of the code already uses for it.
func (b *diyBackend) readJournalObject(ctx context.Context, key string) ([]byte, error) {
	if b.rapid != nil && strings.HasSuffix(key, "/journal.stream") {
		return b.rapid.readStream(ctx, key)
	}
	return b.bucket.ReadAll(ctx, key)
}

func (b *diyBackend) journalRead(ctx context.Context, key string) ([]byte, error) {
	data, err := b.bucket.ReadAll(ctx, key)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return nil, nil
	}
	return data, err
}

// journalPut writes immutable bytes, checking readback only after an ambiguous failure.
func (b *diyBackend) journalPut(ctx context.Context, key string, data []byte) error {
	return b.journalStore(ctx, key, data, &blob.WriterOptions{IfNotExist: true})
}

// journalWrite conditionally replaces the object version observed by a single checkpoint read.
func (b *diyBackend) journalWrite(ctx context.Context, key string, data []byte, expected string) error {
	reader, err := b.bucket.NewReader(ctx, key, nil)
	var current []byte
	var generation int64
	var etag string
	var databaseVersion postgres.ObjectVersion
	if err == nil {
		var raw *storage.Reader
		if reader.As(&raw) {
			generation = raw.Attrs.Generation
		}
		var s3Object awss3.GetObjectOutput
		if reader.As(&s3Object) && s3Object.ETag != nil {
			etag = *s3Object.ETag
		}
		var azureObject azureblob.DownloadStreamResponse
		if reader.As(&azureObject) && azureObject.ETag != nil {
			etag = string(*azureObject.ETag)
		}
		reader.As(&databaseVersion)
		current, err = io.ReadAll(reader)
		err = errors.Join(err, reader.Close())
	} else if gcerrors.Code(err) == gcerrors.NotFound {
		err = nil
	}
	if err != nil {
		return err
	}
	if bytes.Equal(current, data) {
		return b.journalSync(key)
	}
	if (current == nil && expected != "") || (current != nil && journalHash(current) != expected) {
		return fmt.Errorf("journal checkpoint changed unexpectedly: %s", key)
	}
	opts := &blob.WriterOptions{IfNotExist: expected == ""}
	opts.BeforeWrite = func(as func(any) bool) error {
		var handle **storage.ObjectHandle
		if as(&handle) {
			if expected != "" {
				if generation == 0 {
					return errors.New("GCS generation precondition unavailable")
				}
				*handle = (*handle).If(storage.Conditions{GenerationMatch: generation})
				var rapid *rapidGenerationPrecondition
				if as(&rapid) {
					rapid.Generation = generation
				}
			}
			return nil
		}
		var upload *transfermanager.UploadObjectInput
		if as(&upload) {
			if expected != "" {
				if etag == "" {
					return errors.New("S3 ETag precondition unavailable")
				}
				upload.IfMatch = &etag
			}
			return nil
		}
		var azureUpload *azblob.UploadStreamOptions
		if as(&azureUpload) {
			if expected != "" {
				if etag == "" {
					return errors.New("Azure ETag precondition unavailable")
				}
				tag := azcore.ETag(etag)
				if azureUpload.AccessConditions == nil {
					azureUpload.AccessConditions = &azblob.AccessConditions{}
				}
				azureUpload.AccessConditions.ModifiedAccessConditions = &azureblob.ModifiedAccessConditions{IfMatch: &tag}
			}
			return nil
		}
		var databaseWrite *postgres.WriteConditions
		if as(&databaseWrite) {
			if expected != "" {
				if databaseVersion == "" {
					return errors.New("PostgreSQL version precondition unavailable")
				}
				databaseWrite.IfMatch = &databaseVersion
			}
			return nil
		}
		if !strings.HasPrefix(b.url, "file://") {
			return errors.New("journal object version precondition unavailable")
		}
		var file *os.File
		if !as(&file) {
			return errors.New("journal requires conditional object writes or atomic file storage")
		}
		return nil
	}
	return b.journalStore(ctx, key, data, opts)
}

func (b *diyBackend) journalStore(ctx context.Context, key string, data []byte, opts *blob.WriterOptions) error {
	before := opts.BeforeWrite
	opts.BeforeWrite = func(as func(any) bool) error {
		if before != nil {
			if err := before(as); err != nil {
				return err
			}
		}
		var writer *storage.Writer
		if as(&writer) {
			writer.CRC32C = crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
			writer.SendCRC32C = true
		}
		return nil
	}
	if err := b.bucket.WriteAll(ctx, key, data, opts); err != nil {
		// A timed-out write may have committed. Only identical readback proves success.
		observed, readErr := b.journalRead(ctx, key)
		if readErr != nil || !bytes.Equal(observed, data) {
			return errors.Join(err, readErr)
		}
	}
	return b.journalSync(key)
}

func (b *diyBackend) journalSync(key string) error {
	if !strings.HasPrefix(b.url, "file://") {
		return nil
	}
	u, err := url.Parse(b.url)
	if err != nil {
		return err
	}
	if !filepath.IsLocal(key) {
		return errors.New("invalid local journal key")
	}
	name := filepath.Join(u.Path, filepath.FromSlash(key))
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	return b.journalSyncDirectory(key)
}

func (b *diyBackend) journalSyncDirectory(key string) error {
	u, err := url.Parse(b.url)
	if err != nil {
		return err
	}
	name := filepath.Join(u.Path, filepath.FromSlash(key))
	dir, err := os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func encodeJournalCheckpoint(cp *apitype.VersionedCheckpoint, original []byte) ([]byte, error) {
	return encoding.Compress(diyJSONMarshaler, encoding.DetectCompression(original)).Marshal(cp)
}

func decodeJournalCheckpoint(data []byte) (*apitype.VersionedCheckpoint, error) {
	var cp apitype.VersionedCheckpoint
	err := encoding.Compress(encoding.JSON, encoding.DetectCompression(data)).Unmarshal(data, &cp)
	return &cp, err
}

func journalJSON(v any) ([]byte, error) { return json.Marshal(v) }
