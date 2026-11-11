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

// Modified by E Brake for pulumi-journaling: atomic Rapid GCS persistence.

package diy

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // Compatibility with caller-supplied transport checksums, not cryptography.
	"errors"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"github.com/gofrs/uuid"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"google.golang.org/api/option"

	"github.com/pulumi/pulumi/pkg/v3/authhelpers"
)

func gcsBackendMux(ctx context.Context, u *url.URL) (*blob.URLMux, *storage.Client, error) {
	api, err := gcsBackendAPI(u)
	if err != nil {
		return nil, nil, err
	}
	if api == "" {
		mux, err := authhelpers.GoogleCredentialsMux(ctx)
		return mux, nil, err
	}
	credentials, err := authhelpers.ResolveGoogleCredentials(ctx, storage.ScopeReadWrite)
	if err != nil {
		return nil, nil, err
	}
	options := []option.ClientOption{option.WithTokenSource(credentials.TokenSource)}
	if api == "rapid" {
		options = append(options, experimental.WithZonalBucketAPIs())
		if os.Getenv("PULUMI_GCS_DIRECT_PATH") == "required" {
			options = append(options, experimental.WithDirectConnectivityEnforced())
		}
	}
	client, err := storage.NewGRPCClient(ctx, options...)
	if err != nil {
		return nil, nil, err
	}
	mux := &blob.URLMux{}
	mux.RegisterBucket(gcsblob.Scheme, &gcsblob.URLOpener{Options: gcsblob.Options{Client: client}})
	if api != "rapid" {
		return mux, nil, nil
	}
	return mux, client, nil
}

func gcsBackendAPI(u *url.URL) (string, error) {
	q := u.Query()
	values, ok := q["gcs_api"]
	if !ok {
		return "", nil
	}
	if len(values) != 1 || (values[0] != "grpc" && values[0] != "rapid") {
		return "", errors.New("gcs_api must be grpc or rapid")
	}
	q.Del("gcs_api")
	if len(q) != 0 {
		return "", errors.New("gcs_api cannot be combined with other GCS query parameters")
	}
	u.RawQuery = q.Encode()
	return values[0], nil
}

type rapidBucket struct {
	Bucket
	client *storage.Client
	name   string
	prefix string
}

// WriteAll publishes finalized bytes atomically; incomplete streams remain outside state paths.
func (b *rapidBucket) WriteAll(ctx context.Context, key string, data []byte, opts *blob.WriterOptions) error {
	var options blob.WriterOptions
	if opts != nil {
		options = *opts
	}
	if len(options.ContentMD5) > 0 {
		sum := md5.Sum(data) //nolint:gosec // Validate a caller-supplied transport checksum; GCS validates CRC32C.
		if !bytes.Equal(sum[:], options.ContentMD5) {
			return errors.New("Rapid upload content does not match supplied MD5")
		}
	}
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}
	bucket := b.client.Bucket(b.name)
	target := bucket.Object(b.prefix + filepath.ToSlash(key))
	temporary := bucket.Object(path.Join(b.prefix, ".pulumi/rapid-uploads", id.String()))
	writer := temporary.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	writer.Append = true
	writer.FinalizeOnClose = true
	writer.ContentType = options.ContentType
	if writer.ContentType == "" && !options.DisableContentTypeDetection {
		writer.ContentType = http.DetectContentType(data)
	}
	writer.ForceEmptyContentType = options.DisableContentTypeDetection
	writer.ContentDisposition = options.ContentDisposition
	writer.ContentEncoding = options.ContentEncoding
	writer.ContentLanguage = options.ContentLanguage
	writer.CacheControl = options.CacheControl
	writer.Metadata = options.Metadata
	writer.CRC32C = crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	writer.SendCRC32C = true
	writer.AppendFinalCRC32C = writer.CRC32C
	writer.SendAppendFinalCRC32C = true
	if options.BufferSize > 0 {
		writer.ChunkSize = options.BufferSize
	}
	if options.BeforeWrite != nil {
		err = options.BeforeWrite(func(v any) bool {
			switch p := v.(type) {
			case ***storage.ObjectHandle:
				*p = &target
				return true
			case **storage.Writer:
				*p = writer
				return true
			default:
				return false
			}
		})
		if err != nil {
			return err
		}
	}
	var conditions *storage.Conditions
	if options.IfNotExist {
		conditions = &storage.Conditions{DoesNotExist: true}
	} else if options.BeforeWrite != nil {
		// Attrs enforces any handle precondition installed by BeforeWrite. Bind publication to the same generation.
		attrs, err := target.Attrs(ctx)
		if err != nil {
			return err
		}
		conditions = &storage.Conditions{GenerationMatch: attrs.Generation}
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = temporary.Delete(cleanup)
	}()
	if _, err = writer.Write(data); err != nil {
		_ = writer.Close()
		return err
	}
	if err = writer.Close(); err != nil {
		return err
	}
	_, err = temporary.If(storage.Conditions{GenerationMatch: writer.Attrs().Generation}).Move(
		ctx, storage.MoveObjectDestination{Object: target.ObjectName(), Conditions: conditions})
	return err
}

// Copy avoids the rewrite API, which Rapid buckets do not support.
func (b *rapidBucket) Copy(ctx context.Context, dst, src string, opts *blob.CopyOptions) error {
	if opts != nil && opts.BeforeCopy != nil {
		return errors.New("Rapid copies do not support provider-specific copy callbacks")
	}
	var data []byte
	var err error
	if strings.HasSuffix(src, "/journal.stream") {
		data, err = b.readStream(ctx, src)
	} else {
		data, err = b.ReadAll(ctx, src)
	}
	if err != nil {
		return err
	}
	return b.WriteAll(ctx, dst, data, nil)
}

func rapidBucketPrefix(u *url.URL) string {
	prefix := strings.Trim(u.Path, "/")
	if prefix != "" {
		prefix += "/"
	}
	return prefix
}

func (b *rapidBucket) readStream(ctx context.Context, key string) ([]byte, error) {
	object := b.client.Bucket(b.name).Object(b.prefix + filepath.ToSlash(key))
	attrs, err := object.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	if attrs.Size == 0 {
		return nil, nil
	}
	reader, err := object.Generation(attrs.Generation).NewRangeReader(ctx, 0, attrs.Size)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(reader)
	return data, errors.Join(err, reader.Close())
}
