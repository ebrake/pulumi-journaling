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
	"time"

	"github.com/pulumi/pulumi/sdk/v3/go/common/util/logging"
	"gocloud.dev/blob"
)

// tracedBucket logs every storage operation and its latency at verbosity 7. It is installed only when that level
// is enabled, so normal runs pay nothing. Journaling cost is dominated by sequential round trips, so the operation
// count and per-operation latency in this trace show where time goes.
type tracedBucket struct {
	Bucket
}

func traceBucket(b Bucket) Bucket {
	if !logging.V(7).Enabled() {
		return b
	}
	return &tracedBucket{Bucket: b}
}

func traceOp(op, key string) func(error) {
	start := time.Now()
	return func(err error) {
		logging.V(7).Infof("diy bucket %s %s took %s (err=%v)", op, key, time.Since(start), err)
	}
}

func (b *tracedBucket) Copy(ctx context.Context, dst, src string, opts *blob.CopyOptions) (err error) {
	done := traceOp("copy", src+" -> "+dst)
	defer func() { done(err) }()
	return b.Bucket.Copy(ctx, dst, src, opts)
}

func (b *tracedBucket) Delete(ctx context.Context, key string) (err error) {
	done := traceOp("delete", key)
	defer func() { done(err) }()
	return b.Bucket.Delete(ctx, key)
}

func (b *tracedBucket) NewReader(ctx context.Context, key string, opts *blob.ReaderOptions) (*blob.Reader, error) {
	done := traceOp("open", key)
	r, err := b.Bucket.NewReader(ctx, key, opts)
	done(err)
	return r, err
}

func (b *tracedBucket) ReadAll(ctx context.Context, key string) (data []byte, err error) {
	done := traceOp("read", key)
	defer func() { done(err) }()
	return b.Bucket.ReadAll(ctx, key)
}

func (b *tracedBucket) WriteAll(ctx context.Context, key string, p []byte, opts *blob.WriterOptions) (err error) {
	done := traceOp("write", key)
	defer func() { done(err) }()
	return b.Bucket.WriteAll(ctx, key, p, opts)
}

func (b *tracedBucket) Exists(ctx context.Context, key string) (exists bool, err error) {
	done := traceOp("exists", key)
	defer func() { done(err) }()
	return b.Bucket.Exists(ctx, key)
}

func (b *tracedBucket) List(opts *blob.ListOptions) *blob.ListIterator {
	if opts != nil {
		logging.V(7).Infof("diy bucket list %s", opts.Prefix)
	}
	return b.Bucket.List(opts)
}
