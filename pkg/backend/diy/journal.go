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
	"path"
	"sync"

	"github.com/pulumi/pulumi/pkg/v3/backend"
	"github.com/pulumi/pulumi/pkg/v3/engine"
	"github.com/pulumi/pulumi/pkg/v3/resource/stack"
	"github.com/pulumi/pulumi/pkg/v3/secrets"
	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource/config"
)

type diyJournaler struct {
	// engine.Journal has no context arguments.
	ctx            context.Context
	b              *diyBackend
	dir            string
	sm             secrets.Manager
	mu             sync.Mutex
	wg             sync.WaitGroup
	closed         bool
	failure        error
	next           uint64
	durableThrough uint64
	completed      map[uint64]bool
	changed        chan struct{}
	slots          chan struct{}
}

var _ engine.Journal = (*diyJournaler)(nil)

func (b *diyBackend) newDIYJournaler(ctx context.Context, ref *diyBackendReference,
	m *journalManifest, sm secrets.Manager,
) *diyJournaler {
	return &diyJournaler{
		ctx: ctx, b: b, dir: path.Join(b.journalDir(ref), m.Epoch, "entries"), sm: sm,
		completed: map[uint64]bool{}, changed: make(chan struct{}), slots: make(chan struct{}, 50),
	}
}

// AddJournalEntry queues immutable entries. Required writes wait until the submitted prefix is durable.
func (j *diyJournaler) AddJournalEntry(entry engine.JournalEntry) error {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return errors.New("journal is closed")
	}
	if j.failure != nil {
		err := j.failure
		j.mu.Unlock()
		return err
	}
	j.next++
	index := j.next
	j.wg.Add(1)
	j.mu.Unlock()
	// Serialize before returning: the engine may subsequently mutate resource state.
	serialized, err := stack.BatchEncrypt(j.ctx, j.sm, func(
		ctx context.Context, enc config.Encrypter,
	) (apitype.JournalEntry, error) {
		return backend.SerializeJournalEntry(ctx, entry, enc)
	})
	if err != nil {
		j.complete(index, err)
		return err
	}
	data, err := journalJSON(serialized)
	if err != nil {
		j.complete(index, err)
		return err
	}
	select {
	case j.slots <- struct{}{}:
	case <-j.ctx.Done():
		j.complete(index, j.ctx.Err())
		return j.ctx.Err()
	}
	go func() {
		key := path.Join(j.dir, fmt.Sprintf("%020d.journal.json", index))
		err := j.b.journalPut(j.ctx, key, data)
		<-j.slots
		j.complete(index, err)
	}()
	if entry.ElideWrite {
		return nil
	}
	for {
		j.mu.Lock()
		failure, durable, changed := j.failure, j.durableThrough, j.changed
		j.mu.Unlock()
		if failure != nil {
			return failure
		}
		if durable >= index {
			return nil
		}
		select {
		case <-changed:
		case <-j.ctx.Done():
			return j.ctx.Err()
		}
	}
}

// Close waits for queued writes and returns all persistence failures.
func (j *diyJournaler) Close() error {
	j.mu.Lock()
	j.closed = true
	j.mu.Unlock()
	j.wg.Wait()
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.failure
}

func (j *diyJournaler) complete(index uint64, err error) {
	j.mu.Lock()
	j.failure = errors.Join(j.failure, err)
	if err == nil {
		j.completed[index] = true
		for j.completed[j.durableThrough+1] {
			j.durableThrough++
			delete(j.completed, j.durableThrough)
		}
	}
	close(j.changed)
	j.changed = make(chan struct{})
	j.mu.Unlock()
	j.wg.Done()
}
