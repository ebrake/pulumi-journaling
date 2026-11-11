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

// Modified by E Brake for pulumi-journaling: durable Rapid append-stream journals.

package diy

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"path"
	"slices"
	"sync"

	"cloud.google.com/go/storage"

	"github.com/pulumi/pulumi/pkg/v3/backend"
	"github.com/pulumi/pulumi/pkg/v3/engine"
	"github.com/pulumi/pulumi/pkg/v3/resource/stack"
	"github.com/pulumi/pulumi/pkg/v3/secrets"
	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource/config"
)

const (
	rapidJournalFormat = "rapid-stream-v1"
	rapidJournalMagic  = "PULJNL3\n"
)

var rapidJournalCRC = crc32.MakeTable(crc32.Castagnoli)

type rapidJournalStream struct {
	ctx       context.Context
	writer    *storage.Writer
	sm        secrets.Manager
	mu        sync.Mutex
	closed    bool
	failure   error
	submitted int64
	checksum  hash.Hash32
}

func (b *diyBackend) newRapidJournalStream(
	ctx context.Context, ref *diyBackendReference, m *journalManifest, sm secrets.Manager,
) *rapidJournalStream {
	key := b.rapid.prefix + path.Join(b.journalDir(ref), m.Epoch, "journal.stream")
	object := b.rapid.client.Bucket(b.rapid.name).Object(key)
	writer := object.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	writer.Append = true
	writer.FinalizeOnClose = true
	writer.ContentType = "application/octet-stream"
	return &rapidJournalStream{ctx: ctx, writer: writer, sm: sm, checksum: crc32.New(rapidJournalCRC)}
}

func (s *rapidJournalStream) add(entry engine.JournalEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("journal is closed")
	}
	if s.failure != nil {
		return s.failure
	}
	serialized, err := stack.BatchEncrypt(s.ctx, s.sm, func(
		ctx context.Context, enc config.Encrypter,
	) (apitype.JournalEntry, error) {
		return backend.SerializeJournalEntry(ctx, entry, enc)
	})
	if err != nil {
		s.failure = err
		return err
	}
	frame, err := encodeRapidJournalFrame(serialized)
	if err != nil {
		s.failure = err
		return err
	}
	if s.submitted == 0 {
		err = s.write([]byte(rapidJournalMagic))
	}
	if err == nil {
		err = s.write(frame)
	}
	if err == nil && !entry.ElideWrite {
		var persisted int64
		persisted, err = s.writer.Flush()
		if err == nil && persisted < s.submitted {
			err = errors.New("Rapid journal flush did not acknowledge the submitted prefix")
		}
	}
	s.failure = err
	return err
}

func (s *rapidJournalStream) write(data []byte) error {
	n, err := s.writer.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return err
	}
	_, _ = s.checksum.Write(data)
	s.submitted += int64(n)
	return nil
}

func (s *rapidJournalStream) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.failure
	}
	s.closed = true
	if s.submitted == 0 && s.failure == nil {
		s.failure = s.write([]byte(rapidJournalMagic))
	}
	s.writer.SendAppendFinalCRC32C = true
	s.writer.AppendFinalCRC32C = s.checksum.Sum32()
	s.failure = errors.Join(s.failure, s.writer.Close())
	return s.failure
}

func encodeRapidJournalFrame(entry apitype.JournalEntry) ([]byte, error) {
	data, err := journalJSON(entry)
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) > math.MaxUint32 {
		return nil, errors.New("journal entry exceeds stream frame size")
	}
	frame := make([]byte, 12+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	binary.BigEndian.PutUint32(frame[4:], crc32.Checksum(frame[:4], rapidJournalCRC))
	binary.BigEndian.PutUint32(frame[8:], crc32.Checksum(data, rapidJournalCRC))
	copy(frame[12:], data)
	return frame, nil
}

// decodeRapidJournalStream accepts only the complete validated prefix of a crashed writer.
func decodeRapidJournalStream(data []byte) ([]apitype.JournalEntry, error) {
	if len(data) < len(rapidJournalMagic) {
		if bytes.HasPrefix([]byte(rapidJournalMagic), data) {
			return nil, nil
		}
		return nil, errors.New("invalid Rapid journal stream header")
	}
	if string(data[:len(rapidJournalMagic)]) != rapidJournalMagic {
		return nil, errors.New("invalid Rapid journal stream header")
	}
	data = data[len(rapidJournalMagic):]
	var entries []apitype.JournalEntry
	for len(data) >= 12 {
		if crc32.Checksum(data[:4], rapidJournalCRC) != binary.BigEndian.Uint32(data[4:]) {
			return nil, errors.New("Rapid journal frame header checksum mismatch")
		}
		size := uint64(binary.BigEndian.Uint32(data))
		if size == 0 {
			return nil, errors.New("empty Rapid journal frame")
		}
		if size > uint64(len(data)-12) {
			break
		}
		payload := data[12 : 12+size]
		if crc32.Checksum(payload, rapidJournalCRC) != binary.BigEndian.Uint32(data[8:]) {
			return nil, errors.New("Rapid journal frame checksum mismatch")
		}
		var entry apitype.JournalEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			return nil, err
		}
		if entry.SequenceID <= 0 {
			return nil, errors.New("invalid journal sequence")
		}
		entries = append(entries, entry)
		data = data[12+size:]
	}
	seen := map[int64]bool{}
	for _, entry := range entries {
		if seen[entry.SequenceID] {
			return nil, errors.New("duplicate journal sequence")
		}
		seen[entry.SequenceID] = true
	}
	slices.SortFunc(entries, func(a, b apitype.JournalEntry) int {
		if a.SequenceID < b.SequenceID {
			return -1
		}
		if a.SequenceID > b.SequenceID {
			return 1
		}
		return 0
	})
	return entries, nil
}

func (b *diyBackend) readRapidJournal(
	ctx context.Context, ref *diyBackendReference, m *journalManifest,
) ([]apitype.JournalEntry, error) {
	if b.rapid == nil {
		return nil, errors.New("Rapid journal requires the Rapid gRPC backend")
	}
	key := path.Join(b.journalDir(ref), m.Epoch, "journal.stream")
	data, err := b.rapid.readStream(ctx, key)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading Rapid journal: %w", err)
	}
	return decodeRapidJournalStream(data)
}
