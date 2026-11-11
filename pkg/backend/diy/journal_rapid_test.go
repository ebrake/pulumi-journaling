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

package diy

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/stretchr/testify/require"
)

func TestRapidJournalStreamInterruptedTail(t *testing.T) {
	t.Parallel()
	first := apitype.JournalEntry{Version: 1, SequenceID: 1, OperationID: 1, Kind: apitype.JournalEntryKindBegin}
	second := apitype.JournalEntry{
		Version: 1, SequenceID: 2, OperationID: 1, Kind: apitype.JournalEntryKindSuccess,
		State: journalResource(),
	}
	a, err := encodeRapidJournalFrame(first)
	require.NoError(t, err)
	b, err := encodeRapidJournalFrame(second)
	require.NoError(t, err)
	data := append([]byte(rapidJournalMagic), a...)
	data = append(data, b...)
	for size := range len(data) + 1 {
		entries, err := decodeRapidJournalStream(data[:size])
		require.NoError(t, err)
		expected := 0
		if size >= len(rapidJournalMagic)+len(a) {
			expected++
		}
		if size == len(data) {
			expected++
		}
		require.Len(t, entries, expected, "persisted bytes: %d", size)
	}
	entries, err := decodeRapidJournalStream(data)
	require.NoError(t, err)
	require.Equal(t, []apitype.JournalEntry{first, second}, entries)
}

func TestRapidJournalStreamRejectsCorruption(t *testing.T) {
	t.Parallel()
	entry := apitype.JournalEntry{
		Version: 1, SequenceID: 1, OperationID: 1, Kind: apitype.JournalEntryKindSuccess,
		State: journalResource(),
	}
	frame, err := encodeRapidJournalFrame(entry)
	require.NoError(t, err)
	data := append([]byte(rapidJournalMagic), frame...)
	corruptHeader := slices.Clone(data)
	corruptHeader[len(rapidJournalMagic)] ^= 1
	_, err = decodeRapidJournalStream(corruptHeader)
	require.ErrorContains(t, err, "header checksum")
	corrupt := slices.Clone(data)
	corrupt[len(corrupt)-1] ^= 1
	_, err = decodeRapidJournalStream(corrupt)
	require.ErrorContains(t, err, "checksum")
	_, err = decodeRapidJournalStream(append(data, frame...))
	require.ErrorContains(t, err, "duplicate")
	_, err = decodeRapidJournalStream([]byte("incorrect"))
	require.ErrorContains(t, err, "header")
	empty := slices.Clone(data)
	binary.BigEndian.PutUint32(empty[len(rapidJournalMagic):], 0)
	_, err = decodeRapidJournalStream(empty)
	require.Error(t, err)
}
