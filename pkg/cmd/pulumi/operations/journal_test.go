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

package operations

import (
	"context"
	"errors"
	"testing"

	"github.com/pulumi/pulumi/pkg/v3/backend"
	"github.com/stretchr/testify/require"
)

type journalRecoveryBackend struct {
	backend.Backend
	recover func() error
}

func (b *journalRecoveryBackend) RecoverJournal(context.Context, backend.StackReference) error {
	return b.recover()
}

type journalRecoveryStack struct {
	backend.Stack
	owner backend.Backend
}

func (s *journalRecoveryStack) Backend() backend.Backend    { return s.owner }
func (s *journalRecoveryStack) Ref() backend.StackReference { return nil }

func TestReadOnlyOperationDoesNotRecoverJournal(t *testing.T) {
	t.Parallel()
	s := &journalRecoveryStack{owner: &journalRecoveryBackend{recover: func() error {
		t.Fatal("read-only operation attempted journal recovery")
		return nil
	}}}
	require.NoError(t, recoverDIYJournal(t.Context(), s, true))
}

func TestMutatingOperationPreservesRecoveryFailure(t *testing.T) {
	t.Parallel()
	expected := errors.New("journal requires investigation")
	s := &journalRecoveryStack{owner: &journalRecoveryBackend{recover: func() error { return expected }}}
	require.ErrorIs(t, recoverDIYJournal(t.Context(), s, false), expected)
}

func TestOtherBackendNeedsNoJournalRecovery(t *testing.T) {
	t.Parallel()
	require.NoError(t, recoverDIYJournal(t.Context(), &journalRecoveryStack{}, false))
}
