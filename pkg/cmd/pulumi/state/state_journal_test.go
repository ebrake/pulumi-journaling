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

package state

import (
	"context"
	"errors"
	"testing"

	"github.com/pulumi/pulumi/pkg/v3/backend"
	cmdBackend "github.com/pulumi/pulumi/pkg/v3/cmd/pulumi/backend"
	pkgWorkspace "github.com/pulumi/pulumi/pkg/v3/workspace"
	"github.com/pulumi/pulumi/sdk/v3/go/common/diag"
	"github.com/pulumi/pulumi/sdk/v3/go/common/diag/colors"
	"github.com/pulumi/pulumi/sdk/v3/go/common/workspace"

	"github.com/stretchr/testify/require"
)

func TestJournalRecoveryRequiresExplicitStack(t *testing.T) {
	t.Parallel()
	command := newStateJournalCommand(nil, nil)
	command.SetArgs([]string{"recover"})
	require.ErrorContains(t, command.Execute(), `required flag(s) "stack" not set`)
}

func TestJournalDiscardRequiresYes(t *testing.T) {
	t.Parallel()
	command := newStateJournalCommand(nil, nil)
	command.SetArgs([]string{"discard", "--stack", "organization/project/stack"})
	require.ErrorContains(t, command.Execute(), "requires --yes")
}

func TestJournalCommandsAllowMissingProject(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"recover", "discard"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			ws := &pkgWorkspace.MockContext{
				ReadProjectF: func(string) (*workspace.Project, string, error) { return nil, "", workspace.ErrProjectNotFound },
			}
			reachedBackend := errors.New("backend selected without project")
			lm := &cmdBackend.MockLoginManager{LoginF: func(
				_ context.Context, _ pkgWorkspace.Context, _ diag.Sink, _ string, project *workspace.Project,
				_ bool, _ bool, _ colors.Colorization, _ ...cmdBackend.LoginOption,
			) (backend.Backend, error) {
				require.Nil(t, project)
				return nil, reachedBackend
			}}
			cmd := newStateJournalCommand(ws, lm)
			args := []string{operation, "--stack", "organization/project/stack"}
			if operation == "discard" {
				args = append(args, "--yes")
			}
			cmd.SetArgs(args)
			require.ErrorIs(t, cmd.Execute(), reachedBackend)
		})
	}
}
