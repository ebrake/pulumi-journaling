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
	"errors"
	"fmt"
	"os"

	"github.com/pulumi/pulumi/pkg/v3/backend/display"
	"github.com/pulumi/pulumi/pkg/v3/backend/diy"
	cmdBackend "github.com/pulumi/pulumi/pkg/v3/cmd/pulumi/backend"
	"github.com/pulumi/pulumi/pkg/v3/cmd/pulumi/constrictor"
	pkgWorkspace "github.com/pulumi/pulumi/pkg/v3/workspace"
	"github.com/pulumi/pulumi/sdk/v3/go/common/util/cmdutil"
	"github.com/pulumi/pulumi/sdk/v3/go/common/workspace"
	"github.com/spf13/cobra"
)

// newStateJournalCommand exposes provider-free recovery and explicit evidence-preserving discard.
func newStateJournalCommand(ws pkgWorkspace.Context, lm cmdBackend.LoginManager) *cobra.Command {
	cmd := &cobra.Command{Use: "journal", Short: "Manage DIY state journals"}
	for _, discard := range []bool{false, true} {
		var stackName string
		var yes bool
		operation, short := "recover", "Recover an unfinished journal without invoking providers"
		if discard {
			operation, short = "discard", "Archive and discard a journal without replaying interrupted changes"
		}
		action := &cobra.Command{
			Use: operation, Short: short,
			RunE: func(cmd *cobra.Command, _ []string) error {
				if discard && !yes {
					return errors.New("journal discard requires --yes; state may be missing changes from the interrupted run")
				}
				cwd, err := os.Getwd()
				if err != nil {
					return err
				}
				project, _, err := ws.ReadProject(cwd)
				if err != nil && !errors.Is(err, workspace.ErrProjectNotFound) {
					return err
				}
				b, err := cmdBackend.CurrentBackend(cmd.Context(), ws, lm, project, display.Options{
					Color: cmdutil.GetGlobalColorization(), Stdin: cmd.InOrStdin(), Stdout: cmd.OutOrStdout(),
				})
				if err != nil {
					return err
				}
				lb, ok := b.(diy.Backend)
				if !ok {
					return errors.New("journal management requires a DIY backend")
				}
				ref, err := lb.ParseStackReference(stackName)
				if err != nil {
					return err
				}
				if discard {
					if err := lb.DiscardJournal(cmd.Context(), ref); err != nil {
						return err
					}
					_, err := fmt.Fprintln(cmd.OutOrStdout(),
						"State may be missing changes from the interrupted run; run pulumi refresh")
					return err
				}
				return lb.RecoverJournal(cmd.Context(), ref)
			},
		}
		action.Flags().StringVarP(&stackName, "stack", "s", "", "Stack to manage")
		_ = action.MarkFlagRequired("stack")
		if discard {
			action.Flags().BoolVarP(&yes, "yes", "y", false, "Discard without replaying (required)")
		}
		constrictor.AttachArguments(action, constrictor.NoArgs)
		cmd.AddCommand(action)
	}
	return cmd
}
