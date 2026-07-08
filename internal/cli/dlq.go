package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"queuectl/internal/job"
)

func newDLQCommand(dbPathFlag *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dlq",
		Short: "Inspect and retry jobs in the Dead Letter Queue",
	}
	cmd.AddCommand(newDLQListCommand(dbPathFlag))
	cmd.AddCommand(newDLQRetryCommand(dbPathFlag))
	return cmd
}

func newDLQListCommand(dbPathFlag *string) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List dead jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := openStore(ctx, dbPathFlag)
			if err != nil {
				return err
			}
			defer store.Close()

			jobs, err := store.ListJobs(ctx, job.StateDead)
			if err != nil {
				return err
			}
			printJobs(cmd, jobs)
			return nil
		},
	}
}

func newDLQRetryCommand(dbPathFlag *string) *cobra.Command {
	return &cobra.Command{
		Use:   "retry JOB_ID",
		Short: "Move a dead job back to pending",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := openStore(ctx, dbPathFlag)
			if err != nil {
				return err
			}
			defer store.Close()

			if err := store.RetryDeadJob(ctx, args[0]); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "moved job %s from dead to pending\n", args[0])
			return nil
		},
	}
}
