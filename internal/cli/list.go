package cli

import (
	"context"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"queuectl/internal/job"
)

func newListCommand(dbPathFlag *string) *cobra.Command {
	var stateFlag string
	cmd := &cobra.Command{
		Use:   "list --state pending",
		Short: "List jobs in a specific state",
		RunE: func(cmd *cobra.Command, args []string) error {
			state, err := job.ParseState(stateFlag)
			if err != nil {
				return err
			}

			ctx := context.Background()
			store, err := openStore(ctx, dbPathFlag)
			if err != nil {
				return err
			}
			defer store.Close()

			jobs, err := store.ListJobs(ctx, state)
			if err != nil {
				return err
			}
			printJobs(cmd, jobs)
			return nil
		},
	}
	cmd.Flags().StringVar(&stateFlag, "state", "", "job state: pending, processing, completed, failed, dead")
	_ = cmd.MarkFlagRequired("state")
	return cmd
}

func printJobs(cmd *cobra.Command, jobs []job.Job) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "id\tcommand\tstate\tattempts\tmax_retries\tnext_retry_at\tcreated_at\tupdated_at")
	for _, j := range jobs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\n",
			j.ID,
			j.Command,
			j.State,
			j.Attempts,
			j.MaxRetries,
			formatOptionalTime(j.NextRetryAt),
			formatCLITime(j.CreatedAt),
			formatCLITime(j.UpdatedAt),
		)
	}
	_ = w.Flush()
}
