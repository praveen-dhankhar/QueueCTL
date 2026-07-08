package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	appconfig "queuectl/internal/config"
	"queuectl/internal/job"
)

func newStatusCommand(dbPathFlag *string) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show job counts by state and active worker count",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := openStore(ctx, dbPathFlag)
			if err != nil {
				return err
			}
			defer store.Close()

			counts, err := store.CountJobsByState(ctx)
			if err != nil {
				return err
			}
			staleSeconds, err := store.GetConfigInt(ctx, appconfig.KeyWorkerStaleSeconds)
			if err != nil {
				return err
			}
			activeWorkers, err := store.CountActiveWorkers(ctx, staleSeconds)
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			fmt.Fprintln(out, "Jobs:")
			for _, state := range job.AllStates() {
				fmt.Fprintf(out, "%s: %d\n", state, counts[state])
			}
			fmt.Fprintln(out)
			fmt.Fprintln(out, "Workers:")
			fmt.Fprintf(out, "active: %d\n", activeWorkers)
			return nil
		},
	}
}
