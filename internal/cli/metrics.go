package cli

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

func newMetricsCommand(dbPathFlag *string) *cobra.Command {
	var asJSON bool

	cmd := &cobra.Command{
		Use:   "metrics",
		Short: "Show execution metrics: throughput, success rate, and durations",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := openStore(ctx, dbPathFlag)
			if err != nil {
				return err
			}
			defer store.Close()

			m, err := store.GetMetrics(ctx)
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			if asJSON {
				encoder := json.NewEncoder(out)
				encoder.SetIndent("", "  ")
				return encoder.Encode(m)
			}

			fmt.Fprintln(out, "Runs:")
			fmt.Fprintf(out, "total: %d\n", m.TotalRuns)
			fmt.Fprintf(out, "succeeded: %d\n", m.Succeeded)
			fmt.Fprintf(out, "failed: %d\n", m.Failed)
			fmt.Fprintf(out, "interrupted: %d\n", m.Interrupted)
			fmt.Fprintf(out, "success rate: %.1f%%\n", m.SuccessRate)
			fmt.Fprintln(out)
			fmt.Fprintln(out, "Duration (seconds, completed attempts only):")
			fmt.Fprintf(out, "avg: %.2f\n", m.AvgSeconds)
			fmt.Fprintf(out, "p95: %.2f\n", m.P95Seconds)
			fmt.Fprintf(out, "max: %.2f\n", m.MaxSeconds)
			fmt.Fprintln(out)
			fmt.Fprintln(out, "Throughput (jobs completed):")
			fmt.Fprintf(out, "last 1m: %d\n", m.Last1m)
			fmt.Fprintf(out, "last 5m: %d\n", m.Last5m)
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print metrics as a JSON object")
	return cmd
}
