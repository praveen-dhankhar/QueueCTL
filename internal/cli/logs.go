package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

func newLogsCommand(dbPathFlag *string) *cobra.Command {
	return &cobra.Command{
		Use:   "logs JOB_ID",
		Short: "Show stdout/stderr recorded for each execution attempt of a job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := openStore(ctx, dbPathFlag)
			if err != nil {
				return err
			}
			defer store.Close()

			jobID := args[0]
			if _, err := store.GetJob(ctx, jobID); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("job %q not found", jobID)
				}
				return err
			}

			runs, err := store.ListJobRuns(ctx, jobID)
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			if len(runs) == 0 {
				fmt.Fprintf(out, "job %s has no recorded execution attempts yet\n", jobID)
				return nil
			}
			for i, run := range runs {
				if i > 0 {
					fmt.Fprintln(out)
				}
				exitCode := "n/a"
				if run.ExitCode != nil {
					exitCode = fmt.Sprintf("%d", *run.ExitCode)
				}
				fmt.Fprintf(out, "attempt %d  worker=%s  exit_code=%s  started=%s  finished=%s\n",
					run.Attempt, run.WorkerID, exitCode, formatCLITime(run.StartedAt), formatCLITime(run.FinishedAt))
				fmt.Fprintln(out, "stdout:")
				printLogBody(out, run.Stdout)
				fmt.Fprintln(out, "stderr:")
				printLogBody(out, run.Stderr)
			}
			return nil
		},
	}
}

func printLogBody(out io.Writer, body string) {
	if body == "" {
		fmt.Fprintln(out, "  (empty)")
		return
	}
	fmt.Fprintln(out, body)
}
