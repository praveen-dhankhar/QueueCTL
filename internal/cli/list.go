package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"queuectl/internal/job"
)

func newListCommand(dbPathFlag *string) *cobra.Command {
	var stateFlag string
	var jsonOutput bool
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
			if jsonOutput {
				return printJobsJSON(cmd, jobs)
			}
			printJobs(cmd, jobs)
			return nil
		},
	}
	cmd.Flags().StringVar(&stateFlag, "state", "", "job state: pending, processing, completed, failed, dead")
	_ = cmd.MarkFlagRequired("state")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "print a JSON array of job objects to stdout instead of a table")
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

// jsonJob is the wire shape for "queuectl list --json", matching the field
// names and RFC3339 timestamp format ("2025-11-04T10:30:00Z") from the job
// spec in the assignment. next_retry_at is an addition beyond the required
// fields (useful for inspecting a failed job's backoff), omitted when unset
// rather than emitted as null.
type jsonJob struct {
	ID          string  `json:"id"`
	Command     string  `json:"command"`
	State       string  `json:"state"`
	Attempts    int     `json:"attempts"`
	MaxRetries  int     `json:"max_retries"`
	NextRetryAt *string `json:"next_retry_at,omitempty"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
}

// printJobsJSON writes jobs to stdout as a JSON array and nothing else, per
// the interface contract ("queuectl list --state <state> --json prints a
// JSON array of job objects to stdout (and nothing else on stdout)"). jobs
// is never marshaled as a nil slice, so an empty result is "[]", not "null".
func printJobsJSON(cmd *cobra.Command, jobs []job.Job) error {
	out := make([]jsonJob, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jsonJob{
			ID:          j.ID,
			Command:     j.Command,
			State:       string(j.State),
			Attempts:    j.Attempts,
			MaxRetries:  j.MaxRetries,
			NextRetryAt: formatOptionalRFC3339(j.NextRetryAt),
			CreatedAt:   j.CreatedAt.UTC().Format(time.RFC3339),
			UpdatedAt:   j.UpdatedAt.UTC().Format(time.RFC3339),
		})
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("marshal jobs as json: %w", err)
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), string(encoded))
	return err
}

func formatOptionalRFC3339(t *time.Time) *string {
	if t == nil {
		return nil
	}
	formatted := t.UTC().Format(time.RFC3339)
	return &formatted
}
