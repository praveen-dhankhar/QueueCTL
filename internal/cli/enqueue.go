package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	appconfig "queuectl/internal/config"
	"queuectl/internal/job"
)

type enqueueInput struct {
	ID             string `json:"id"`
	Command        string `json:"command"`
	MaxRetries     *int   `json:"max_retries"`
	TimeoutSeconds *int   `json:"timeout_seconds"`
}

func newEnqueueCommand(dbPathFlag *string) *cobra.Command {
	return &cobra.Command{
		Use:   "enqueue '{\"id\":\"job1\",\"command\":\"echo hello\"}'",
		Short: "Enqueue a shell-command job from JSON input",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := openStore(ctx, dbPathFlag)
			if err != nil {
				return err
			}
			defer store.Close()

			var input enqueueInput
			if err := json.Unmarshal([]byte(args[0]), &input); err != nil {
				return fmt.Errorf("invalid enqueue JSON: %w", err)
			}
			input.Command = strings.TrimSpace(input.Command)
			if input.Command == "" {
				return fmt.Errorf("command is required")
			}
			id := strings.TrimSpace(input.ID)
			if id == "" {
				id, err = generateID()
				if err != nil {
					return err
				}
			}

			maxRetries := 0
			if input.MaxRetries != nil {
				if *input.MaxRetries < 1 {
					return fmt.Errorf("max_retries must be >= 1")
				}
				maxRetries = *input.MaxRetries
			} else {
				maxRetries, err = store.GetConfigInt(ctx, appconfig.KeyMaxRetries)
				if err != nil {
					return err
				}
			}

			// Zero is a legitimate value here (run with no timeout), so an
			// explicit "timeout_seconds": 0 is honored rather than falling
			// back to the config default. Only an absent field falls back.
			timeoutSeconds := 0
			if input.TimeoutSeconds != nil {
				if *input.TimeoutSeconds < 0 {
					return fmt.Errorf("timeout_seconds must be >= 0")
				}
				timeoutSeconds = *input.TimeoutSeconds
			} else {
				timeoutSeconds, err = store.GetConfigInt(ctx, appconfig.KeyJobTimeoutSeconds)
				if err != nil {
					return err
				}
			}

			j, err := job.New(id, input.Command, maxRetries, time.Now())
			if err != nil {
				return err
			}
			j.TimeoutSeconds = timeoutSeconds
			if err := store.InsertJob(ctx, j); err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "constraint") {
					return fmt.Errorf("job id %q already exists", id)
				}
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "enqueued job %s\n", id)
			return nil
		},
	}
}

func generateID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate job id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
