package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	appconfig "queuectl/internal/config"
	"queuectl/internal/storage"
)

// NewRootCommand builds the queuectl Cobra command tree: enqueue, worker
// start/stop, status, list, dlq list/retry, and config set.
func NewRootCommand() *cobra.Command {
	var dbPathFlag string

	root := &cobra.Command{
		Use:           "queuectl",
		Short:         "CLI-based persistent background job queue",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&dbPathFlag, "db", "", "path to SQLite database")

	root.AddCommand(newEnqueueCommand(&dbPathFlag))
	root.AddCommand(newWorkerCommand(&dbPathFlag))
	root.AddCommand(newStatusCommand(&dbPathFlag))
	root.AddCommand(newListCommand(&dbPathFlag))
	root.AddCommand(newDLQCommand(&dbPathFlag))
	root.AddCommand(newConfigCommand(&dbPathFlag))

	return root
}

func openStore(ctx context.Context, dbPathFlag *string) (*storage.Store, error) {
	path := resolvedDBPath(dbPathFlag)
	store, err := storage.Open(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("open queue database %s: %w", path, err)
	}
	return store, nil
}

func resolvedDBPath(dbPathFlag *string) string {
	return appconfig.ResolveDBPath(*dbPathFlag)
}
