package cli

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	appconfig "queuectl/internal/config"
	"queuectl/internal/storage"
	workerpkg "queuectl/internal/worker"
)

func newWorkerCommand(dbPathFlag *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Start or stop worker supervisor processes",
	}
	cmd.AddCommand(newWorkerStartCommand(dbPathFlag))
	cmd.AddCommand(newWorkerStopCommand(dbPathFlag))
	return cmd
}

func newWorkerStartCommand(dbPathFlag *string) *cobra.Command {
	var count int
	cmd := &cobra.Command{
		Use:   "start --count 3",
		Short: "Start a foreground worker supervisor",
		RunE: func(cmd *cobra.Command, args []string) error {
			if count < 1 {
				return errors.New("count must be >= 1")
			}

			ctx := context.Background()
			dbPath := resolvedDBPath(dbPathFlag)
			store, err := storage.Open(ctx, dbPath)
			if err != nil {
				return err
			}
			defer store.Close()

			signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			pool := workerpkg.NewPool(store, count, appconfig.WorkerPIDPath(dbPath), logger)
			return pool.Start(signalCtx)
		},
	}
	cmd.Flags().IntVar(&count, "count", 1, "number of worker goroutines to start")
	return cmd
}

func newWorkerStopCommand(dbPathFlag *string) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Gracefully stop the worker supervisor",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			dbPath := resolvedDBPath(dbPathFlag)
			store, err := storage.Open(ctx, dbPath)
			if err != nil {
				return err
			}
			defer store.Close()

			timeoutSeconds, err := store.GetConfigInt(ctx, appconfig.KeyStopTimeoutSeconds)
			if err != nil {
				return err
			}
			return workerpkg.StopSupervisor(appconfig.WorkerPIDPath(dbPath), time.Duration(timeoutSeconds)*time.Second, cmd.OutOrStdout(), force)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "skip process verification if it cannot be performed (e.g. sandboxed environments where ps is blocked) and signal the PID anyway")
	return cmd
}
