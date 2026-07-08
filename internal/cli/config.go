package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	appconfig "queuectl/internal/config"
)

func newConfigCommand(dbPathFlag *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage queuectl runtime configuration",
	}
	cmd.AddCommand(newConfigSetCommand(dbPathFlag))
	cmd.AddCommand(newConfigGetCommand(dbPathFlag))
	cmd.AddCommand(newConfigListCommand(dbPathFlag))
	return cmd
}

func newConfigGetCommand(dbPathFlag *string) *cobra.Command {
	return &cobra.Command{
		Use:   "get KEY",
		Short: "Print a single configuration value",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := openStore(ctx, dbPathFlag)
			if err != nil {
				return err
			}
			defer store.Close()

			value, err := store.GetConfigInt(ctx, args[0])
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s=%d\n", args[0], value)
			return nil
		},
	}
}

func newConfigListCommand(dbPathFlag *string) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "Print every configuration key and its current value",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := openStore(ctx, dbPathFlag)
			if err != nil {
				return err
			}
			defer store.Close()

			entries, err := store.ListConfig(ctx)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			for _, entry := range entries {
				fmt.Fprintf(out, "%s=%d\n", entry.Key, entry.Value)
			}
			return nil
		},
	}
}

func newConfigSetCommand(dbPathFlag *string) *cobra.Command {
	return &cobra.Command{
		Use:   "set KEY VALUE",
		Short: "Set a numeric queuectl configuration value",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			value, err := appconfig.ValidateConfigValue(key, args[1])
			if err != nil {
				return err
			}

			ctx := context.Background()
			store, err := openStore(ctx, dbPathFlag)
			if err != nil {
				return err
			}
			defer store.Close()

			if err := store.SetConfigInt(ctx, key, value); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s=%d\n", key, value)
			return nil
		},
	}
}
