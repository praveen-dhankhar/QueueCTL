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
	return cmd
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
