package config_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appconfig "queuectl/internal/config"
)

func TestValidateConfigValueAcceptsBoundaryMinimums(t *testing.T) {
	minimums := map[string]int{
		appconfig.KeyMaxRetries:         1,
		appconfig.KeyBackoffBase:        1,
		appconfig.KeyPollIntervalMS:     50,
		appconfig.KeyLockTimeoutSeconds: 1,
		appconfig.KeyWorkerStaleSeconds: int(2 * appconfig.HeartbeatInterval / time.Second),
		appconfig.KeyStopTimeoutSeconds: 1,
	}
	for key, minimum := range minimums {
		value, err := appconfig.ValidateConfigValue(key, strconv.Itoa(minimum))
		require.NoError(t, err, "key %s at its minimum should be valid", key)
		require.Equal(t, minimum, value)
	}
}

func TestValidateConfigValueRejectsBelowMinimum(t *testing.T) {
	_, err := appconfig.ValidateConfigValue(appconfig.KeyPollIntervalMS, "49")
	require.Error(t, err)
	require.Contains(t, err.Error(), "poll-interval-ms")

	_, err = appconfig.ValidateConfigValue(appconfig.KeyMaxRetries, "0")
	require.Error(t, err)

	_, err = appconfig.ValidateConfigValue(appconfig.KeyLockTimeoutSeconds, "-1")
	require.Error(t, err)
}

// TestValidateConfigValueRejectsWorkerStaleSecondsBelowHeartbeatMargin
// guards the fix for a real status-flicker bug: worker-stale-seconds used
// to accept any value >= 1, but the heartbeat that keeps a worker looking
// "active" only fires every appconfig.HeartbeatInterval. A worker-stale-
// seconds smaller than that would make `queuectl status` intermittently
// report a perfectly healthy worker as inactive, purely from the gap
// between two ordinary heartbeats - not from any actual staleness.
func TestValidateConfigValueRejectsWorkerStaleSecondsBelowHeartbeatMargin(t *testing.T) {
	tooSmall := int(2*appconfig.HeartbeatInterval/time.Second) - 1
	_, err := appconfig.ValidateConfigValue(appconfig.KeyWorkerStaleSeconds, strconv.Itoa(tooSmall))
	require.Error(t, err)
	require.Contains(t, err.Error(), "worker-stale-seconds")
}

func TestValidateConfigValueRejectsNonInteger(t *testing.T) {
	_, err := appconfig.ValidateConfigValue(appconfig.KeyMaxRetries, "not-a-number")
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be an integer")

	_, err = appconfig.ValidateConfigValue(appconfig.KeyMaxRetries, "3.5")
	require.Error(t, err)
}

func TestValidateConfigValueRejectsUnknownKey(t *testing.T) {
	_, err := appconfig.ValidateConfigValue("not-a-real-key", "5")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown config key")
}

func TestValidateConfigValueAcceptsAboveMinimum(t *testing.T) {
	value, err := appconfig.ValidateConfigValue(appconfig.KeyBackoffBase, "10")
	require.NoError(t, err)
	require.Equal(t, 10, value)
}
