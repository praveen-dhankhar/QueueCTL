package cli

import "time"

// formatCLITime renders a timestamp in the CLI's display format, always in
// UTC so output is stable regardless of the invoking shell's local zone.
func formatCLITime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

// formatOptionalTime is formatCLITime for a possibly-nil pointer (e.g.
// Job.NextRetryAt), rendering as an empty string when nil.
func formatOptionalTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return formatCLITime(*t)
}
