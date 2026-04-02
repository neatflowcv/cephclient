package cli

import "strings"

func quoteField(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `\"`) + `"`
}
