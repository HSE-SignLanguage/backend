package api

import "strings"

func combineTranscript(context, literal string) string {
	context = strings.TrimSpace(context)
	literal = strings.TrimSpace(literal)

	switch {
	case context == "":
		return literal
	case literal == "":
		return context
	default:
		return context + " " + literal
	}
}

func shouldSkipLiteral(text string) bool {
	return strings.EqualFold(strings.TrimSpace(text), "no")
}
