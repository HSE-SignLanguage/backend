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

func extractNewSegment(previous, updated string) string {
	previous = strings.TrimSpace(previous)
	updated = strings.TrimSpace(updated)

	if updated == "" {
		return ""
	}

	if previous == "" {
		return updated
	}

	maxOverlap := min(len(previous), len(updated))
	overlap := 0
	for i := maxOverlap; i > 0; i-- {
		if previous[len(previous)-i:] == updated[:i] {
			overlap = i
			break
		}
	}

	if len(updated) > overlap {
		return strings.TrimSpace(updated[overlap:])
	}
	return ""
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
