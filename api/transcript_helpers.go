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

func trimContext(context string, maxRunes int) string {
	context = strings.TrimSpace(context)
	if maxRunes <= 0 {
		return ""
	}

	runes := []rune(context)
	if len(runes) <= maxRunes {
		return context
	}
	return strings.TrimSpace(string(runes[len(runes)-maxRunes:]))
}
