package sdk

import (
	"strings"
	"unicode/utf8"
)

// Split preserves delimiters and returns nil when the byte limit cannot hold a rune.
func Split(text string, limit int) []string {
	if text == "" {
		return []string{""}
	}
	if limit <= 0 {
		return nil
	}
	var parts []string
	for len(text) > limit {
		end := limit
		for end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
		if end == 0 {
			return nil
		}
		if i := strings.LastIndexByte(text[:end], '\n'); i >= 0 {
			end = i + 1
		} else if i := strings.LastIndexByte(text[:end], ' '); i >= 0 {
			end = i + 1
		}
		parts = append(parts, text[:end])
		text = text[end:]
	}
	return append(parts, text)
}
