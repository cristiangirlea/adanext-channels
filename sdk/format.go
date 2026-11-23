package sdk

import "strings"

// EscapeMarkdownV2 renders arbitrary message text literally in Telegram MarkdownV2.
func EscapeMarkdownV2(text string) string {
	var b strings.Builder
	for _, r := range text {
		if strings.ContainsRune("_*[]()~`>#+-=|{}.!\\", r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// PlainText preserves the original text for services with formatting disabled.
func PlainText(text string) string { return text }
