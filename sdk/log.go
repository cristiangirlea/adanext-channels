package sdk

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// Logger serialises diagnostics and defaults to stderr.
type Logger struct {
	Writer io.Writer
	Redact []string
	mu     sync.Mutex
}

func (l *Logger) clean(text string) string {
	if l != nil && len(l.Redact) > 0 {
		secrets := slices.Clone(l.Redact)
		// A shorter matching prefix must not expose the rest of a longer secret.
		slices.SortStableFunc(secrets, func(a, b string) int { return cmp.Compare(len(b), len(a)) })
		var pairs []string
		for _, secret := range secrets {
			if secret != "" {
				pairs = append(pairs, secret, "[redacted]")
			}
		}
		text = strings.NewReplacer(pairs...).Replace(text)
	}
	var line strings.Builder
	for _, r := range text {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			q := strconv.QuoteRuneToASCII(r)
			line.WriteString(q[1 : len(q)-1])
		} else {
			line.WriteRune(r)
		}
	}
	return line.String()
}

func (l *Logger) Printf(format string, args ...any) {
	var out io.Writer = os.Stderr
	if l != nil {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.Writer != nil {
			out = l.Writer
		}
	}
	fmt.Fprintln(out, l.clean(fmt.Sprintf(format, args...)))
}
