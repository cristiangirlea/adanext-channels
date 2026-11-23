package main

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

const apiBase = "https://api.telegram.org"

var info = sdk.Info{Name: "telegram", Label: "Telegram", Version: "1"}

func main() {
	os.Exit(serve(context.Background(), os.Stdin, os.Stdout, os.Stderr))
}

func groupsEnabled() bool {
	return os.Getenv("ADA_TELEGRAM_GROUPS") == "1"
}

func serve(ctx context.Context, in io.Reader, out, stderr io.Writer) int {
	token := os.Getenv("ADA_TELEGRAM_TOKEN")
	s := newService(apiBase, token, groupsEnabled(), stderr)
	if token == "" {
		s.log.Printf("telegram: ADA_TELEGRAM_TOKEN is required")
		return p.ExitFailed
	}
	err := sdk.Run(ctx, info, s, in, out, s.log)
	if errors.Is(err, sdk.ErrRefused) {
		return p.ExitRefused
	}
	if err != nil {
		s.log.Printf("telegram: %v", err)
		return p.ExitFailed
	}
	return p.ExitOK
}
