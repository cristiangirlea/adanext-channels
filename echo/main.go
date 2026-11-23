package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

func main() {
	dir := flag.String("dir", ".", "directory holding the echo service files")
	flag.Parse()
	os.Exit(serve(context.Background(), *dir, os.Stdin, os.Stdout, os.Stderr))
}

func serve(ctx context.Context, dir string, in io.Reader, out, stderr io.Writer) int {
	logger := &sdk.Logger{Writer: stderr}
	err := sdk.Run(ctx, sdk.Info{Name: "echo", Label: "Echo", Version: "1"}, &directory{dir: dir}, in, out, logger)
	if errors.Is(err, sdk.ErrRefused) {
		return p.ExitRefused
	}
	if err != nil {
		logger.Printf("echo: %v", err)
		return p.ExitFailed
	}
	return p.ExitOK
}
