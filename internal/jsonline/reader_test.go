package jsonline

import (
	"bufio"
	"errors"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
	"io"
	"strings"
	"testing"
)

func TestLineBoundaries(t *testing.T) {
	for _, end := range []string{"\n", "\r\n"} {
		r := bufio.NewReaderSize(strings.NewReader(strings.Repeat("a", p.MaxLine)+end+strings.Repeat("b", p.MaxLine+1)+end+"ok"+end+"incomplete"), 127)
		line, err := Read(r)
		if err != nil || len(line) != p.MaxLine {
			t.Fatal(len(line), err)
		}
		if _, err = Read(r); !errors.Is(err, ErrTooLong) {
			t.Fatal(err)
		}
		if line, err = Read(r); string(line) != "ok" || err != nil {
			t.Fatal(string(line), err)
		}
		if _, err = Read(r); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(err)
		}
	}
}
