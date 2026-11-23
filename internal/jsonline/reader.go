// Package jsonline reads bounded protocol or service records from a stream.
package jsonline

import (
	"bufio"
	"errors"
	"io"

	"github.com/cristiangirlea/harness/contracts/pluginproto"
)

var ErrTooLong = errors.New("line exceeds protocol limit")

// Read drains an oversized record so the next read starts at a line boundary.
func Read(r *bufio.Reader) ([]byte, error) {
	var record []byte
	oversized := false
	for {
		chunk, err := r.ReadSlice('\n')
		if len(record)+len(chunk) > pluginproto.MaxLine+2 {
			oversized = true
		}
		if !oversized {
			record = append(record, chunk...)
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			if err == io.EOF && (len(record) > 0 || oversized) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if oversized {
			return nil, ErrTooLong
		}
		record = record[:len(record)-1]
		if n := len(record); n > 0 && record[n-1] == '\r' {
			record = record[:n-1]
		}
		if len(record) > pluginproto.MaxLine {
			return nil, ErrTooLong
		}
		return record, nil
	}
}
