// Package wstest provides the server side of an offline WebSocket test.
package wstest

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

type Peer struct {
	net.Conn
	r *bufio.Reader
}

func Upgrade(w http.ResponseWriter, r *http.Request) (*Peer, error) {
	key := r.Header.Get("Sec-WebSocket-Key")
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(raw) != 16 || r.Header.Get("Sec-WebSocket-Version") != "13" || r.Header.Get("Sec-WebSocket-Extensions") != "" || r.Header.Get("Upgrade") != "websocket" || r.Header.Get("Connection") != "Upgrade" {
		return nil, fmt.Errorf("unexpected upgrade headers")
	}
	conn, rw, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	digest := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	_, err = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(digest[:]))
	if err == nil {
		err = rw.Flush()
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Peer{conn, rw.Reader}, nil
}

func (p *Peer) Send(first byte, data []byte) error {
	header := []byte{first}
	switch n := len(data); {
	case n < 126:
		header = append(header, byte(n))
	case n <= 65535:
		header = binary.BigEndian.AppendUint16(append(header, 126), uint16(n))
	default:
		header = binary.BigEndian.AppendUint64(append(header, 127), uint64(n))
	}
	_, err := p.Write(append(header, data...))
	return err
}

func (p *Peer) Frame() (byte, []byte, error) {
	var h [2]byte
	if _, err := io.ReadFull(p.r, h[:]); err != nil {
		return 0, nil, err
	}
	if h[1]&128 == 0 {
		return 0, nil, fmt.Errorf("unmasked client frame")
	}
	n := uint64(h[1] & 127)
	var err error
	switch n {
	case 126:
		var size uint16
		err = binary.Read(p.r, binary.BigEndian, &size)
		n = uint64(size)
	case 127:
		err = binary.Read(p.r, binary.BigEndian, &n)
	}
	if err != nil || n > 4<<20 {
		return 0, nil, fmt.Errorf("invalid client frame size")
	}
	var mask [4]byte
	if _, err = io.ReadFull(p.r, mask[:]); err != nil {
		return 0, nil, err
	}
	data := make([]byte, n)
	if _, err = io.ReadFull(p.r, data); err != nil {
		return 0, nil, err
	}
	for i := range data {
		data[i] ^= mask[i%4]
	}
	return h[0], data, nil
}
