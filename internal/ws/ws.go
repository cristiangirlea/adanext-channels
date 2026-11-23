// Package ws implements the HTTP/1.1 WebSocket client used by the Discord gateway.
package ws

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const readLimit = 4 << 20

type Conn struct {
	limit     atomic.Int64
	stream    io.ReadWriteCloser
	r         *bufio.Reader
	socket    net.Conn
	readMu    sync.Mutex
	writeMu   chan struct{}
	closing   chan struct{}
	closeOnce sync.Once
}

type CloseError struct{ Code int }

func (e *CloseError) Error() string { return fmt.Sprintf("websocket closed (%d)", e.Code) }

func Dial(ctx context.Context, address string, header http.Header) (*Conn, error) {
	u, err := url.Parse(address)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return nil, errors.New("invalid websocket URL")
	}
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	default:
		return nil, errors.New("invalid websocket scheme")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, errors.New("invalid websocket request")
	}
	for k, values := range header {
		for _, v := range values {
			req.Header.Add(k, v)
		}
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("websocket nonce: %w", err)
	}
	key := base64.StdEncoding.EncodeToString(nonce[:])
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", key)
	req.Header.Del("Sec-WebSocket-Extensions")
	// Keep the transport's TLS verification and retain the socket for per-operation cancellation.
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var socket net.Conn
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { socket = info.Conn }}
	transport := &http.Transport{DialContext: dialer.DialContext, Protocols: new(http.Protocols), DisableCompression: true, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second}
	transport.Protocols.SetHTTP1(true)
	defer transport.CloseIdleConnections()
	resp, err := transport.RoundTrip(req.WithContext(httptrace.WithClientTrace(ctx, trace)))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("websocket upgrade request failed")
	}
	digest := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	stream, ok := resp.Body.(io.ReadWriteCloser)
	upgraded := resp.ProtoMajor == 1 && resp.ProtoMinor == 1 && resp.StatusCode == 101 &&
		hasToken(resp.Header.Values("Connection"), "upgrade") && strings.EqualFold(resp.Header.Get("Upgrade"), "websocket")
	accepted := len(resp.Header.Values("Sec-WebSocket-Accept")) == 1 &&
		resp.Header.Get("Sec-WebSocket-Accept") == base64.StdEncoding.EncodeToString(digest[:])
	extended := len(resp.Header.Values("Sec-WebSocket-Extensions")) != 0 || len(resp.Header.Values("Sec-WebSocket-Protocol")) != 0
	if !ok || !upgraded || !accepted || extended {
		resp.Body.Close()
		return nil, fmt.Errorf("invalid websocket upgrade (HTTP %d)", resp.StatusCode)
	}
	conn := &Conn{stream: stream, r: bufio.NewReader(stream), socket: socket,
		writeMu: make(chan struct{}, 1), closing: make(chan struct{}),
	}
	conn.SetReadLimit(readLimit)
	return conn, nil
}

// SetReadLimit bounds each reassembled message; a nonpositive limit restores the default.
func (c *Conn) SetReadLimit(n int) {
	if n <= 0 {
		n = readLimit
	}
	c.limit.Store(int64(n))
}

func hasToken(values []string, want string) bool {
	for _, value := range values {
		for token := range strings.SplitSeq(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

// Wait for a racing cancellation before clearing the deadline for the next operation.
func deadline(ctx context.Context, set func(time.Time) error) func() {
	if d, ok := ctx.Deadline(); ok {
		set(d)
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { set(time.Now()); close(done) })
	return func() {
		if !stop() {
			<-done
		}
		set(time.Time{})
	}
}

func (c *Conn) Write(ctx context.Context, data []byte) error {
	op := byte(1)
	if !utf8.Valid(data) {
		op = 2
	}
	return c.write(ctx, op, data)
}

func (c *Conn) write(ctx context.Context, op byte, data []byte) error {
	select {
	case c.writeMu <- struct{}{}:
		defer func() { <-c.writeMu }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if op != 8 {
		select {
		case <-c.closing:
			return net.ErrClosed
		default:
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	finish := deadline(ctx, c.socket.SetWriteDeadline)
	defer finish()
	frame := make([]byte, 0, 14+len(data))
	frame = append(frame, 0x80|op)
	switch n := len(data); {
	case n < 126:
		frame = append(frame, 0x80|byte(n))
	case n <= 65535:
		frame = binary.BigEndian.AppendUint16(append(frame, 0xfe), uint16(n))
	default:
		frame = binary.BigEndian.AppendUint64(append(frame, 0xff), uint64(n))
	}
	var key [4]byte
	if _, err := rand.Read(key[:]); err != nil {
		return fmt.Errorf("websocket mask: %w", err)
	}
	frame = append(frame, key[:]...)
	for i, b := range data {
		frame = append(frame, b^key[i%4])
	}
	if _, err := c.stream.Write(frame); err != nil {
		c.stream.Close()
		return fmt.Errorf("write websocket frame: %w", err)
	}
	return nil
}

type protocolError string

func (e protocolError) Error() string { return string(e) }

const tooLarge = protocolError("websocket message exceeds read limit")

func (c *Conn) frame(remaining int) (byte, bool, []byte, error) {
	var h [2]byte
	if _, err := io.ReadFull(c.r, h[:]); err != nil {
		return 0, false, nil, err
	}
	op := h[0] & 15
	fin := h[0]&0x80 != 0
	n := uint64(h[1] & 127)
	if h[0]&0x70 != 0 || h[1]&0x80 != 0 || op > 2 && op < 8 || op > 10 || op >= 8 && (!fin || n > 125) {
		return 0, false, nil, protocolError("invalid websocket frame header")
	}
	switch n {
	case 126:
		var size uint16
		if err := binary.Read(c.r, binary.BigEndian, &size); err != nil {
			return 0, false, nil, err
		}
		n = uint64(size)
		if n < 126 {
			return 0, false, nil, protocolError("invalid websocket length")
		}
	case 127:
		if err := binary.Read(c.r, binary.BigEndian, &n); err != nil {
			return 0, false, nil, err
		}
		if n <= 65535 || n>>63 != 0 {
			return 0, false, nil, protocolError("invalid websocket length")
		}
	}
	if op < 8 && n > uint64(remaining) {
		return 0, false, nil, tooLarge
	}
	data := make([]byte, n)
	_, err := io.ReadFull(c.r, data)
	return op, fin, data, err
}

func (c *Conn) Read(ctx context.Context) (_ []byte, readErr error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	select {
	case <-c.closing:
		return nil, net.ErrClosed
	default:
	}
	// A failed read may already have consumed part of a frame or fragmented message.
	defer func() {
		if readErr != nil {
			c.closeOnce.Do(func() { close(c.closing) })
			c.stream.Close()
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	finish := deadline(ctx, c.socket.SetReadDeadline)
	defer finish()
	var data []byte
	var kind byte
	limit := int(c.limit.Load())
	for {
		op, fin, payload, err := c.frame(limit - len(data))
		if err != nil {
			var invalid protocolError
			if errors.As(err, &invalid) {
				code := 1002
				if invalid == tooLarge {
					code = 1009
				}
				c.shutdown(code, "")
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("read websocket frame: %w", err)
		}
		switch op {
		case 8:
			code := 1005
			if len(payload) == 1 {
				c.shutdown(1002, "")
				return nil, errors.New("invalid websocket close payload")
			}
			if len(payload) >= 2 {
				code = int(binary.BigEndian.Uint16(payload))
				if !validCode(code) || !utf8.Valid(payload[2:]) {
					c.shutdown(1002, "")
					return nil, errors.New("invalid websocket close payload")
				}
			}
			c.sendClose(payload)
			c.stream.Close()
			return nil, &CloseError{code}
		case 9:
			if err := c.write(ctx, 10, payload); err != nil {
				return nil, err
			}
			continue
		case 10:
			continue
		case 0:
			if kind == 0 {
				c.shutdown(1002, "")
				return nil, errors.New("unexpected websocket continuation")
			}
		default:
			if kind != 0 {
				c.shutdown(1002, "")
				return nil, errors.New("interrupted websocket message")
			}
			kind = op
		}
		data = append(data, payload...)
		if fin {
			if kind == 1 && !utf8.Valid(data) {
				c.shutdown(1007, "")
				return nil, errors.New("invalid websocket text")
			}
			return data, nil
		}
	}
}

func validCode(code int) bool {
	return code >= 3000 && code < 5000 || code >= 1000 && code <= 1014 && code != 1004 && code != 1005 && code != 1006
}

func (c *Conn) sendClose(payload []byte) {
	c.closeOnce.Do(func() {
		close(c.closing)
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		c.write(ctx, 8, payload)
	})
}

func closePayload(code int, reason string) []byte {
	if !validCode(code) || !utf8.ValidString(reason) || len(reason) > 123 {
		code = 1002
		reason = ""
	}
	return append(binary.BigEndian.AppendUint16(nil, uint16(code)), reason...)
}

func (c *Conn) shutdown(code int, reason string) {
	c.sendClose(closePayload(code, reason))
	c.stream.Close()
}

func (c *Conn) Close(code int, reason string) {
	// Bound the handshake even if the peer stops reading or never answers.
	timer := time.AfterFunc(250*time.Millisecond, func() { c.socket.Close() })
	defer timer.Stop()
	defer c.stream.Close()
	c.sendClose(closePayload(code, reason))
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		op, _, _, err := c.frame(readLimit)
		if err != nil || op == 8 {
			return
		}
	}
}
