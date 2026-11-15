// Streaming JSON scanner for dbt artifacts.
//
// dbt artifacts are large objects keyed by opaque unique_ids. encoding/json
// can stream tokens but has no "skip this value" primitive, and decoding an
// unselected compiled_code string allocates it. This scanner is a small
// bracket- and quote-aware reader over a bufio.Reader that:
//
//   - tracks the absolute byte offset, so a scan can stop after any entry
//     and resume later by seeking (the cursor is just an int64, which is what
//     makes the scan survive the HTTP transport's per-batch requests);
//   - skips a value without allocating it (skipValue);
//   - returns the raw bytes of a value it does want (readRaw), which the
//     caller then decodes with encoding/json.
//
// Nesting depth and single-value size are bounded so hostile input cannot
// exhaust the stack or memory.

package dbt

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	// maxDepth bounds JSON nesting (dbt artifacts nest < 20 deep).
	maxDepth = 512
	// maxValueBytes bounds one value read into memory (a single node's code).
	maxValueBytes = 256 << 20
)

// errMalformed marks an artifact that is not the JSON shape expected.
var errMalformed = errors.New("malformed dbt artifact")

type scanner struct {
	f   *os.File
	r   *bufio.Reader
	off int64 // absolute offset of the next unread byte
}

// openScanner opens path positioned at byte offset off.
func openScanner(path string, off int64) (*scanner, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return &scanner{f: f, r: bufio.NewReaderSize(f, 256<<10), off: off}, nil
}

func (s *scanner) Close() error { return s.f.Close() }

func (s *scanner) readByte() (byte, error) {
	b, err := s.r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0, fmt.Errorf("%w: unexpected end of file at byte %d", errMalformed, s.off)
		}
		return 0, err
	}
	s.off++
	return b, nil
}

// peek skips whitespace and returns the next significant byte without consuming it.
func (s *scanner) peek() (byte, error) {
	for {
		b, err := s.r.Peek(1)
		if err != nil {
			if err == io.EOF {
				return 0, fmt.Errorf("%w: unexpected end of file at byte %d", errMalformed, s.off)
			}
			return 0, err
		}
		switch b[0] {
		case ' ', '\t', '\n', '\r':
			_, _ = s.r.ReadByte()
			s.off++
		default:
			return b[0], nil
		}
	}
}

// expect consumes the next significant byte and checks it is c.
func (s *scanner) expect(c byte) error {
	b, err := s.peek()
	if err != nil {
		return err
	}
	if b != c {
		return fmt.Errorf("%w: expected %q at byte %d, found %q", errMalformed, c, s.off, b)
	}
	_, err = s.readByte()
	return err
}

// readString reads a JSON string (the opening quote must be next).
func (s *scanner) readString() (string, error) {
	raw, err := s.readRaw()
	if err != nil {
		return "", err
	}
	if len(raw) == 0 || raw[0] != '"' {
		return "", fmt.Errorf("%w: expected a string at byte %d", errMalformed, s.off)
	}
	var out string
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("%w: %v", errMalformed, err)
	}
	return out, nil
}

// chunk returns the buffered bytes (filling the buffer if it is empty).
func (s *scanner) chunk() ([]byte, error) {
	n := s.r.Buffered()
	if n == 0 {
		if _, err := s.r.Peek(1); err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("%w: unexpected end of file at byte %d", errMalformed, s.off)
			}
			return nil, err
		}
		n = s.r.Buffered()
	}
	return s.r.Peek(n)
}

// advance consumes n buffered bytes, copying them to sink when non-nil.
func (s *scanner) advance(buf []byte, n int, sink *[]byte) error {
	if sink != nil {
		if len(*sink)+n > maxValueBytes {
			return fmt.Errorf("%w: value larger than %d bytes", errMalformed, maxValueBytes)
		}
		*sink = append(*sink, buf[:n]...)
	}
	_, _ = s.r.Discard(n)
	s.off += int64(n)
	return nil
}

// skipString advances past a string whose opening quote was already
// consumed, scanning the buffer in bulk.
func (s *scanner) skipString(sink *[]byte) error {
	escaped := false
	for {
		buf, err := s.chunk()
		if err != nil {
			return err
		}
		i := 0
		if escaped {
			i, escaped = 1, false
		}
		for i < len(buf) {
			j := bytes.IndexAny(buf[i:], "\\\"")
			if j < 0 {
				i = len(buf)
				break
			}
			i += j
			if buf[i] == '"' {
				return s.advance(buf, i+1, sink)
			}
			// backslash: skip it and the escaped byte (which may be in the next chunk)
			if i+1 >= len(buf) {
				i = len(buf)
				escaped = true
				break
			}
			i += 2
		}
		if err := s.advance(buf, i, sink); err != nil {
			return err
		}
	}
}

// consumeValue advances past one value; when sink is non-nil the bytes are
// appended to it. With a nil sink nothing is allocated.
func (s *scanner) consumeValue(sink *[]byte) error {
	first, err := s.peek()
	if err != nil {
		return err
	}
	switch first {
	case '"':
		b, _ := s.readByte()
		if sink != nil {
			*sink = append(*sink, b)
		}
		return s.skipString(sink)
	case '{', '[':
		depth := 0
		for {
			buf, err := s.chunk()
			if err != nil {
				return err
			}
			j := bytes.IndexAny(buf, "\"{}[]")
			if j < 0 {
				if err := s.advance(buf, len(buf), sink); err != nil {
					return err
				}
				continue
			}
			c := buf[j]
			if err := s.advance(buf, j+1, sink); err != nil {
				return err
			}
			switch c {
			case '{', '[':
				depth++
				if depth > maxDepth {
					return fmt.Errorf("%w: nesting deeper than %d", errMalformed, maxDepth)
				}
			case '}', ']':
				depth--
				if depth == 0 {
					return nil
				}
				if depth < 0 {
					return fmt.Errorf("%w: unbalanced brackets at byte %d", errMalformed, s.off)
				}
			case '"':
				if err := s.skipString(sink); err != nil {
					return err
				}
			}
		}
	default:
		// number, true, false, null: read until a delimiter.
		n := 0
		for {
			b, err := s.r.Peek(1)
			if err != nil {
				if err == io.EOF && n > 0 {
					return nil
				}
				return fmt.Errorf("%w: unexpected end of file at byte %d", errMalformed, s.off)
			}
			switch b[0] {
			case ',', '}', ']', ' ', '\t', '\n', '\r', ':':
				if n == 0 {
					return fmt.Errorf("%w: unexpected %q at byte %d", errMalformed, b[0], s.off)
				}
				return nil
			}
			c, _ := s.readByte()
			n++
			if n > 64 {
				return fmt.Errorf("%w: scalar too long at byte %d", errMalformed, s.off)
			}
			if sink != nil {
				*sink = append(*sink, c)
			}
		}
	}
}

// skipValue advances past one value without allocating it.
func (s *scanner) skipValue() error { return s.consumeValue(nil) }

// readRaw returns the raw bytes of the next value.
func (s *scanner) readRaw() ([]byte, error) {
	buf := make([]byte, 0, 64)
	if err := s.consumeValue(&buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// readObjectFields reads an object (opening brace next), returning the raw
// value of every key in want (all keys when want is nil). Other keys are
// skipped without decoding.
func (s *scanner) readObjectFields(want map[string]bool) (map[string]json.RawMessage, error) {
	if err := s.expect('{'); err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{}
	first := true
	for {
		b, err := s.peek()
		if err != nil {
			return nil, err
		}
		if b == '}' {
			_, _ = s.readByte()
			return out, nil
		}
		if !first {
			if err := s.expect(','); err != nil {
				return nil, err
			}
		}
		first = false
		key, err := s.readString()
		if err != nil {
			return nil, err
		}
		if err := s.expect(':'); err != nil {
			return nil, err
		}
		if want == nil || want[key] {
			raw, err := s.readRaw()
			if err != nil {
				return nil, err
			}
			out[key] = raw
		} else if err := s.skipValue(); err != nil {
			return nil, err
		}
	}
}

// seekTopLevelKey scans the top-level object from the start of the file and
// leaves the scanner just after `"key":`. found is false when the key is absent.
func (s *scanner) seekTopLevelKey(key string) (bool, error) {
	if err := s.expect('{'); err != nil {
		return false, err
	}
	first := true
	for {
		b, err := s.peek()
		if err != nil {
			return false, err
		}
		if b == '}' {
			return false, nil
		}
		if !first {
			if err := s.expect(','); err != nil {
				return false, err
			}
		}
		first = false
		k, err := s.readString()
		if err != nil {
			return false, err
		}
		if err := s.expect(':'); err != nil {
			return false, err
		}
		if k == key {
			return true, nil
		}
		if err := s.skipValue(); err != nil {
			return false, err
		}
	}
}
