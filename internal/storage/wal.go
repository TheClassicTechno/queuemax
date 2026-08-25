package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// WAL is a simple append-only, file-backed write-ahead log.
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// Open opens (creating if needed) the WAL file at path.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Append durably writes one record: it returns nil only after the record
// has been written to the file and fsynced (DESIGN_DECISIONS.md #6/#7).
func (w *WAL) Append(op OpType, payload []byte) error {
	if len(payload) > MaxPayloadBytes {
		return fmt.Errorf("storage: payload of %d bytes exceeds MaxPayloadBytes (%d)", len(payload), MaxPayloadBytes)
	}

	buf := encodeRecord(op, payload)

	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("storage: write record: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("storage: short write: wrote %d of %d bytes", n, len(buf))
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("storage: sync: %w", err)
	}

	return nil
}

// ErrCorrupt is returned (wrapped) by Replay when it encounters WAL
// corruption that cannot be explained by an ordinary crash mid-write.
// See DESIGN_DECISIONS.md #11.
var ErrCorrupt = errors.New("storage: WAL corruption detected")

// Replay reads every valid record from the start of the WAL, in order,
// invoking fn for each one, and returns the number of records replayed.
//
// A record truncated at the physical end of the file — a partial header,
// a partial body, or a checksum mismatch with nothing following it — is
// treated as an expected crash artifact: Replay stops there and returns
// (n, nil). Corruption anywhere else (bad magic, unsupported version, an
// unreasonable length or checksum mismatch with more data behind it) is
// not explainable by an ordinary crash, so Replay returns a non-nil error
// wrapping ErrCorrupt instead of silently skipping it.
func (w *WAL) Replay(fn func(Record) error) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("storage: seek to start: %w", err)
	}

	count := 0
	header := make([]byte, headerSize)

	for {
		_, err := io.ReadFull(w.file, header)
		if errors.Is(err, io.EOF) {
			break // clean end of log
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break // truncated header at the physical tail
		}
		if err != nil {
			return count, fmt.Errorf("storage: read header: %w", err)
		}

		if !bytes.Equal(header[0:4], magic[:]) {
			return count, fmt.Errorf("%w: bad magic at record %d", ErrCorrupt, count)
		}

		h := decodeHeader(header)

		if h.version != currentVersion {
			return count, fmt.Errorf("%w: unsupported version %d at record %d", ErrCorrupt, h.version, count)
		}

		if !h.op.valid() {
			// A checksum-consistent record with an unrecognized op can't
			// be explained by an ordinary crash (a crash truncates bytes;
			// it doesn't produce internally-consistent records for op
			// values nothing ever wrote) — so, unlike length/checksum
			// errors below, this gets no tail-truncation leniency.
			return count, fmt.Errorf("%w: unknown op %d at record %d", ErrCorrupt, h.op, count)
		}

		if h.length > MaxPayloadBytes {
			more, err := w.hasMoreData()
			if err != nil {
				return count, err
			}
			if !more {
				break // truncated tail: unreasonable length but nothing follows
			}
			return count, fmt.Errorf("%w: unreasonable length %d at record %d", ErrCorrupt, h.length, count)
		}

		body := make([]byte, h.length+footerSize)
		_, err = io.ReadFull(w.file, body)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			break // truncated body at the physical tail
		}
		if err != nil {
			return count, fmt.Errorf("storage: read body: %w", err)
		}

		payload := body[:h.length]
		wantSum := binary.BigEndian.Uint32(body[h.length:])
		gotSum := checksum(h.version, h.op, h.length, payload)

		if gotSum != wantSum {
			more, err := w.hasMoreData()
			if err != nil {
				return count, err
			}
			if !more {
				break // checksum mismatch at the physical tail: expected crash artifact
			}
			return count, fmt.Errorf("%w: checksum mismatch at record %d", ErrCorrupt, count)
		}

		if err := fn(Record{Op: h.op, Payload: payload}); err != nil {
			return count, err
		}
		count++
	}

	return count, nil
}

// hasMoreData reports whether at least one more byte remains in the file
// at the current read position.
func (w *WAL) hasMoreData() (bool, error) {
	var b [1]byte
	_, err := io.ReadFull(w.file, b[:])
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("storage: peek: %w", err)
	}
	return true, nil
}
