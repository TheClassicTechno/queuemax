package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func openTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

// writeRaw writes exactly b to a fresh WAL file at path, for tests that
// need to craft torn/corrupted state that Append can never produce.
func writeRaw(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write raw WAL bytes: %v", err)
	}
}

func TestAppendReplaySingle(t *testing.T) {
	w, _ := openTestWAL(t)

	if err := w.Append(OpEnqueue, []byte("hello")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	var got []Record
	n, err := w.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if n != 1 {
		t.Fatalf("n = %d, want 1", n)
	}
	if len(got) != 1 || got[0].Op != OpEnqueue || string(got[0].Payload) != "hello" {
		t.Fatalf("got %+v", got)
	}
}

func TestAppendReplayMany(t *testing.T) {
	w, _ := openTestWAL(t)

	const count = 200
	for i := 0; i < count; i++ {
		op := OpEnqueue
		if i%3 == 0 {
			op = OpAck
		}
		if err := w.Append(op, []byte(fmt.Sprintf("record-%d", i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	var got []Record
	n, err := w.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if n != count {
		t.Fatalf("n = %d, want %d", n, count)
	}
	for i, r := range got {
		want := fmt.Sprintf("record-%d", i)
		if string(r.Payload) != want {
			t.Fatalf("record %d payload = %q, want %q", i, r.Payload, want)
		}
	}
}

func TestCloseReopenReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := w.Append(OpEnqueue, []byte{byte(i)}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	n, err := w2.Replay(func(Record) error { return nil })
	if err != nil {
		t.Fatalf("Replay after reopen: %v", err)
	}
	if n != 5 {
		t.Fatalf("n = %d, want 5", n)
	}
}

func TestReplayTruncatedHeaderAtTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	rec1 := encodeRecord(OpEnqueue, []byte("a"))
	rec2 := encodeRecord(OpAck, []byte("b"))
	partialHeader := encodeRecord(OpEnqueue, []byte("c"))[:5] // half of a 10-byte header

	raw := append(append(append([]byte{}, rec1...), rec2...), partialHeader...)
	writeRaw(t, path, raw)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	n, err := w.Replay(func(Record) error { return nil })
	if err != nil {
		t.Fatalf("Replay: got error %v, want nil (silent truncate)", err)
	}
	if n != 2 {
		t.Fatalf("n = %d, want 2", n)
	}
}

func TestReplayTruncatedBodyAtTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	rec1 := encodeRecord(OpEnqueue, []byte("a"))
	rec2 := encodeRecord(OpAck, []byte("b"))
	rec3Full := encodeRecord(OpEnqueue, []byte("ccccc"))
	rec3Partial := rec3Full[:headerSize+2] // full header, only 2 of the body+checksum bytes

	raw := append(append(append([]byte{}, rec1...), rec2...), rec3Partial...)
	writeRaw(t, path, raw)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	n, err := w.Replay(func(Record) error { return nil })
	if err != nil {
		t.Fatalf("Replay: got error %v, want nil (silent truncate)", err)
	}
	if n != 2 {
		t.Fatalf("n = %d, want 2", n)
	}
}

func TestReplayChecksumCorruptionAtTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	rec1 := encodeRecord(OpEnqueue, []byte("a"))
	rec2 := encodeRecord(OpAck, []byte("b"))
	rec3 := encodeRecord(OpEnqueue, []byte("ccccc"))
	rec3[headerSize] ^= 0xFF // flip a payload byte so the checksum no longer matches

	raw := append(append(append([]byte{}, rec1...), rec2...), rec3...)
	writeRaw(t, path, raw)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	n, err := w.Replay(func(Record) error { return nil })
	if err != nil {
		t.Fatalf("Replay: got error %v, want nil (silent truncate)", err)
	}
	if n != 2 {
		t.Fatalf("n = %d, want 2 (corrupted record excluded)", n)
	}
}

func TestReplayChecksumCorruptionMidLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	rec1 := encodeRecord(OpEnqueue, []byte("a"))
	rec2 := encodeRecord(OpAck, []byte("b"))
	rec3 := encodeRecord(OpEnqueue, []byte("ccccc"))
	rec3[headerSize] ^= 0xFF // corrupt rec3's payload
	rec4 := encodeRecord(OpEnqueue, []byte("d"))

	raw := append(append(append(append([]byte{}, rec1...), rec2...), rec3...), rec4...)
	writeRaw(t, path, raw)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	n, err := w.Replay(func(Record) error { return nil })
	if err == nil {
		t.Fatalf("Replay: got nil error, want hard error (corruption followed by more data)")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Replay error = %v, want it to wrap ErrCorrupt", err)
	}
	if n != 2 {
		t.Fatalf("n = %d, want 2 (only the valid prefix)", n)
	}
}

func TestReplayUnreasonableLengthAtTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	rec1 := encodeRecord(OpEnqueue, []byte("a"))
	rec2 := encodeRecord(OpAck, []byte("b"))

	badHeader := make([]byte, headerSize)
	copy(badHeader[0:4], magic[:])
	badHeader[4] = currentVersion
	badHeader[5] = byte(OpEnqueue)
	binary.BigEndian.PutUint32(badHeader[6:10], MaxPayloadBytes+1)

	raw := append(append(append([]byte{}, rec1...), rec2...), badHeader...) // nothing follows the header
	writeRaw(t, path, raw)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	n, err := w.Replay(func(Record) error { return nil })
	if err != nil {
		t.Fatalf("Replay: got error %v, want nil (silent truncate)", err)
	}
	if n != 2 {
		t.Fatalf("n = %d, want 2", n)
	}
}

func TestReplayUnreasonableLengthMidLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	rec1 := encodeRecord(OpEnqueue, []byte("a"))
	rec2 := encodeRecord(OpAck, []byte("b"))

	badHeader := make([]byte, headerSize)
	copy(badHeader[0:4], magic[:])
	badHeader[4] = currentVersion
	badHeader[5] = byte(OpEnqueue)
	binary.BigEndian.PutUint32(badHeader[6:10], MaxPayloadBytes+1)

	raw := append(append(append([]byte{}, rec1...), rec2...), badHeader...)
	raw = append(raw, 0x00) // one more byte follows the bogus header

	writeRaw(t, path, raw)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	n, err := w.Replay(func(Record) error { return nil })
	if err == nil {
		t.Fatalf("Replay: got nil error, want hard error (unreasonable length with data behind it)")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Replay error = %v, want it to wrap ErrCorrupt", err)
	}
	if n != 2 {
		t.Fatalf("n = %d, want 2 (only the valid prefix)", n)
	}
}

func TestReplayValidPrefixDamagedTailViaTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 4; i++ {
		if err := w.Append(OpEnqueue, []byte(fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	// Simulate a crash that tore off the tail of the last record's checksum.
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	var got []Record
	n, err := w2.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: got error %v, want nil (silent truncate)", err)
	}
	if n != 3 {
		t.Fatalf("n = %d, want 3 (last record torn off)", n)
	}
	for i, r := range got {
		want := fmt.Sprintf("rec-%d", i)
		if string(r.Payload) != want {
			t.Fatalf("record %d payload = %q, want %q", i, r.Payload, want)
		}
	}
}

func TestAppendRejectsOversizedPayload(t *testing.T) {
	w, _ := openTestWAL(t)

	oversized := make([]byte, MaxPayloadBytes+1)
	if err := w.Append(OpEnqueue, oversized); err == nil {
		t.Fatal("Append: got nil error for oversized payload, want error")
	}
}
