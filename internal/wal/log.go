package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"baseDB/internal/page"
	"baseDB/internal/pager"
)

// Log file layout:
//
//	[file header, 24 bytes][record][record]...
//
// File header: magic "BASEWAL1" | version u32 | reserved u32 | startLSN u64.
//
// LSNs keep increasing for the life of the database, also across log
// truncations (Reset). The first record in the file has LSN startLSN, and in
// general:
//
//	LSN = startLSN + (file offset - header size)
//
// A brand-new log has startLSN = header size, so LSN = file offset. No
// record ever has LSN 0, so a pageLSN of 0 means "never logged".
const (
	logMagic       = "BASEWAL1"
	logVersion     = 2
	fileHeaderSize = 24
)

// ErrBadLogFile means the file is not a baseDB log.
var ErrBadLogFile = errors.New("wal: not a valid log file")

// Log is an append-only write-ahead log. Records are first collected in an
// in-memory buffer by Append and become durable with Flush/FlushUntil, which
// write the buffer and fsync the file.
//
// Log implements buffer.LogFlusher, so the buffer pool can enforce the WAL
// rule (a page reaches disk only after the log up to its pageLSN).
//
// It is safe for concurrent use.
type Log struct {
	mu      sync.Mutex
	path    string
	file    *pager.LogFile
	start   page.LSN // LSN of the first record in the file
	buf     []byte   // encoded records not yet written to the file
	bufLSN  page.LSN // LSN where buf[0] goes
	durable page.LSN // every record before this LSN is written and synced
	nextTxn uint64
	tailErr error // why Open discarded the end of the log, if it did
	noSync  bool
}

// Options configures a Log.
type Options struct {
	// NoSync skips the fsync in Flush. Records are still written and
	// checksummed, but a power failure can lose recent commits. Only for
	// tests that check data structure logic, not durability.
	NoSync bool
}

// Open opens the log at path with default options (fsync on every flush).
// An existing log is scanned from the start: everything up to the last
// valid record is kept, and an incomplete or corrupt tail (left by a crash)
// is cut off so that new records are never appended after garbage.
// TailError reports what was cut.
func Open(path string) (*Log, error) {
	return OpenWithOptions(path, Options{})
}

// OpenWithOptions is Open with explicit options.
func OpenWithOptions(path string, opts Options) (*Log, error) {
	// A crash during Reset may have left a half-written replacement behind.
	if err := pager.RemoveStaleLogTemp(path); err != nil {
		return nil, err
	}
	file, err := pager.OpenLogFile(path)
	if err != nil {
		return nil, err
	}
	l := &Log{path: path, file: file, nextTxn: 1, noSync: opts.NoSync}
	if err := l.init(); err != nil {
		file.Close()
		return nil, err
	}
	return l, nil
}

func encodeHeader(start page.LSN) []byte {
	h := make([]byte, fileHeaderSize)
	copy(h, logMagic)
	binary.LittleEndian.PutUint32(h[8:], logVersion)
	binary.LittleEndian.PutUint64(h[16:], uint64(start))
	return h
}

func (l *Log) offset(lsn page.LSN) int64 { return int64(lsn-l.start) + fileHeaderSize }

func (l *Log) init() error {
	size, err := l.file.Size()
	if err != nil {
		return err
	}
	if size == 0 {
		if err := l.file.WriteAt(encodeHeader(fileHeaderSize), 0); err != nil {
			return err
		}
		if err := l.file.Sync(); err != nil {
			return err
		}
		l.start, l.bufLSN, l.durable = fileHeaderSize, fileHeaderSize, fileHeaderSize
		return nil
	}

	if l.start, err = readHeader(l.file, size); err != nil {
		return err
	}
	end, scanErr := scan(l.file, l.start, size, func(r Record) error {
		l.nextTxn = max(l.nextTxn, r.TxnID+1)
		return nil
	})
	if scanErr != nil {
		if !errors.Is(scanErr, ErrIncomplete) && !errors.Is(scanErr, ErrCorrupt) {
			return scanErr // an I/O error, not a damaged log
		}
		l.tailErr = scanErr
		if err := l.file.Truncate(l.offset(end)); err != nil {
			return err
		}
		if err := l.file.Sync(); err != nil {
			return err
		}
	}
	l.bufLSN, l.durable = end, end
	return nil
}

// readHeader validates the file header and returns the start LSN.
func readHeader(file *pager.LogFile, size int64) (page.LSN, error) {
	var h [fileHeaderSize]byte
	if size < fileHeaderSize {
		return 0, fmt.Errorf("%w: file too short", ErrBadLogFile)
	}
	if _, err := file.ReadAt(h[:], 0); err != nil {
		return 0, fmt.Errorf("wal: read header: %w", err)
	}
	if string(h[:8]) != logMagic {
		return 0, fmt.Errorf("%w: bad magic", ErrBadLogFile)
	}
	if v := binary.LittleEndian.Uint32(h[8:]); v != logVersion {
		return 0, fmt.Errorf("%w: version %d, want %d", ErrBadLogFile, v, logVersion)
	}
	start := page.LSN(binary.LittleEndian.Uint64(h[16:]))
	if start < fileHeaderSize {
		return 0, fmt.Errorf("%w: start LSN %d", ErrBadLogFile, start)
	}
	return start, nil
}

// scan reads the records of a file whose first record has LSN start, up to
// file size size, and calls fn for each valid one. It stops at the first
// invalid record and returns the LSN just after the last valid record, with
// ErrIncomplete or ErrCorrupt saying why it stopped early (nil if it reached
// size cleanly).
//
// After an invalid record nothing can be trusted (its length field may be
// garbage, so the next record cannot even be located), so scanning never
// skips ahead.
func scan(file *pager.LogFile, start page.LSN, size int64, fn func(Record) error) (page.LSN, error) {
	var lenBuf [4]byte
	off := int64(fileHeaderSize)
	lsn := func() page.LSN { return start + page.LSN(off-fileHeaderSize) }
	for off < size {
		if size-off < 4 {
			return lsn(), fmt.Errorf("%w at LSN %d", ErrIncomplete, lsn())
		}
		if _, err := file.ReadAt(lenBuf[:], off); err != nil {
			return lsn(), fmt.Errorf("wal: read at %d: %w", off, err)
		}
		length := int64(binary.LittleEndian.Uint32(lenBuf[:]))
		if length < recordHeaderSize || length > MaxRecordSize {
			return lsn(), fmt.Errorf("%w at LSN %d: length %d", ErrCorrupt, lsn(), length)
		}
		if off+length > size {
			return lsn(), fmt.Errorf("%w at LSN %d", ErrIncomplete, lsn())
		}
		buf := make([]byte, length)
		if _, err := file.ReadAt(buf, off); err != nil {
			return lsn(), fmt.Errorf("wal: read at %d: %w", off, err)
		}
		r, err := decodeRecord(buf, lsn())
		if err != nil {
			return lsn(), err
		}
		if err := fn(r); err != nil {
			return lsn(), err
		}
		off += length
	}
	return lsn(), nil
}

// TailError returns why Open cut off the end of the log (wrapping
// ErrIncomplete or ErrCorrupt), or nil if the log was intact.
func (l *Log) TailError() error { return l.tailErr }

// StartLSN returns the LSN of the oldest record still in the log. Changes
// before it are only in the data file. A page whose pageLSN is below it has
// not been changed since the last checkpoint, so its next change is logged
// as a full page image (see package mtr).
func (l *Log) StartLSN() page.LSN {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.start
}

// Size returns the size of the log in bytes, including records not yet
// flushed.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.offset(l.bufLSN) + int64(len(l.buf))
}

// BeginTxn returns a new transaction ID. IDs keep increasing across
// restarts: Open continues after the largest ID found in the log.
func (l *Log) BeginTxn() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	id := l.nextTxn
	l.nextTxn++
	return id
}

// Append assigns LSNs to recs (in order, back to back) and adds them to the
// log buffer. They are not durable until FlushUntil covers them. Records
// appended in one call are contiguous in the log.
func (l *Log) Append(recs ...*Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendLocked(recs)
}

func (l *Log) appendLocked(recs []*Record) error {
	start := len(l.buf)
	for _, r := range recs {
		r.LSN = l.bufLSN + page.LSN(len(l.buf))
		var err error
		if l.buf, err = r.appendTo(l.buf); err != nil {
			l.buf = l.buf[:start] // all or nothing
			return err
		}
	}
	return nil
}

// FlushUntil makes the record at lsn, and everything before it, durable.
// It is a no-op if that is already the case (in particular for lsn 0, a
// page that was never logged, and for LSNs before the start of the log).
func (l *Log) FlushUntil(lsn page.LSN) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if lsn < l.durable {
		return nil
	}
	if end := l.bufLSN + page.LSN(len(l.buf)); lsn >= end {
		return fmt.Errorf("wal: flush to LSN %d, but the log ends at %d", lsn, end)
	}
	return l.flushLocked()
}

// Flush makes every appended record durable.
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.flushLocked()
}

// flushLocked writes the buffer and fsyncs. Flushes always end at a record
// boundary, so "lsn < durable" means the whole record at lsn is durable.
func (l *Log) flushLocked() error {
	if len(l.buf) > 0 {
		if err := l.file.WriteAt(l.buf, l.offset(l.bufLSN)); err != nil {
			return err
		}
		l.bufLSN += page.LSN(len(l.buf))
		l.buf = l.buf[:0]
	}
	if l.durable == l.bufLSN {
		return nil
	}
	if !l.noSync {
		if err := l.file.Sync(); err != nil {
			return err
		}
	}
	l.durable = l.bufLSN
	return nil
}

// DurableLSN returns the LSN up to which the log is durable.
func (l *Log) DurableLSN() page.LSN {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.durable
}

// Reset is the log's part of a checkpoint. The caller must have made every
// change in the log durable in the data file (all dirty pages written, the
// data file and its meta page synced), and must not run transactions
// concurrently. Reset then replaces the log with a new one that:
//
//   - continues at the current end LSN, so LSNs never go backwards, and
//   - contains one committed transaction with a META record holding meta,
//     so every log always has a META record from which a torn meta page can
//     be rebuilt.
//
// The replacement is atomic (pager.ReplaceLogFile): a crash leaves either
// the old log (recovery then redoes changes that are already on disk, which
// is harmless) or the new one.
func (l *Log) Reset(meta page.Meta) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.flushLocked(); err != nil {
		return err
	}

	start := l.bufLSN
	txn := l.nextTxn
	recs := []*Record{
		{Type: TypeBegin, TxnID: txn},
		{Type: TypeMeta, TxnID: txn, Meta: meta},
		{Type: TypeCommit, TxnID: txn},
	}
	var body []byte
	lsn := start
	for _, r := range recs {
		r.LSN = lsn
		before := len(body)
		var err error
		if body, err = r.appendTo(body); err != nil {
			return err
		}
		lsn += page.LSN(len(body) - before)
	}

	if err := l.file.Close(); err != nil {
		return err
	}
	replaceErr := pager.ReplaceLogFile(l.path, append(encodeHeader(start), body...))
	file, err := pager.OpenLogFile(l.path)
	if err != nil {
		return errors.Join(replaceErr, err)
	}
	l.file = file
	if replaceErr != nil {
		// The old file may or may not have been replaced: re-read whatever
		// is there.
		l.buf, l.tailErr = l.buf[:0], nil
		return errors.Join(replaceErr, l.init())
	}
	l.nextTxn++
	l.start, l.bufLSN, l.durable = start, lsn, lsn
	l.tailErr = nil
	return nil
}

// Close flushes and closes the log.
func (l *Log) Close() error {
	err := l.Flush()
	if cerr := l.file.Close(); err == nil {
		err = cerr
	}
	return err
}

// Abandon closes the file without flushing the buffer, as if the process had
// died at this moment. For crash tests only.
func (l *Log) Abandon() error {
	return l.file.Close()
}

// Txn is a committed transaction read back from the log.
type Txn struct {
	ID        uint64
	Records   []Record // PAGE and META records, in log order
	CommitLSN page.LSN
}

// Transactions returns the committed transactions in the durable part of
// the log, in commit order. Transactions without a COMMIT record (cut off by
// a crash) are not returned: their changes never happened.
func (l *Log) Transactions() ([]Txn, error) {
	txns, _, err := l.TransactionsWithOpen()
	return txns, err
}

// TransactionsWithOpen is Transactions, plus the number of transactions that
// were begun in the log but never committed (cut off by a crash).
func (l *Log) TransactionsWithOpen() ([]Txn, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return transactions(l.file, l.start, l.offset(l.durable))
}

// ReadTransactions reads the committed transactions of the log at path
// without opening it for writing (nothing is truncated).
func ReadTransactions(path string) ([]Txn, error) {
	file, err := pager.OpenLogFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	size, err := file.Size()
	if err != nil {
		return nil, err
	}
	start, err := readHeader(file, size)
	if err != nil {
		return nil, err
	}
	txns, _, err := transactions(file, start, size)
	if errors.Is(err, ErrIncomplete) {
		err = nil // a torn tail is expected after a crash
	}
	return txns, err
}

// transactions groups the records of the file by transaction and checks the
// grouping: every transaction starts with BEGIN, every PAGE/META record
// belongs to an open transaction, and nothing follows a COMMIT. It also
// returns how many transactions were begun but never committed.
func transactions(file *pager.LogFile, start page.LSN, size int64) ([]Txn, int, error) {
	open := map[uint64]*Txn{}
	done := map[uint64]bool{}
	var out []Txn
	_, err := scan(file, start, size, func(r Record) error {
		fail := func(msg string) error {
			return fmt.Errorf("%w at LSN %d: txn %d: %s", ErrCorrupt, r.LSN, r.TxnID, msg)
		}
		t := open[r.TxnID]
		switch r.Type {
		case TypeBegin:
			if t != nil || done[r.TxnID] {
				return fail("duplicate BEGIN")
			}
			open[r.TxnID] = &Txn{ID: r.TxnID}
		case TypePage, TypeMeta:
			if t == nil {
				return fail(r.Type.String() + " outside a transaction")
			}
			t.Records = append(t.Records, r)
		case TypeCommit:
			if t == nil {
				return fail("COMMIT without BEGIN")
			}
			t.CommitLSN = r.LSN
			out = append(out, *t)
			delete(open, r.TxnID)
			done[r.TxnID] = true
		}
		return nil
	})
	return out, len(open), err
}
