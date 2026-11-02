// Package protocol is baseDB's network protocol: a small, line-based text
// protocol in the spirit of Redis' inline commands or memcached.
//
// Request: one line, a command and its arguments, split like the terminal
// program splits them (spaces; double quotes with escapes for spaces and
// any byte, see Tokenize):
//
//	put user:1 "Batuhan Deniz"\n
//
// Response: zero or more ROW lines, then exactly one status line:
//
//	ROW <key> <value>      a key/value pair (scan, prefix) or a name/text
//	                       pair (stats), both always quoted
//	OK [text]              success, with an optional message
//	VALUE <value>          the value of a get (quoted)
//	NOT_FOUND              get/delete of a missing key
//	ERR <CODE> <message>   failure; CODE is one of the Code* constants
//
// Every line ends with \n. Keys and values are always quoted on ROW and
// VALUE lines, so they can contain anything (newlines become \n) and a
// line never needs to be split in a special way. The protocol can be typed
// by hand with telnet or nc.
//
// See docs/ARCHITECTURE.md (week 8).
package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Status of a response.
const (
	StatusOK       = "OK"
	StatusValue    = "VALUE"
	StatusNotFound = "NOT_FOUND"
	StatusErr      = "ERR"
)

// Error codes of ERR responses.
const (
	CodeSyntax   = "SYNTAX"   // the line could not be split (bad quote/escape)
	CodeUnknown  = "UNKNOWN"  // unknown command
	CodeUsage    = "USAGE"    // wrong number of arguments
	CodeTooLarge = "TOOLARGE" // key, value or transaction over its limit
	CodeTxOpen   = "TXOPEN"   // begin while a transaction is open
	CodeNoTx     = "NOTX"     // commit/rollback without a transaction
	CodeDeadlock = "DEADLOCK" // deadlock victim: the transaction was rolled back
	CodeTxFailed = "TXFAILED" // commit failed: nothing applied, transaction over
	CodeClosed   = "CLOSED"   // the database is shutting down
	CodeBusy     = "BUSY"     // too many connections
	CodeInternal = "INTERNAL" // anything else
)

// MaxLine is the longest request or response line accepted. A key of 256
// bytes and a value of 1 KiB need at most 4 bytes per byte when escaped, so
// real lines stay far below it.
const MaxLine = 64 << 10

// ErrLineTooLong means a line was longer than MaxLine.
var ErrLineTooLong = fmt.Errorf("protocol: line longer than %d bytes", MaxLine)

// Row is one ROW line.
type Row struct{ Key, Value []byte }

// Response is one complete response.
type Response struct {
	Rows    []Row
	Status  string // StatusOK, StatusValue, StatusNotFound or StatusErr
	Code    string // for StatusErr
	Message string // text after OK / ERR code
	Value   []byte // for StatusValue
}

// OK returns an OK response with an optional message.
func OK(message string) Response { return Response{Status: StatusOK, Message: message} }

// Value returns a VALUE response.
func Value(v []byte) Response { return Response{Status: StatusValue, Value: v} }

// NotFound returns a NOT_FOUND response.
func NotFound() Response { return Response{Status: StatusNotFound} }

// Err returns an ERR response.
func Err(code, format string, a ...any) Response {
	return Response{Status: StatusErr, Code: code, Message: fmt.Sprintf(format, a...)}
}

// Error is the Go error for an ERR response.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }

// Is makes errors.Is match on the code: errors.Is(err, &Error{Code: X}).
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// Err returns the response as an error (nil unless it is an ERR response).
func (r Response) Err() error {
	if r.Status != StatusErr {
		return nil
	}
	return &Error{Code: r.Code, Message: r.Message}
}

// Line encodes a request (command and arguments) as one line, quoting the
// arguments so that Tokenize gives them back exactly.
func Line(args ...string) string {
	var b strings.Builder
	for i, a := range args {
		if i > 0 {
			b.WriteByte(' ')
		}
		if i == 0 {
			b.WriteString(a) // the command name
		} else {
			b.WriteString(Quote([]byte(a)))
		}
	}
	b.WriteByte('\n')
	return b.String()
}

// Write encodes r onto w.
func (r Response) Write(w io.Writer) error {
	var b strings.Builder
	for _, row := range r.Rows {
		b.WriteString("ROW ")
		b.WriteString(Quote(row.Key))
		b.WriteByte(' ')
		b.WriteString(Quote(row.Value))
		b.WriteByte('\n')
	}
	switch r.Status {
	case StatusValue:
		b.WriteString("VALUE " + Quote(r.Value))
	case StatusNotFound:
		b.WriteString(StatusNotFound)
	case StatusErr:
		b.WriteString("ERR " + r.Code + " " + oneLine(r.Message))
	default:
		b.WriteString(StatusOK)
		if r.Message != "" {
			b.WriteString(" " + oneLine(r.Message))
		}
	}
	b.WriteByte('\n')
	_, err := io.WriteString(w, b.String())
	return err
}

// oneLine keeps a free-text message on one line.
func oneLine(s string) string { return strings.NewReplacer("\n", " ", "\r", " ").Replace(s) }

// ReadLine reads one line (without the trailing \n or \r\n). A line longer
// than MaxLine is an error (ErrLineTooLong). r must have a buffer of at
// least MaxLine bytes (bufio.NewReaderSize(conn, MaxLine)).
func ReadLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return "", ErrLineTooLong
	}
	if err != nil {
		if err == io.EOF && len(line) > 0 {
			return strings.TrimRight(string(line), "\r"), nil // last line without \n
		}
		return "", err
	}
	return strings.TrimRight(string(line[:len(line)-1]), "\r"), nil
}

// ReadResponse reads one complete response.
func ReadResponse(r *bufio.Reader) (Response, error) {
	var resp Response
	for {
		line, err := ReadLine(r)
		if err != nil {
			return Response{}, err
		}
		word, rest, _ := strings.Cut(line, " ")
		switch word {
		case "ROW":
			args, err := Tokenize(rest)
			if err != nil || len(args) != 2 {
				return Response{}, fmt.Errorf("protocol: bad ROW line %q", line)
			}
			resp.Rows = append(resp.Rows, Row{Key: []byte(args[0]), Value: []byte(args[1])})
		case StatusOK:
			resp.Status, resp.Message = StatusOK, rest
			return resp, nil
		case StatusValue:
			args, err := Tokenize(rest)
			if err != nil || len(args) != 1 {
				return Response{}, fmt.Errorf("protocol: bad VALUE line %q", line)
			}
			resp.Status, resp.Value = StatusValue, []byte(args[0])
			return resp, nil
		case StatusNotFound:
			resp.Status = StatusNotFound
			return resp, nil
		case StatusErr:
			code, msg, _ := strings.Cut(rest, " ")
			resp.Status, resp.Code, resp.Message = StatusErr, code, msg
			return resp, nil
		default:
			return Response{}, fmt.Errorf("protocol: unexpected line %q", line)
		}
	}
}
