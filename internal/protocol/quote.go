package protocol

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Tokenize splits a command line into arguments.
//
//	put user:1 "Batuhan Deniz"     → [put user:1 Batuhan Deniz]
//	put k "line1\nline2"           → escapes inside quotes: \" \\ \n \t \r \0 \xHH
//	put k ""                       → an empty argument
//
// Outside quotes, arguments are separated by spaces and tabs and taken
// literally. Inside double quotes, everything up to the closing quote is one
// argument, with the escapes above. Arguments are bytes, so \xHH can store
// any binary value.
func Tokenize(line string) ([]string, error) {
	var args []string
	var cur strings.Builder
	inArg := false // building an argument (needed for "" = empty argument)
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case c == ' ' || c == '\t':
			if inArg {
				args = append(args, cur.String())
				cur.Reset()
				inArg = false
			}
		case c == '"':
			inArg = true
			end, err := readQuoted(line, i+1, &cur)
			if err != nil {
				return nil, err
			}
			i = end // index of the closing quote
		default:
			inArg = true
			cur.WriteByte(c)
		}
	}
	if inArg {
		args = append(args, cur.String())
	}
	return args, nil
}

var errUnterminated = errors.New(`missing closing quote (")`)

// readQuoted reads a quoted string starting after the opening quote at
// line[start-1] into b, and returns the index of the closing quote.
func readQuoted(line string, start int, b *strings.Builder) (int, error) {
	for i := start; i < len(line); i++ {
		c := line[i]
		if c == '"' {
			return i, nil
		}
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		i++
		if i >= len(line) {
			return 0, errUnterminated
		}
		switch e := line[i]; e {
		case '"', '\\':
			b.WriteByte(e)
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case '0':
			b.WriteByte(0)
		case 'x':
			if i+2 >= len(line) {
				return 0, fmt.Errorf(`\x needs two hex digits`)
			}
			v, err := strconv.ParseUint(line[i+1:i+3], 16, 8)
			if err != nil {
				return 0, fmt.Errorf(`\x%s is not a hex byte`, line[i+1:i+3])
			}
			b.WriteByte(byte(v))
			i += 2
		default:
			return 0, fmt.Errorf(`unknown escape \%c (use \" \\ \n \t \r \0 \xHH)`, e)
		}
	}
	return 0, errUnterminated
}

// Display shows a key so that it can be typed back in: as is if it is plain
// text without spaces or quotes, otherwise as a quoted string.
func Display(b []byte) string {
	if len(b) == 0 || !utf8.Valid(b) {
		return Quote(b)
	}
	for _, r := range string(b) {
		if r == ' ' || r == '"' || r == '\\' || !unicode.IsPrint(r) {
			return Quote(b)
		}
	}
	return string(b)
}

// Quote shows b as a quoted string using the escapes Tokenize accepts, so
// the output can be pasted back as input. Printable UTF-8 text (including
// letters like ş, ğ, ı) is shown as is; control characters and bytes that
// are not valid UTF-8 are escaped.
func Quote(b []byte) string {
	var s strings.Builder
	s.WriteByte('"')
	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		switch {
		case r == '"' || r == '\\':
			s.WriteByte('\\')
			s.WriteRune(r)
		case r == '\n':
			s.WriteString(`\n`)
		case r == '\t':
			s.WriteString(`\t`)
		case r == '\r':
			s.WriteString(`\r`)
		case r == utf8.RuneError && size == 1, !unicode.IsPrint(r):
			for _, c := range b[:size] {
				fmt.Fprintf(&s, `\x%02x`, c)
			}
		default:
			s.Write(b[:size])
		}
		b = b[size:]
	}
	s.WriteByte('"')
	return s.String()
}
