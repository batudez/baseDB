package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func TestTokenize(t *testing.T) {
	cases := []struct {
		line string
		want []string
	}{
		{`put user:1 Batuhan`, []string{"put", "user:1", "Batuhan"}},
		{`  put   k    v  `, []string{"put", "k", "v"}},
		{`put "user name" "Batuhan Deniz"`, []string{"put", "user name", "Batuhan Deniz"}},
		{`put k ""`, []string{"put", "k", ""}},
		{`put k "say \"hi\""`, []string{"put", "k", `say "hi"`}},
		{`put k "a\\b\n\t\r\0"`, []string{"put", "k", "a\\b\n\t\r\x00"}},
		{`put k "\x00\xff\x41"`, []string{"put", "k", "\x00\xffA"}},
		{`put k ab"cd"ef`, []string{"put", "k", "abcdef"}},
		{`put şehir "İstanbul"`, []string{"put", "şehir", "İstanbul"}},
		{``, nil},
	}
	for _, c := range cases {
		got, err := Tokenize(c.line)
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("Tokenize(%q) = %q, %v; want %q", c.line, got, err, c.want)
		}
	}
	for _, bad := range []string{`put k "open`, `put k "\q"`, `put k "\x4"`, `put k "\xzz"`, `put k "ends\`} {
		if _, err := Tokenize(bad); err == nil {
			t.Errorf("Tokenize(%q) accepted invalid input", bad)
		}
	}
}

// Whatever Quote/Display print can be typed back in and gives the same bytes.
func TestQuoteRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	samples := [][]byte{nil, []byte("plain"), []byte("Ayşe Yılmaz"), []byte("a\"b\\c\nd"), {0, 0xff, 0x80, 'x'}}
	for i := 0; i < 500; i++ {
		b := make([]byte, rng.Intn(40))
		rng.Read(b)
		samples = append(samples, b)
	}
	for _, b := range samples {
		for _, shown := range []string{Quote(b), Display(b)} {
			args, err := Tokenize("put " + shown)
			if err != nil || len(args) != 2 || !bytes.Equal([]byte(args[1]), b) {
				t.Fatalf("%q shown as %s reads back as %q (%v)", b, shown, args, err)
			}
		}
	}
	if Display([]byte("user:1")) != "user:1" || Quote([]byte("Ayşe")) != `"Ayşe"` {
		t.Fatal("plain text should be shown as is")
	}
}

func TestLineRoundTrip(t *testing.T) {
	args := []string{"put", "key with space", "line1\nline2\x00\xff", ""}
	got, err := Tokenize(strings.TrimSuffix(Line(args...), "\n"))
	if err != nil || !reflect.DeepEqual(got, args) {
		t.Fatalf("Line/Tokenize round trip = %q, %v", got, err)
	}
	if strings.Count(Line(args...), "\n") != 1 {
		t.Fatal("a request must be exactly one line")
	}
}

func TestResponseRoundTrip(t *testing.T) {
	responses := []Response{
		OK(""),
		OK("committed 2 writes"),
		Value([]byte("multi\nline \"value\" \x00\xff")),
		Value(nil),
		NotFound(),
		Err(CodeDeadlock, "deadlock: run it again"),
		{Rows: []Row{{[]byte("a"), []byte("1")}, {[]byte("b c"), []byte("x\ny")}}, Status: StatusOK, Message: "2"},
	}
	var buf bytes.Buffer
	for _, r := range responses {
		if err := r.Write(&buf); err != nil {
			t.Fatal(err)
		}
	}
	rd := bufio.NewReaderSize(&buf, MaxLine)
	for i, want := range responses {
		got, err := ReadResponse(rd)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		if want.Value == nil && got.Value != nil && len(got.Value) == 0 {
			got.Value = nil
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("response %d:\n got %+v\nwant %+v", i, got, want)
		}
	}
}

func TestErrorMatchesByCode(t *testing.T) {
	err := Err(CodeDeadlock, "some text").Err()
	if !errors.Is(err, &Error{Code: CodeDeadlock}) || errors.Is(err, &Error{Code: CodeBusy}) {
		t.Fatal("errors.Is must match on the code")
	}
	if OK("").Err() != nil {
		t.Fatal("OK is not an error")
	}
}

func TestReadLineLimits(t *testing.T) {
	long := strings.Repeat("x", MaxLine+10) + "\n"
	r := bufio.NewReaderSize(strings.NewReader(long), MaxLine)
	if _, err := ReadLine(r); !errors.Is(err, ErrLineTooLong) {
		t.Fatalf("err = %v, want ErrLineTooLong", err)
	}
	r = bufio.NewReaderSize(strings.NewReader("get a\r\nget b"), MaxLine)
	for _, want := range []string{"get a", "get b"} {
		if got, err := ReadLine(r); err != nil || got != want {
			t.Fatalf("ReadLine = %q, %v; want %q", got, err, want)
		}
	}
}
