package server

import "testing"

func TestPrefixEnd(t *testing.T) {
	cases := map[string]string{"user:": "user;", "a": "b", "a\xff": "b", "\xff\xff": "", "": ""}
	for p, want := range cases {
		if got := string(PrefixEnd([]byte(p))); got != want {
			t.Errorf("PrefixEnd(%q) = %q, want %q", p, got, want)
		}
	}
}
