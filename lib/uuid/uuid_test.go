package uuid

import (
	"testing"
)

func TestUUIDTwoWay(t *testing.T) {
	u := Gen4()

	s := Fmt(u)

	p, err := Parse(s)
	if err != nil {
		t.Fatalf("Couldn't parse formatted uuid %#v: %v", s, err)
	}

	if p != u {
		t.Fatalf("UUID after Parse(Fmt()) was not the same as original UUID")
	}
}
