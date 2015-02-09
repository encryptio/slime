package meta

import (
	"reflect"
	"testing"
	"testing/quick"
)

func TestLocationSerialization(t *testing.T) {
	serialDeserial := func(l *Location) bool {
		pair := l.toPair()
		l2 := new(Location)
		err := l2.fromPair(pair)
		if err != nil {
			t.Logf("Couldn't fromPair(toPair(%#v)): %v", l, err)
			return false
		}

		if len(l.AllocSplit) == 0 {
			l.AllocSplit = nil
		}
		if len(l2.AllocSplit) == 0 {
			l2.AllocSplit = nil
		}

		if !reflect.DeepEqual(l, l2) {
			t.Logf("input is %#v\n", l)
			t.Logf("output is %#v\n", l2)
			return false
		}
		return true
	}

	if err := quick.Check(serialDeserial, nil); err != nil {
		t.Error(err)
	}
}
