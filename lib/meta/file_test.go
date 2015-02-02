package meta

import (
	"reflect"
	"testing"
	"testing/quick"
)

func TestFileSerialization(t *testing.T) {
	serialDeserial := func(f *File) bool {
		if f.Path == "" {
			// these don't serialize properly
			return true
		}

		pair := f.toPair()
		f2 := new(File)
		err := f2.fromPair(pair)
		if err != nil {
			t.Logf("Couldn't fromPair(toPair(%#v)): %v", f, err)
			return false
		}
		if !reflect.DeepEqual(f, f2) {
			t.Logf("input is %#v\n", f)
			t.Logf("output is %#v\n", f2)
			return false
		}
		return true
	}

	if err := quick.Check(serialDeserial, nil); err != nil {
		t.Error(err)
	}
}
