package storetests

import (
	"bytes"
	"reflect"
	"testing"

	"git.encryptio.com/slime/lib/store"
)

func ShouldList(t *testing.T, s store.Store, after string, limit int, expect []string) {
	got, err := s.List(after, limit)
	if err != nil {
		t.Errorf("Unexpected error from List(%#v, %v): %v", after, limit, err)
		return
	}

	if !((len(got) == 0 && len(expect) == 0) ||
		reflect.DeepEqual(got, expect)) {

		t.Errorf("List(%#v, %v) = %#v, but wanted %#v",
			after, limit, got, expect)
	}
}

func ShouldFullList(t *testing.T, s store.Store, expect []string) {
	ShouldList(t, s, "", 0, expect)
}

func ShouldSet(t *testing.T, s store.Store, key string, data []byte) {
	err := s.Set(key, data)
	if err != nil {
		t.Errorf("Set(%#v, %#v) returned unexpected error %v", key, data, err)
	}
}

func ShouldGet(t *testing.T, s store.Store, key string, data []byte) {
	got, err := s.Get(key)
	if err != nil {
		t.Errorf("Get(%#v) returned unexpected error %v", key, err)
		return
	}

	if !bytes.Equal(got, data) {
		t.Errorf("Get(%#v) = %#v, but wanted %#v", key, got, data)
	}
}

func ShouldGetError(t *testing.T, s store.Store, key string, wantErr error) {
	got, err := s.Get(key)
	if err != wantErr {
		t.Errorf("Get(%#v) = (%#v, %v), but wanted err = %v",
			key, got, err, wantErr)
	}
}

func ShouldGetMiss(t *testing.T, s store.Store, key string) {
	ShouldGetError(t, s, key, store.ErrNotFound)
}

func ShouldDelete(t *testing.T, s store.Store, key string) {
	err := s.Delete(key)
	if err != nil {
		t.Errorf("Delete(%#v) returned unexpected error %v", key, err)
	}
}

func ShouldDeleteMiss(t *testing.T, s store.Store, key string) {
	err := s.Delete(key)
	if err != store.ErrNotFound {
		t.Errorf("Delete(%#v) = %v, but wanted %v", key, err, store.ErrNotFound)
	}
}

func ShouldFreeSpace(t *testing.T, s store.Store) {
	free, err := s.FreeSpace()
	if err != nil {
		t.Errorf("FreeSpace() returned unexpected error %v", err)
	}
	if free <= 0 {
		t.Errorf("FreeSpace() returned nonpositive %v", free)
	}
}

func TestStoreBasics(t *testing.T, s store.Store) {
	ShouldFullList(t, s, nil)
	ShouldGetMiss(t, s, "hello")
	ShouldSet(t, s, "hello", []byte("world"))
	ShouldGet(t, s, "hello", []byte("world"))
	ShouldFullList(t, s, []string{"hello"})
	ShouldSet(t, s, "a", []byte("a"))
	ShouldSet(t, s, "empty", nil)
	ShouldFullList(t, s, []string{"a", "empty", "hello"})
	ShouldGet(t, s, "a", []byte("a"))
	ShouldGet(t, s, "empty", nil)
	ShouldGet(t, s, "hello", []byte("world"))
	ShouldDeleteMiss(t, s, "b")
	ShouldDelete(t, s, "a")
	ShouldDeleteMiss(t, s, "a")
	ShouldGetMiss(t, s, "a")
	ShouldFullList(t, s, []string{"empty", "hello"})
	ShouldDelete(t, s, "empty")
	ShouldDelete(t, s, "hello")
	ShouldFullList(t, s, nil)

	ShouldSet(t, s, "a", []byte("a"))
	ShouldSet(t, s, "x", []byte("x"))
	ShouldSet(t, s, "z", []byte("z"))
	ShouldSet(t, s, "c", []byte("c"))
	ShouldSet(t, s, "b", []byte("b"))
	ShouldSet(t, s, "y", []byte("y"))
	ShouldList(t, s, "", 1, []string{"a"})
	ShouldList(t, s, "", 3, []string{"a", "b", "c"})
	ShouldList(t, s, "a", 3, []string{"b", "c", "x"})
	ShouldList(t, s, "c", 5, []string{"x", "y", "z"})
	ShouldList(t, s, "z", 3, nil)
	ShouldDelete(t, s, "a")
	ShouldDelete(t, s, "b")
	ShouldDelete(t, s, "c")
	ShouldDelete(t, s, "x")
	ShouldDelete(t, s, "y")
	ShouldDelete(t, s, "z")
	ShouldFullList(t, s, nil)

	ShouldFreeSpace(t, s)
}
