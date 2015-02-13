package store

import (
	"bytes"
	"reflect"
	"testing"
)

func shouldList(t *testing.T, s Store, expect []string) {
	got, err := s.List("", 0)
	if err != nil {
		t.Errorf("Unexpected error from List(): %v", err)
		return
	}

	if !((len(got) == 0 && len(expect) == 0) ||
		reflect.DeepEqual(got, expect)) {

		t.Errorf("List() = %#v, but wanted %#v", got, expect)
	}
}

func shouldSet(t *testing.T, s Store, key string, data []byte) {
	err := s.Set(key, data)
	if err != nil {
		t.Errorf("Set(%#v, %#v) returned unexpected error %v", key, data, err)
	}
}

func shouldGet(t *testing.T, s Store, key string, data []byte) {
	got, err := s.Get(key)
	if err != nil {
		t.Errorf("Get(%#v) returned unexpected error %v", key, err)
		return
	}

	if !bytes.Equal(got, data) {
		t.Errorf("Get(%#v) = %#v, but wanted %#v", key, got, data)
	}
}

func shouldGetError(t *testing.T, s Store, key string, wantErr error) {
	got, err := s.Get(key)
	if err != wantErr {
		t.Errorf("Get(%#v) = (%#v, %v), but wanted err = %v",
			key, got, err, wantErr)
	}
}

func shouldGetMiss(t *testing.T, s Store, key string) {
	shouldGetError(t, s, key, ErrNotFound)
}

func shouldDelete(t *testing.T, s Store, key string) {
	err := s.Delete(key)
	if err != nil {
		t.Errorf("Delete(%#v) returned unexpected error %v", err)
	}
}

func shouldDeleteMiss(t *testing.T, s Store, key string) {
	err := s.Delete(key)
	if err != ErrNotFound {
		t.Errorf("Delete(%#v) = %v, but wanted %v", key, err, ErrNotFound)
	}
}

func shouldFreeSpace(t *testing.T, s Store) {
	free, err := s.FreeSpace()
	if err != nil {
		t.Errorf("FreeSpace() returned unexpected error %v", err)
	}
	if free <= 0 {
		t.Errorf("FreeSpace() returned nonpositive %v", free)
	}
}

func testStoreBasics(t *testing.T, s Store) {
	shouldList(t, s, nil)
	shouldGetMiss(t, s, "hello")
	shouldSet(t, s, "hello", []byte("world"))
	shouldGet(t, s, "hello", []byte("world"))
	shouldList(t, s, []string{"hello"})
	shouldSet(t, s, "a", []byte("a"))
	shouldSet(t, s, "empty", nil)
	shouldList(t, s, []string{"a", "empty", "hello"})
	shouldGet(t, s, "a", []byte("a"))
	shouldGet(t, s, "empty", nil)
	shouldGet(t, s, "hello", []byte("world"))
	shouldDeleteMiss(t, s, "b")
	shouldDelete(t, s, "a")
	shouldDeleteMiss(t, s, "a")
	shouldGetMiss(t, s, "a")
	shouldList(t, s, []string{"empty", "hello"})
	shouldDelete(t, s, "empty")
	shouldDelete(t, s, "hello")
	shouldList(t, s, nil)
	shouldFreeSpace(t, s)
}
