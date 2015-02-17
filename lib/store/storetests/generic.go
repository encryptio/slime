package storetests

import (
	"crypto/sha256"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"git.encryptio.com/slime/lib/store"
)

func TestStore(t *testing.T, s store.Store) {
	TestStoreBasics(t, s)
	TestStoreList(t, s)
	TestStoreCAS(t, s)
	TestStoreCASRace(t, s)
}

func TestStoreBasics(t *testing.T, s store.Store) {
	t.Logf("TestStoreBasics()")

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
}

func TestStoreList(t *testing.T, s store.Store) {
	t.Logf("TestStoreList()")

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

func TestStoreCAS(t *testing.T, s store.Store) {
	t.Logf("TestStoreCAS()")

	ShouldFullList(t, s, nil)

	a := []byte("a")
	b := []byte("b")
	c := []byte("c")
	aSHA := sha256.Sum256(a)
	bSHA := sha256.Sum256(b)
	cSHA := sha256.Sum256(c)

	ShouldSetWith256(t, s, "1", a, aSHA)
	ShouldGet(t, s, "1", a)
	ShouldCASWith256(t, s, "1", aSHA, b, bSHA)
	ShouldGet(t, s, "1", b)
	ShouldCASWith256Fail(t, s, "1", aSHA, c, cSHA)
	ShouldGet(t, s, "1", b)
	ShouldCASWith256(t, s, "1", bSHA, b, bSHA)
	ShouldGet(t, s, "1", b)
	ShouldDelete(t, s, "1")

	ShouldFullList(t, s, nil)
}

func TestStoreCASRace(t *testing.T, s store.Store) {
	t.Logf("TestStoreCASRace()")

	const (
		goroutines = 5
		iterations = 25
	)

	ShouldFullList(t, s, nil)

	ShouldSet(t, s, "key", []byte("0"))

	errs := make(chan error)
	casFailures := uint64(0)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				for {
					time.Sleep(time.Millisecond)
					data, oldsha, err := s.GetWith256("key")
					if err != nil {
						errs <- err
						return
					}

					num, err := strconv.ParseInt(string(data), 10, 64)
					if err != nil {
						errs <- err
						return
					}

					num++

					data = strconv.AppendInt(data[:0], num, 10)
					newsha := sha256.Sum256(data)

					err = s.CASWith256("key", oldsha, data, newsha)
					if err != nil {
						if err == store.ErrCASFailure {
							atomic.AddUint64(&casFailures, 1)
							continue
						}
						errs <- err
						return
					}

					break
				}
			}
			errs <- nil
		}()
	}
	for i := 0; i < goroutines; i++ {
		err := <-errs
		if err != nil {
			t.Errorf("Got error from goroutine: %v", err)
		}
	}

	t.Logf("%v cas failures", casFailures)

	ShouldGet(t, s, "key", []byte(strconv.FormatInt(goroutines*iterations, 10)))

	ShouldDelete(t, s, "key")
}
