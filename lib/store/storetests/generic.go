package storetests

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"git.encryptio.com/slime/lib/retry"
	"git.encryptio.com/slime/lib/store"
)

func TestStore(t *testing.T, s store.Store) {
	TestStoreBasics(t, s)
	TestStoreList(t, s)
	TestStoreCASCountRace(t, s)
}

func TestStoreBasics(t *testing.T, s store.Store) {
	t.Logf("TestStoreBasics()")

	ShouldFullList(t, s, nil)
	ShouldGetMiss(t, s, "hello")
	ShouldCAS(t, s, "hello", store.MissingV, store.MissingV)
	ShouldGetMiss(t, s, "hello")
	ShouldCAS(t, s, "hello", store.MissingV, store.DataV([]byte("world")))
	ShouldGet(t, s, "hello", []byte("world"))
	ShouldCAS(t, s, "a", store.AnyV, store.MissingV)
	ShouldFullList(t, s, []string{"hello"})
	ShouldCAS(t, s, "b", store.AnyV, store.DataV([]byte("beta")))
	ShouldCAS(t, s, "b", store.AnyV, store.DataV([]byte("other")))
	ShouldGet(t, s, "b", []byte("other"))
	ShouldFullList(t, s, []string{"b", "hello"})
	ShouldCASFail(t, s, "b", store.MissingV, store.MissingV)
	ShouldCAS(t, s, "hello", store.AnyV, store.MissingV)
	ShouldFullList(t, s, []string{"b"})
	ShouldCAS(t, s, "empty", store.AnyV, store.DataV(nil))
	ShouldGet(t, s, "empty", nil)
	ShouldCAS(t, s, "empty", store.DataV(nil), store.DataV([]byte("one")))
	ShouldCAS(t, s, "empty", store.DataV([]byte("one")), store.DataV([]byte("two")))
	ShouldGet(t, s, "empty", []byte("two"))

	ShouldCAS(t, s, "b", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "empty", store.AnyV, store.MissingV)
	ShouldFullList(t, s, nil)

	ShouldFreeSpace(t, s)
}

func TestStoreList(t *testing.T, s store.Store) {
	t.Logf("TestStoreList()")

	ShouldFullList(t, s, nil)
	ShouldCAS(t, s, "a", store.MissingV, store.DataV([]byte("a")))
	ShouldCAS(t, s, "x", store.MissingV, store.DataV([]byte("x")))
	ShouldCAS(t, s, "z", store.MissingV, store.DataV([]byte("z")))
	ShouldCAS(t, s, "c", store.MissingV, store.DataV([]byte("c")))
	ShouldCAS(t, s, "b", store.MissingV, store.DataV([]byte("b")))
	ShouldCAS(t, s, "y", store.MissingV, store.DataV([]byte("y")))
	ShouldList(t, s, "", 1, []string{"a"})
	ShouldList(t, s, "", 3, []string{"a", "b", "c"})
	ShouldList(t, s, "a", 3, []string{"b", "c", "x"})
	ShouldList(t, s, "c", 5, []string{"x", "y", "z"})
	ShouldList(t, s, "z", 3, nil)
	ShouldCAS(t, s, "a", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "b", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "c", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "x", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "y", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "z", store.AnyV, store.MissingV)
	ShouldFullList(t, s, nil)
}

func TestStoreCASCountRace(t *testing.T, s store.Store) {
	t.Logf("TestStoreCASCountRace()")

	const (
		goroutines = 5
		iterations = 25
	)

	ShouldFullList(t, s, nil)

	ShouldCAS(t, s, "key", store.AnyV, store.DataV([]byte("0")))

	errs := make(chan error)
	casFailures := uint64(0)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			for j := 0; j < iterations; j++ {
				done := false
				r := retry.New(10)
				for r.Next() {
					time.Sleep(time.Millisecond)
					data, oldsha, err := s.Get("key")
					if err != nil {
						t.Logf("Routine %v: Couldn't get key: %v", i, err)
						errs <- err
						return
					}

					num, err := strconv.ParseInt(string(data), 10, 64)
					if err != nil {
						t.Logf("Routine %v: Couldn't parse int: %v", i, err)
						errs <- err
						return
					}

					num++

					data = strconv.AppendInt(data[:0], num, 10)

					err = s.CAS("key",
						store.CASV{Present: true, SHA256: oldsha},
						store.DataV(data))
					if err != nil {
						if err == store.ErrCASFailure {
							atomic.AddUint64(&casFailures, 1)
							continue
						}
						t.Logf("Routine %v: Couldn't cas: %v", i, err)
						errs <- err
						return
					}

					t.Logf("routine %v iteration %v succeeded in casing to %v",
						i, j, num)

					done = true
					break
				}
				if !done {
					errs <- fmt.Errorf("too many retries")
					return
				}
			}
			errs <- nil
		}(i)
	}
	for i := 0; i < goroutines; i++ {
		err := <-errs
		if err != nil {
			t.Errorf("Got error from goroutine: %v", err)
		}
	}

	t.Logf("%v cas failures", casFailures)

	ShouldGet(t, s, "key", []byte(strconv.FormatInt(goroutines*iterations, 10)))
	ShouldCAS(t, s, "key", store.AnyV, store.MissingV)
}