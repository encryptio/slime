package tests

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"git.encryptio.com/slime/lib/meta/store"
)

func testShuffleShardedIncrement(t *testing.T, s store.Store) {
	const (
		transactionsPerGoroutine = 500
		parallelism              = 10
		rowCount                 = 4000
	)

	err := clearStore(s)
	if err != nil {
		t.Fatalf("Couldn't clear store: %v", err)
	}

	_, err = s.RunTx(func(ctx store.Ctx) (interface{}, error) {
		zero := []byte("0")
		for i := 0; i < rowCount; i++ {
			key := []byte(fmt.Sprintf("%v", i))
			err := ctx.Set(store.Pair{key, zero})
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't add testing rows: %v", err)
	}

	errCh := make(chan error, parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			for j := 0; j < transactionsPerGoroutine; j++ {
				_, err := s.RunTx(func(ctx store.Ctx) (interface{}, error) {
					// pick two random rows
					var idA, idB int
					for idA == idB {
						idA = rand.Intn(rowCount)
						idB = rand.Intn(rowCount)
					}
					keyA := []byte(fmt.Sprintf("%v", idA))
					keyB := []byte(fmt.Sprintf("%v", idB))

					// read them
					pairA, err := ctx.Get(keyA)
					if err != nil {
						return nil, err
					}
					pairB, err := ctx.Get(keyB)
					if err != nil {
						return nil, err
					}

					// maybe swap their contents
					if rand.Intn(4) == 0 {
						pairA, pairB = pairB, pairA
					}

					// increment one of them
					num, err := strconv.ParseInt(string(pairA.Value), 10, 0)
					if err != nil {
						return nil, err
					}
					num++
					pairA.Value = []byte(strconv.FormatInt(num, 10))

					// write both back
					err = ctx.Set(pairA)
					if err != nil {
						return nil, err
					}
					err = ctx.Set(pairB)
					if err != nil {
						return nil, err
					}

					return nil, nil
				})
				if err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}

	for i := 0; i < parallelism; i++ {
		err := <-errCh
		if err != nil {
			t.Fatalf("Couldn't run incrementer transaction: %v = %#v", err, err)
		}
	}

	var total int
	_, err = s.RunTx(func(ctx store.Ctx) (interface{}, error) {
		pairs, err := ctx.Range(nil, nil, 0)
		if err != nil {
			return nil, err
		}

		for _, pair := range pairs {
			val, err := strconv.ParseInt(string(pair.Value), 10, 0)
			if err != nil {
				return nil, err
			}
			total += int(val)
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't run total transaction: %v", err)
	}

	err = clearStore(s)
	if err != nil {
		t.Fatalf("Couldn't clear store: %v", err)
	}

	if total != parallelism*transactionsPerGoroutine {
		t.Errorf("shuffle sharded increment got %v at end, wanted %v",
			total, parallelism*transactionsPerGoroutine)
	}
}

func testRangeMaxRandomReplacement(t *testing.T, s store.Store) {
	const (
		transactionsPerGoroutine = 20
		parallelism              = 4
		rowCount                 = 5000
	)

	err := clearStore(s)
	if err != nil {
		t.Fatalf("Couldn't clear store: %v", err)
	}

	_, err = s.RunTx(func(ctx store.Ctx) (interface{}, error) {
		zero := []byte("0")
		for i := 0; i < rowCount; i++ {
			key := []byte(fmt.Sprintf("%v", i))
			err := ctx.Set(store.Pair{key, zero})
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't add testing rows: %v", err)
	}

	errCh := make(chan error, parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			for j := 0; j < transactionsPerGoroutine; j++ {
				_, err := s.RunTx(func(ctx store.Ctx) (interface{}, error) {
					// find the max value of all pairs
					var max int64
					pairs, err := ctx.Range(nil, nil, 0)
					if err != nil {
						return nil, err
					}
					for _, pair := range pairs {
						num, err := strconv.ParseInt(string(pair.Value), 10, 0)
						if err != nil {
							return nil, err
						}
						if num > max {
							max = num
						}
					}

					// increment
					max++

					// write to a random pair
					var pair store.Pair
					id := int64(rand.Intn(rowCount))
					pair.Key = []byte(strconv.FormatInt(id, 10))
					pair.Value = []byte(strconv.FormatInt(max, 10))

					err = ctx.Set(pair)
					if err != nil {
						return nil, err
					}

					return nil, nil
				})
				if err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}

	for i := 0; i < parallelism; i++ {
		err := <-errCh
		if err != nil {
			t.Fatalf("Couldn't run incrementer transaction: %v = %#v", err, err)
		}
	}

	var max int64
	_, err = s.RunTx(func(ctx store.Ctx) (interface{}, error) {
		pairs, err := ctx.Range(nil, nil, 0)
		if err != nil {
			return nil, err
		}
		for _, pair := range pairs {
			val, err := strconv.ParseInt(string(pair.Value), 10, 0)
			if err != nil {
				return nil, err
			}
			if val > max {
				max = val
			}
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't run total transaction: %v", err)
	}

	err = clearStore(s)
	if err != nil {
		t.Fatalf("Couldn't clear store: %v", err)
	}

	if max != parallelism*transactionsPerGoroutine {
		t.Errorf("shuffle sharded increment got %v at end, wanted %v",
			max, parallelism*transactionsPerGoroutine)
	}
}
