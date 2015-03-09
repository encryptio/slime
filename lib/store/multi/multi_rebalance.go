package multi

import (
	"errors"
	"log"
	"time"

	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/uuid"

	"git.encryptio.com/kvl"
)

// TODO: tests for rebalancing (requires mocking/overriding FreeSpace())

var (
	rebalanceWait = time.Minute * 3

	errModifiedDuringBalance = errors.New("file modified during balance")
)

const (
	rebalanceFileCount     = 50
	rebalanceMaxScan       = 500
	rebalanceMinDifference = 1024 * 1024 * 1024 // 1 GiB
)

func (m *Multi) rebalanceLoop() {
	for {
		select {
		case <-m.stop:
			return
		case <-time.After(jitterDuration(rebalanceWait)):
			err := m.rebalanceStep()
			if err != nil {
				log.Printf("Couldn't run rebalance step: %v", err)
			}
		}
	}
}

func (m *Multi) rebalanceStep() error {
	stores := m.finder.Stores()
	frees := make(map[[16]byte]int64, len(stores))
	for k, st := range stores {
		free, err := st.FreeSpace()
		if err != nil {
			log.Printf("Couldn't get free space on %v: %v", uuid.Fmt(st.UUID()), err)
			continue
		}

		frees[k] = free
	}

	moved := 0
	scanned := 0
	defer func() {
		if moved > 0 {
			log.Printf("Rebalanced %v chunks out of %v scanned", moved, scanned)
		}
	}()

	for moved < rebalanceFileCount && scanned < rebalanceMaxScan {
		ret, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
			l, err := meta.Open(ctx)
			if err != nil {
				return nil, err
			}

			startkey, err := l.GetConfig("rebalpos")
			if err != nil {
				return nil, err
			}

			files, err := l.ListFiles(string(startkey), 20)
			if err != nil {
				return nil, err
			}

			if len(files) > 0 {
				err = l.SetConfig("rebalpos", []byte(files[len(files)-1].Path))
			} else {
				err = l.SetConfig("rebalpos", []byte{})
			}
			if err != nil {
				return nil, err
			}

			return files, nil
		})
		if err != nil {
			return err
		}

		files := ret.([]meta.File)

		if len(files) == 0 {
			break
		}

		for _, f := range files {
			did, err := m.rebalanceFile(f, stores, frees)
			if err != nil {
				log.Printf("Failed to rebalance %v: %v", f.Path, err)
			}
			if did {
				moved++
				if moved >= rebalanceFileCount {
					break
				}
			}
			scanned++
			if scanned >= rebalanceMaxScan {
				break
			}
		}
	}

	return nil
}

func (m *Multi) rebalanceFile(f meta.File, stores map[[16]byte]store.Store, frees map[[16]byte]int64) (bool, error) {
	// search for the lowest free location that this file is stored on
	var minI int
	var minF int64
	var minS store.Store
	for i, l := range f.Locations {
		free, ok := frees[l]
		if !ok {
			continue
		}

		if minS == nil || minF > free {
			minS = stores[l]
			minF = free
			minI = i
		}
	}

	if minS == nil {
		return false, nil
	}

	// search for the highest free location that this file is NOT stored on
	var maxF int64
	var maxS store.Store
	for id, st := range stores {
		found := false
		for _, locid := range f.Locations {
			if locid == id {
				found = true
				break
			}
		}
		if found {
			continue
		}

		free, ok := frees[id]
		if !ok {
			continue
		}

		if maxS == nil || maxF < free {
			maxF = free
			maxS = st
		}
	}

	if maxS == nil {
		return false, nil
	}

	if maxF-minF < rebalanceMinDifference {
		return false, nil
	}

	// we should move chunk minI on minS to maxS

	localKey := localKeyFor(&f, minI)
	data, hash, err := minS.Get(localKey)
	if err != nil {
		return false, err
	}

	setCASV := store.CASV{
		Present: true,
		SHA256:  hash,
		Data:    data,
	}

	err = maxS.CAS(localKey, store.MissingV, setCASV)
	if err != nil {
		return false, err
	}

	newF := f
	newF.Locations = make([][16]byte, len(f.Locations))
	copy(newF.Locations, f.Locations)
	newF.Locations[minI] = maxS.UUID()

	_, err = m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		l, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		f2, err := l.GetFile(f.Path)
		if err != nil {
			return nil, err
		}

		if f2.PrefixID != f.PrefixID {
			return nil, errModifiedDuringBalance
		}

		if len(f2.Locations) != len(f.Locations) {
			return nil, errModifiedDuringBalance
		}

		for i, floc := range f.Locations {
			if floc != f2.Locations[i] {
				return nil, errModifiedDuringBalance
			}
		}

		err = l.SetFile(&newF)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		maxS.CAS(localKey, setCASV, store.MissingV) // ignore error
		return false, err
	}

	err = minS.CAS(localKey, setCASV, store.MissingV)
	if err != nil {
		log.Printf("Couldn't remove rebalanced chunk from old location %v: %v",
			uuid.Fmt(maxS.UUID()), err)
	}

	frees[minS.UUID()] += int64(len(data))
	frees[maxS.UUID()] -= int64(len(data))

	log.Printf("Rebalanced a chunk of %v from %v to %v",
		f.Path, uuid.Fmt(minS.UUID()), uuid.Fmt(maxS.UUID()))

	return true, nil
}
