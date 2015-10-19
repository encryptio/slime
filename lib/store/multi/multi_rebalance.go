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
	rebalanceWait = time.Second * 30

	errModifiedDuringBalance = errors.New("file modified during balance")
)

const (
	rebalanceFileCount     = 10
	rebalanceMaxScan       = 100
	rebalanceMinDifference = 1024 * 1024 * 1024 // 1 GiB
)

func (m *Multi) rebalanceLoop() error {
	for {
		select {
		case <-m.tomb.Dying():
			return nil
		case <-time.After(jitterDuration(rebalanceWait)):
			err := m.rebalanceStep()
			if err != nil {
				log.Printf("Couldn't run rebalance step: %v", err)
			}
		}
	}
}

func (m *Multi) rebalanceStep() error {
	finderEntries := m.finder.Stores()

	// bail out early if there's no rebalancing possible
	mostFree := int64(0)
	leastFree := int64(0)
	markedOne := false
	for _, fe := range finderEntries {
		if !markedOne {
			markedOne = true
			mostFree = fe.Free
			leastFree = fe.Free
		}
		if fe.Free > mostFree {
			mostFree = fe.Free
		}
		if fe.Free < leastFree {
			leastFree = fe.Free
		}
	}
	if mostFree-leastFree < rebalanceMinDifference {
		return nil
	}

	moved := 0
	scanned := 0
	defer func() {
		log.Printf("Rebalanced %v chunks out of %v scanned", moved, scanned)
	}()

	for moved < rebalanceFileCount && scanned < rebalanceMaxScan {
		var files []meta.File
		err := m.db.RunTx(func(ctx kvl.Ctx) error {
			files = nil

			l, err := meta.Open(ctx)
			if err != nil {
				return err
			}

			startkey, err := l.GetConfig("rebalpos")
			if err != nil {
				return err
			}

			files, err = l.ListFiles(string(startkey), 20)
			if err != nil {
				return err
			}

			if len(files) > 0 {
				err = l.SetConfig("rebalpos", []byte(files[len(files)-1].Path))
			} else {
				err = l.SetConfig("rebalpos", []byte{})
			}
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			return err
		}

		if len(files) == 0 {
			break
		}

		for _, f := range files {
			did, err := m.rebalanceFile(f, finderEntries)
			if err != nil {
				log.Printf("Failed to rebalance %v: %v", f.Path, err)
			}
			scanned++
			if did {
				moved++
				if moved >= rebalanceFileCount {
					break
				}
			}
			if scanned >= rebalanceMaxScan {
				break
			}
		}
	}

	return nil
}

func (m *Multi) rebalanceFile(f meta.File, finderEntries map[[16]byte]FinderEntry) (bool, error) {
	// search for the lowest free location that this file is stored on
	var minI int
	var minF int64
	var minS store.Store
	for i, l := range f.Locations {
		fe, ok := finderEntries[l]
		if !ok {
			continue
		}

		if minS == nil || minF > fe.Free {
			minS = fe.Store
			minF = fe.Free
			minI = i
		}
	}

	if minS == nil {
		return false, nil
	}

	// search for the highest free location that this file is NOT stored on
	var maxF int64
	var maxS store.Store
	for id, fe := range finderEntries {
		if fe.Dead {
			continue
		}

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

		if maxS == nil || maxF < fe.Free {
			maxF = fe.Free
			maxS = fe.Store
		}
	}

	if maxS == nil {
		return false, nil
	}

	if maxF-minF < rebalanceMinDifference {
		return false, nil
	}

	// we should move chunk minI on minS to maxS

	err := m.db.RunTx(func(ctx kvl.Ctx) error {
		l, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		return l.WALMark(f.PrefixID)
	})
	if err != nil {
		return false, err
	}
	defer func() {
		// TODO: how to handle errors here?
		m.db.RunTx(func(ctx kvl.Ctx) error {
			l, err := meta.Open(ctx)
			if err != nil {
				return err
			}

			return l.WALClear(f.PrefixID)
		})
	}()

	localKey := localKeyFor(&f, minI)
	data, st, err := minS.Get(localKey, nil)
	if err != nil {
		return false, err
	}

	setCASV := store.CASV{
		Present: true,
		SHA256:  st.SHA256,
		Data:    data,
	}

	err = maxS.CAS(localKey, store.MissingV, setCASV, nil)
	if err != nil {
		return false, err
	}

	newF := f
	newF.Locations = make([][16]byte, len(f.Locations))
	copy(newF.Locations, f.Locations)
	newF.Locations[minI] = maxS.UUID()

	err = m.db.RunTx(func(ctx kvl.Ctx) error {
		l, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		f2, err := l.GetFile(f.Path)
		if err != nil {
			return err
		}

		if f2.PrefixID != f.PrefixID {
			return errModifiedDuringBalance
		}

		if len(f2.Locations) != len(f.Locations) {
			return errModifiedDuringBalance
		}

		for i, floc := range f.Locations {
			if floc != f2.Locations[i] {
				return errModifiedDuringBalance
			}
		}

		err = l.SetFile(&newF)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		maxS.CAS(localKey, setCASV, store.MissingV, nil) // ignore error
		return false, err
	}

	err = minS.CAS(localKey, setCASV, store.MissingV, nil)
	if err != nil {
		log.Printf("Couldn't remove rebalanced chunk from old location %v: %v",
			uuid.Fmt(maxS.UUID()), err)
	}

	fe := finderEntries[minS.UUID()]
	fe.Free += int64(len(data))
	finderEntries[minS.UUID()] = fe

	fe = finderEntries[maxS.UUID()]
	fe.Free -= int64(len(data))
	finderEntries[maxS.UUID()] = fe

	return true, nil
}
