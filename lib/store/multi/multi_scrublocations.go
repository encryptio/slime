package multi

import (
	"log"
	"time"

	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/uuid"

	"git.encryptio.com/kvl"
)

var (
	scrubLocationsWait  = time.Second * 10
	scrubLocationsCount = 1000
)

func (m *Multi) scrubLocationsAll() {
	// Get the number of locations; scrubLocationsStep will return true when it
	// hits the end of each of them.
	//
	// We assume there are no concurrent adding/removing of Locations.
	ret, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		return layer.AllLocations()
	})
	if err != nil {
		log.Printf("Couldn't get all locations: %v", err)
		return
	}
	need := len(ret.([]meta.Location)) * 2

	if need == 0 {
		return
	}

	endpoints := 0
	for {
		done, err := m.scrubLocationsStep()
		if err != nil {
			log.Printf("Couldn't scrubLocationsStep in scrubLocationsAll: %v", err)
			return
		}
		if done {
			endpoints++
			if endpoints >= need {
				return
			}
		}
	}
}

func (m *Multi) scrubLocationsLoop() error {
	for {
		select {
		case <-m.tomb.Dying():
			return nil
		case <-time.After(jitterDuration(scrubLocationsWait)):
			_, err := m.scrubLocationsStep()
			if err != nil {
				log.Printf("Couldn't run scrubLocationsStep: %v", err)
			}
		}
	}
}

func (m *Multi) scrubLocationsStep() (bool, error) {
	uuidCmp := func(a, b [16]byte) int {
		for i := 0; i < 16; i++ {
			if a[i] < b[i] {
				return -1
			} else if a[i] > b[i] {
				return 1
			}
		}
		return 0
	}

	var thisLoc meta.Location
	var from string
	ret, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		pos, err := layer.GetConfig("scrublocationpos")
		if err != nil {
			return nil, err
		}

		// parse errors okay, since will be the zero UUID
		since, _ := uuid.Parse(string(pos))

		allLocations, err := layer.AllLocations()
		if err != nil {
			return nil, err
		}

		if len(allLocations) == 0 {
			// no locations exist
			return nil, nil
		}

		for {
			// get the Location with the lowest UUID greater than since
			haveLoc := false
			for _, loc := range allLocations {
				if uuidCmp(loc.UUID, since) >= 0 && (!haveLoc || uuidCmp(thisLoc.UUID, loc.UUID) > 0) {
					thisLoc = loc
					haveLoc = true
				}
			}

			if haveLoc {
				break
			}

			// all locations handled already, try again from the top
			if since == [16]byte{} {
				panic("locations exist but couldn't find one")
			}
			since = [16]byte{}
		}

		// set since to the next highest UUID
		since = thisLoc.UUID
		for i := 15; i >= 0; i-- {
			since[i]++
			if since[i] != 0 {
				break
			}
		}

		err = layer.SetConfig("scrublocationpos", []byte(uuid.Fmt(since)))
		if err != nil {
			return nil, err
		}

		// now get the file list in the location we've chosen, resuming at the
		// saved position (per location)

		fromB, err := layer.GetConfig(
			"scrublocationpos-" + uuid.Fmt(thisLoc.UUID))
		if err != nil {
			return nil, err
		}
		from = string(fromB)

		files, err := layer.GetLocationContents(thisLoc.UUID, from,
			scrubLocationsCount)
		if err != nil {
			return nil, err
		}

		var newLocPos string
		if len(files) > 0 {
			newLocPos = files[len(files)-1]
		} else {
			newLocPos = ""
		}
		err = layer.SetConfig(
			"scrublocationpos-"+uuid.Fmt(thisLoc.UUID), []byte(newLocPos))
		if err != nil {
			return nil, err
		}

		return files, nil
	})
	if err != nil {
		return false, err
	}
	if ret == nil {
		return false, nil
	}

	wantFiles := ret.([]string)
	wantFilesMap := make(map[string]struct{}, len(wantFiles))
	for _, f := range wantFiles {
		wantFilesMap[f] = struct{}{}
	}

	st := m.finder.StoreFor(thisLoc.UUID)
	if st == nil {
		log.Printf("Couldn't scrubLocation %v, it is offline",
			uuid.Fmt(thisLoc.UUID))
		return false, nil
	}

	haveFiles, err := st.List(from, scrubLocationsCount, nil)
	if err != nil {
		log.Printf("Couldn't List from %v: %v", uuid.Fmt(thisLoc.UUID), err)
		return false, nil
	}

	haveFilesMap := make(map[string]struct{}, len(haveFiles))
	for _, f := range haveFiles {
		haveFilesMap[f] = struct{}{}
	}

	checkHaveFiles := make([]string, 0, len(haveFiles))
	for _, f := range haveFiles {
		if len(wantFiles) < scrubLocationsCount || f <= wantFiles[len(wantFiles)-1] {
			checkHaveFiles = append(checkHaveFiles, f)
		}
	}

	checkWantFiles := make([]string, 0, len(wantFiles))
	for _, f := range wantFiles {
		if len(haveFiles) < scrubLocationsCount || f <= haveFiles[len(haveFiles)-1] {
			checkWantFiles = append(checkWantFiles, f)
		}
	}

	for _, have := range checkHaveFiles {
		if _, ok := wantFilesMap[have]; !ok {
			pid, err := prefixIDFromLocalKey(have)
			if err != nil {
				log.Printf("Couldn't figure out PrefixID from localKey(%#v): %v",
					have, err)

				err = st.CAS(have, store.AnyV, store.MissingV, nil)
				if err != nil {
					log.Printf("Couldn't delete extraneous chunk %v from %v: %v",
						have, uuid.Fmt(thisLoc.UUID), err)
					continue
				}

				continue
			}

			ret, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
				layer, err := meta.Open(ctx)
				if err != nil {
					return nil, err
				}

				found, err := layer.WALCheck(pid)
				if err != nil {
					return nil, err
				}

				if !found {
					// check to see if the write finished
					found, err = layer.LocationShouldHave(thisLoc.UUID, have)
					if err != nil {
						return nil, err
					}
				}

				return found, nil
			})
			if err != nil {
				log.Printf("Couldn't check WAL for PrefixID %v: %v",
					uuid.Fmt(pid), err)
				continue
			}

			inWAL := ret.(bool)

			if inWAL {
				log.Printf("extraneous chunk %v on %v, but is in WAL, skipping",
					have, uuid.Fmt(thisLoc.UUID))
				continue
			}

			log.Printf("deleting extraneous chunk %v on %v",
				have, uuid.Fmt(thisLoc.UUID))
			err = st.CAS(have, store.AnyV, store.MissingV, nil)
			if err != nil {
				log.Printf("Couldn't delete extraneous chunk %v from %v: %v",
					have, uuid.Fmt(thisLoc.UUID), err)
				continue
			}
		}
	}

	for _, want := range checkWantFiles {
		if _, ok := haveFilesMap[want]; !ok {
			log.Printf("missing chunk %v on %v", want, uuid.Fmt(thisLoc.UUID))

			pid, err := prefixIDFromLocalKey(want)
			if err != nil {
				log.Printf("Couldn't figure out PrefixID from localKey(%#v): %v",
					want, err)
				continue
			}

			var inWAL bool
			var path string
			_, err = m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
				layer, err := meta.Open(ctx)
				if err != nil {
					return nil, err
				}

				inWAL, err = layer.WALCheck(pid)
				if err != nil {
					return nil, err
				}

				if !inWAL {
					path, err = layer.PathForPrefixID(pid)
					if err != nil {
						return nil, err
					}
				}

				return nil, nil
			})
			if err != nil {
				log.Printf("Couldn't get path for PrefixID %v: %v",
					uuid.Fmt(pid), err)
				continue
			}

			if inWAL {
				log.Printf("skipping rebuild of %v, prefix in WAL", path)
				continue
			}

			err = m.rebuild(path)
			if err != nil {
				log.Printf("Couldn't rebuild %v: %v", path, err)
				continue
			}

			log.Printf("successfully rebuilt %v", path)
		}
	}

	if thisLoc.Dead && len(wantFiles) > 0 {
		// dead stores should be cleared

		for _, want := range wantFiles {
			pid, err := prefixIDFromLocalKey(want)
			if err != nil {
				// already logged in a loop above
				continue
			}

			var inWAL bool
			var path string
			_, err = m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
				layer, err := meta.Open(ctx)
				if err != nil {
					return nil, err
				}

				inWAL, err = layer.WALCheck(pid)
				if err != nil {
					return nil, err
				}

				if !inWAL {
					path, err = layer.PathForPrefixID(pid)
					if err != nil {
						return nil, err
					}
				}

				return nil, nil
			})
			if err != nil {
				log.Printf("Couldn't check wal/path for PrefixID %v: %v",
					uuid.Fmt(pid), err)
				continue
			}

			if inWAL {
				log.Printf("skipping rebuild of PrefixID %v on dead store, prefix in WAL", uuid.Fmt(pid))
				continue
			}

			err = m.rebuild(path)
			if err != nil {
				log.Printf("Couldn't rebuild %v: %v", path, err)
				continue
			}

			log.Printf("successfully rebuilt %v", path)
		}
	}

	return len(wantFiles) == 0, nil
}
