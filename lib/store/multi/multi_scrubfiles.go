package multi

import (
	"fmt"
	"log"
	"time"

	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/uuid"

	"git.encryptio.com/kvl"
)

var (
	scrubFilesWait  = time.Second * 15
	scrubFilesCount = 50
)

func (m *Multi) scrubFilesAll() {
	endpoints := 0
	for {
		done, err := m.scrubFilesStep()
		if err != nil {
			log.Printf("Couldn't scrubFilesStep in scrubFilesAll: %v", err)
			return
		}
		if done {
			endpoints++
			if endpoints >= 2 {
				return
			}
		}
	}
}

func (m *Multi) scrubFilesLoop() {
	for {
		select {
		case <-m.stop:
			return
		case <-time.After(jitterDuration(scrubFilesWait)):
			_, err := m.scrubFilesStep()
			if err != nil {
				log.Printf("Couldn't run scrubFilesStep: %v", err)
			}
		}
	}
}

func (m *Multi) scrubFilesStep() (bool, error) {
	ret, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		startKey, err := layer.GetConfig("scrubpos")
		if err != nil {
			return nil, err
		}

		files, err := layer.ListFiles(string(startKey), scrubFilesCount)
		if err != nil {
			return nil, err
		}

		if len(files) > 0 {
			err = layer.SetConfig("scrubpos", []byte(files[len(files)-1].Path))
			if err != nil {
				return nil, err
			}
		} else {
			err = layer.SetConfig("scrubpos", []byte{})
			if err != nil {
				return nil, err
			}
		}

		return files, nil
	})
	if err != nil {
		return false, err
	}

	files := ret.([]meta.File)

	ret, err = m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		allLocations, err := layer.AllLocations()
		if err != nil {
			return nil, err
		}

		return allLocations, nil
	})

	locs := ret.([]meta.Location)
	allLocations := make(map[[16]byte]meta.Location, len(locs))
	for _, loc := range locs {
		allLocations[loc.UUID] = loc
	}

	for _, file := range files {
		m.scrubFile(file, allLocations)
	}

	return len(files) == 0, nil
}

func (m *Multi) scrubFile(file meta.File, allLocs map[[16]byte]meta.Location) {
	m.mu.Lock()
	conf := m.config
	m.mu.Unlock()

	var messages []string
	rebuild := false
	for i, id := range file.Locations {
		loc, ok := allLocs[id]
		if !ok {
			rebuild = true
			messages = append(messages, fmt.Sprintf("location %v does not exist", uuid.Fmt(id)))
			continue
		}

		if loc.Dead {
			rebuild = true
			messages = append(messages, fmt.Sprintf("location %v is marked dead", uuid.Fmt(id)))
			continue
		}

		st := m.finder.StoreFor(id)
		if st == nil {
			messages = append(messages, fmt.Sprintf("on %v, but it is not online (warning only)", uuid.Fmt(id)))
			continue
		}

		localKey := localKeyFor(&file, i)
		_, err := st.Stat(localKey)
		if err != nil {
			if err == store.ErrNotFound {
				rebuild = true
				messages = append(messages, fmt.Sprintf("missing from %v", uuid.Fmt(id)))
				continue
			}
			messages = append(messages, fmt.Sprintf("on %v, but it returned %v (warning only)", uuid.Fmt(id), err))
			continue
		}
	}

	if len(file.Locations) != conf.Total || int(file.DataChunks) != conf.Need {
		rebuild = true
		messages = append(messages, fmt.Sprintf("has redundancy %v of %v, but want %v of %v",
			file.DataChunks, len(file.Locations), conf.Need, conf.Total))
	}

	for _, msg := range messages {
		log.Printf("scan on %v: %v", file.Path, msg)
	}

	if rebuild {
		data, hash, err := m.Get(file.Path)
		if err != nil {
			log.Printf("scan on %v: couldn't get for rebuild: %v", file.Path, err)
			return
		}

		err = m.CAS(file.Path,
			store.CASV{Present: true, SHA256: hash},
			store.CASV{Present: true, SHA256: hash, Data: data})
		if err != nil {
			log.Printf("scan on %v: couldn't cas for rebuild: %v", file.Path, err)
			return
		}

		log.Printf("scan on %v: successfully rebuilt", file.Path)
	}
}
