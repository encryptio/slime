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
	scrubFilesWait  = time.Second * 5
	scrubFilesCount = 100
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

func (m *Multi) scrubFilesLoop() error {
	for {
		select {
		case <-m.tomb.Dying():
			return nil
		case <-time.After(jitterDuration(scrubFilesWait)):
			_, err := m.scrubFilesStep()
			if err != nil {
				log.Printf("Couldn't run scrubFilesStep: %v", err)
			}
		}
	}
}

func (m *Multi) scrubFilesStep() (bool, error) {
	var files []meta.File
	err := m.db.RunTx(func(ctx kvl.Ctx) error {
		files = nil

		layer, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		startKey, err := layer.GetConfig("scrubpos")
		if err != nil {
			return err
		}

		files, err = layer.ListFiles(string(startKey), scrubFilesCount)
		if err != nil {
			return err
		}

		if len(files) > 0 {
			err = layer.SetConfig("scrubpos", []byte(files[len(files)-1].Path))
			if err != nil {
				return err
			}
		} else {
			err = layer.SetConfig("scrubpos", []byte{})
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return false, err
	}

	var locs []meta.Location
	err = m.db.RunTx(func(ctx kvl.Ctx) error {
		layer, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		locs, err = layer.AllLocations()
		return err
	})

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
	for _, id := range file.Locations {
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
		err := m.rebuild(file.Path)
		if err != nil {
			log.Printf("scan on %v: couldn't rebuild: %v", file.Path, err)
			return
		}

		log.Printf("scan on %v: successfully rebuilt", file.Path)
	}
}

func (m *Multi) rebuild(path string) error {
	data, hash, err := m.Get(path, nil)
	if err != nil {
		return err
	}

	err = m.CAS(path,
		store.CASV{Present: true, SHA256: hash},
		store.CASV{Present: true, SHA256: hash, Data: data}, nil)
	if err != nil {
		return err
	}

	return nil
}
