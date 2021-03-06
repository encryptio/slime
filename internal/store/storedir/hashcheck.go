package storedir

import (
	"encoding/base64"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/encryptio/slime/internal/store"
	"github.com/encryptio/slime/internal/uuid"
)

func (ds *Directory) Hashcheck() (good, bad int64) {
	after := ""
	for {
		var goodStep, badStep int64
		goodStep, badStep, after = ds.hashstepInner(after)
		good += goodStep
		bad += badStep

		if after == "" {
			return
		}
	}
}

func (ds *Directory) hashcheckLoop() error {
	for {
		_, bad := ds.hashstep()
		if bad != 0 {
			log.Printf("Found %v bad items hash check on %v\n",
				bad, uuid.Fmt(ds.UUID()))
		}

		select {
		case <-time.After(5 * time.Second):
		case <-ds.tomb.Dying():
			return nil
		}
	}
}

func (ds *Directory) hashstep() (good, bad int64) {
	statePath := filepath.Join(ds.Dir, "hashcheck-at")
	after := ""

	data, err := ioutil.ReadFile(statePath)
	if err == nil {
		after = string(data)
	} else if !os.IsNotExist(err) {
		log.Printf("Couldn't read %v: %v", statePath, err)
		return
	}

	good, bad, after = ds.hashstepInner(after)

	err = ioutil.WriteFile(statePath, []byte(after), 0666)
	if err != nil {
		log.Printf("Couldn't write to %v: %v", statePath, err)
		return
	}

	return
}

func (ds *Directory) hashstepInner(afterIn string) (good, bad int64, after string) {
	after = afterIn

	keys, err := ds.List(after, 100, nil)
	if err != nil {
		log.Printf("Couldn't list in %v for hash check: %v", ds.Dir, err)
		return
	}

	if len(keys) == 0 {
		after = ""
		return
	}

	for _, key := range keys {
		data, _, err := ds.Get(key, store.GetOptions{})
		if err != nil && err != store.ErrNotFound {
			bad++
		} else {
			good++
		}

		wait := ds.perFileWait + time.Duration(len(data))*ds.perByteWait
		data = nil // free memory before sleep
		if wait > 0 {
			time.Sleep(wait)
		}

		after = key

		select {
		case <-ds.tomb.Dying():
			return
		default:
		}
	}

	return
}

func (ds *Directory) quarantine(key, path string) {
	quarantinePath := filepath.Join(ds.Dir, "quarantine", base64.URLEncoding.EncodeToString([]byte(key)))

	err := os.Rename(path, quarantinePath)
	if err != nil {
		log.Printf("Couldn't quarantine %v into %v: %v",
			path, quarantinePath, err)
	}
}
