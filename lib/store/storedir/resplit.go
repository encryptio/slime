package storedir

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func (ds *Directory) resplitLoop() {
	for {
		ds.resplit()

		select {
		case <-time.After(30 * time.Minute):
		case <-ds.stop:
			return
		}
	}
}

func (ds *Directory) resplit() error {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	for i := 0; i < len(ds.splits); i++ {
		s := ds.splits[i]

		fis, err := ioutil.ReadDir(filepath.Join(ds.Dir, "data", s.Name))
		if err != nil {
			return err
		}

		if len(fis) < ds.minSplitSize {
			ds.mu.RUnlock()
			err := ds.resplitMerge(s.Name)
			ds.mu.RLock()
			if err != nil {
				return err
			}
		}

		if len(fis) > ds.maxSplitSize {
			ds.mu.RUnlock()
			err := ds.resplitSplit(s.Name)
			ds.mu.RLock()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (ds *Directory) resplitMerge(name string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if len(ds.splits) < 2 {
		return nil
	}

	fromIdx := -1
	for i := 0; i < len(ds.splits); i++ {
		if ds.splits[i].Name == name {
			fromIdx = i
			break
		}
	}
	if fromIdx == -1 {
		return nil
	}

	var toIdx int
	if fromIdx == 0 {
		toIdx = 1
	} else {
		toIdx = fromIdx - 1
	}

	fromDir := filepath.Join(ds.Dir, "data", ds.splits[fromIdx].Name)
	toDir := filepath.Join(ds.Dir, "data", ds.splits[toIdx].Name)

	contents, err := ioutil.ReadDir(fromDir)
	if err != nil {
		return err
	}

	for _, fi := range contents {
		fromPath := filepath.Join(fromDir, fi.Name())
		toPath := filepath.Join(toDir, fi.Name())

		err := os.Rename(fromPath, toPath)
		if err != nil {
			// TODO: mark this Directory as failed and do not respond to further requests
			log.Printf("Couldn't move %v to %v during resplitting: %v", fromPath, toPath, err)
			return err
		}
	}

	os.Remove(fromDir)

	if ds.splits[toIdx].Low > ds.splits[fromIdx].Low {
		ds.splits[toIdx].Low = ds.splits[fromIdx].Low
	}
	if ds.splits[toIdx].High < ds.splits[fromIdx].High {
		ds.splits[toIdx].High = ds.splits[fromIdx].High
	}

	// remove ds.splits[fromIdx] in place
	copy(ds.splits[fromIdx:], ds.splits[fromIdx+1:])
	ds.splits = ds.splits[:len(ds.splits)-1]

	return nil
}

func (ds *Directory) resplitSplit(name string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	idx := -1
	for i := 0; i < len(ds.splits); i++ {
		if ds.splits[i].Name == name {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil
	}

	dir := filepath.Join(ds.Dir, "data", ds.splits[idx].Name)

	contents, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	if len(contents) < 2 {
		return nil
	}

	var decoded []string
	for _, fi := range contents {
		keyBytes, err := base64.URLEncoding.DecodeString(fi.Name())
		if err != nil {
			continue
		}

		decoded = append(decoded, string(keyBytes))
	}

	sort.Strings(decoded)

	toOriginal := decoded[:len(decoded)/2]
	toNew := decoded[len(decoded)/2:]

	var newDir string
	var newName string
	for {
		newName = fmt.Sprintf("%v", rand.Intn(len(ds.splits)+10))
		newDir = filepath.Join(ds.Dir, "data", newName)
		err := os.Mkdir(newDir, 0777)
		if err != nil {
			if os.IsExist(err) {
				continue
			}
			return err
		}
		break
	}

	for _, key := range toNew {
		name := base64.URLEncoding.EncodeToString([]byte(key))
		oldPath := filepath.Join(dir, name)
		newPath := filepath.Join(newDir, name)
		err := os.Rename(oldPath, newPath)
		if err != nil {
			// TODO: mark this Directory as failed and do not respond to further requests
			log.Printf("Couldn't move %v to %v during resplitting: %v", oldPath, newPath, err)
			return err
		}
	}

	newSplit := split{
		Name: newName,
		Low:  toNew[0],
		High: toNew[len(toNew)-1],
	}

	ds.splits[idx].Low = toOriginal[0]
	ds.splits[idx].High = toOriginal[len(toOriginal)-1]

	// insert
	ds.splits = append(ds.splits[:idx+1], append([]split{newSplit}, ds.splits[idx+1:]...)...)

	return nil
}
