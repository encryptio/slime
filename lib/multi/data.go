package multi

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"git.encryptio.com/slime/lib/chunk"
	"git.encryptio.com/slime/lib/gf"
	"git.encryptio.com/slime/lib/rs"
	"git.encryptio.com/slime/lib/store"
	"log"
	"strings"
	"sync"
)

var (
	ErrNotEnoughChunks = errors.New("not enough chunks available to reconstruct data")
	ErrNotFound        = errors.New("no such value")
)

type Result struct {
	Data   []byte
	Length int64
	SHA256 [32]byte
}

func splitVector(data []uint32, count int) [][]uint32 {
	perVector := (len(data) + count - 1) / count

	parts := make([][]uint32, count)
	for i := 0; i < count; i++ {
		if len(data) >= perVector {
			parts[i] = data[:perVector]
			data = data[perVector:]
		} else {
			n := make([]uint32, perVector)
			copy(n, data)
			parts[i] = n
			data = nil
		}
	}

	if len(data) > 0 {
		panic("splitVector has leftovers")
	}

	// pad the last with zeroes if needed
	if len(parts[len(parts)-1]) != perVector {
		n := make([]uint32, perVector)
		copy(n, parts[len(parts)-1])
		parts[len(parts)-1] = n
	}

	return parts
}

func (m *Multi) Set(path string, data []byte) error {
	var wg sync.WaitGroup

	if len(data) == 0 {
		return m.Delete(path)
	}

	prefix := fmt.Sprintf("%s.v1d", path)

	mapping, all := gf.MapToGF(data)
	parts := splitVector(all, m.config.ChunksNeed)

	parityParts := make([][]uint32, m.config.ChunksTotal-m.config.ChunksNeed)
	for i := range parityParts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			parityParts[i] = rs.CreateParity(parts, i+len(parts), nil)
		}(i)
	}

	fi := chunk.FileInfo{
		SHA256:       sha256.Sum256(data),
		FullLength:   uint32(len(data)),
		DataChunks:   uint32(m.config.ChunksNeed),
		ParityChunks: uint32(m.config.ChunksTotal - m.config.ChunksNeed),
		MappingValue: mapping,
	}

	wg.Wait()
	parts = append(parts, parityParts...)

	chunks := make([]chunk.Chunk, len(parts))
	for i, part := range parts {
		chunks[i] = chunk.Chunk{
			FileInfo:   fi,
			ChunkIndex: uint32(i),
			Data:       gf.MapFromGF(mapping, part),
		}
	}

	errs := make([]error, len(chunks))
	targets, err := m.findPreferred(len(chunks))
	if err != nil {
		return err
	}

	for i, tgt := range targets {
		wg.Add(1)
		go func(i int, tgt store.Target) {
			defer wg.Done()

			thisName := fmt.Sprintf("%s.v1d%di%d", path, fi.DataChunks, i)
			data, err := chunks[i].MarshalBinary()
			if err != nil {
				log.Printf("Couldn't marshal chunk to binary: %v", err)
				errs[i] = err
				return
			}

			err = tgt.Set(thisName, data)
			if err != nil {
				log.Printf("Couldn't write chunk %d to %v: %v", i, tgt.Name(), err)
				errs[i] = err
				return
			}

			fis, err := tgt.Search(prefix)
			if err != nil {
				log.Printf("Couldn't search %v from %v after write: %v", prefix, tgt.Name(), err)
				errs[i] = err
				return
			}

			for _, fi := range fis {
				if !fi.IsDir && strings.HasPrefix(fi.Name, prefix) && fi.Name != thisName {
					err = tgt.Set(fi.Name, nil)
					if err != nil {
						log.Printf("Couldn't remove %v after write of %v from %v: %v", fi.Name, thisName, tgt.Name(), err)
						errs[i] = err
						return
					}
				}
			}
		}(i, tgt)
	}
	wg.Wait()

	var anyErr error
	for _, err := range errs {
		if err != nil {
			anyErr = err
			break
		}
	}

	errs = make([]error, len(m.targets))
	for i, tgt := range m.targets {
		found := false
		for _, otgt := range targets {
			if otgt == tgt {
				found = true
				break
			}
		}

		if found {
			continue
		}

		wg.Add(1)
		go func(i int, tgt store.Target) {
			defer wg.Done()

			fis, err := tgt.Search(prefix)
			if err != nil {
				log.Printf("Couldn't search %v from %v: %v", prefix, tgt.Name(), err)
				errs[i] = err
				return
			}

			for _, fi := range fis {
				if !fi.IsDir && strings.HasPrefix(fi.Name, prefix) {
					err = tgt.Set(fi.Name, nil)
					if err != nil {
						log.Printf("Couldn't remove %v from %v: %v", fi.Name, tgt.Name(), err)
						errs[i] = err
						return
					}
				}
			}
		}(i, tgt)
	}
	wg.Wait()

	if anyErr != nil {
		for _, err := range errs {
			if err != nil {
				anyErr = err
				break
			}
		}
	}

	return anyErr
}

func (m *Multi) Delete(path string) error {
	var wg sync.WaitGroup
	prefix := fmt.Sprintf("%s.v1d", path)

	errs := make([]error, len(m.targets))
	for i, tgt := range m.targets {
		wg.Add(1)
		go func(i int, tgt store.Target) {
			defer wg.Done()

			fis, err := tgt.Search(prefix)
			if err != nil {
				log.Printf("Couldn't search %v from %v: %v", prefix, tgt.Name(), err)
				errs[i] = err
				return
			}

			for _, fi := range fis {
				if !fi.IsDir && strings.HasPrefix(fi.Name, prefix) {
					err = tgt.Set(fi.Name, nil)
					if err != nil {
						log.Printf("Couldn't remove %v from %v: %v", fi.Name, tgt.Name(), err)
						errs[i] = err
						return
					}
				}
			}
		}(i, tgt)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Multi) Get(path string) (Result, error) {
	var r Result
	var wg sync.WaitGroup

	// search for chunks on all our targets
	prefix := path + ".v1d"
	searchResults := make([][]store.FileInfo, len(m.targets))
	searchErrors := make([]error, len(m.targets))
	for i, tgt := range m.targets {
		wg.Add(1)
		go func(i int, tgt store.Target) {
			defer wg.Done()

			searchResults[i], searchErrors[i] = tgt.Search(prefix)
			if searchErrors[i] != nil {
				log.Printf("Couldn't Search for %v on %v: %v", prefix, tgt.Name(), searchErrors[i])
			}
		}(i, tgt)
	}
	wg.Wait()

	chunkCount := 0
	for _, res := range searchResults {
		for _, fi := range res {
			if !fi.IsDir {
				chunkCount++
			}
		}
	}

	if chunkCount == 0 {
		return r, ErrNotFound
	}

	// load all the chunks we found
	chunks := make([]*chunk.Chunk, chunkCount)
	chunkIndex := 0
	for i, res := range searchResults {
		if len(res) == 0 {
			continue
		}

		wg.Add(1)
		go func(tgt store.Target, res []store.FileInfo, into []*chunk.Chunk) {
			defer wg.Done()

			for i := range res {
				if res[i].IsDir {
					continue
				}

				data, err := tgt.Get(res[i].Name)
				if err != nil {
					log.Printf("Couldn't Get %v from %v: %v", res[i], tgt.Name(), err)
					continue
				}

				chunk := new(chunk.Chunk)
				err = chunk.UnmarshalBinary(data)
				if err != nil {
					log.Printf("Couldn't read chunk %v from %v: %v", res[i], tgt.Name(), err)
					continue
				}

				into[i] = chunk
			}
		}(m.targets[i], res, chunks[chunkIndex:chunkIndex+len(res)])
		chunkIndex += len(res)
	}
	wg.Wait()

	// filter out nil chunks (failed reads)
	for i := 0; i < len(chunks); i++ {
		if chunks[i] == nil {
			copy(chunks[i:], chunks[i+1:])
			chunks = chunks[:len(chunks)-1]
			i--
		}
	}

	if len(chunks) == 0 {
		return r, ErrNotEnoughChunks
	}

	// verify that the chunks we got are actually for the same file
	for i := 1; i < len(chunks); i++ {
		if chunks[i].FileInfo != chunks[0].FileInfo {
			log.Printf("Got non-matching chunk for prefix %v", prefix)

			// remove chunks[i]
			copy(chunks[i:], chunks[i+1:])
			chunks = chunks[:len(chunks)-1]
			i--
		}
	}

	if len(chunks) < int(chunks[0].DataChunks) {
		return r, ErrNotEnoughChunks
	}

	// build the rs vectors
	vecs := make([][]uint32, 0, chunks[0].DataChunks)
	indices := make([]int, 0, chunks[0].DataChunks)
	for _, chunk := range chunks {
		mapping := chunks[0].MappingValue

		vecs = append(vecs, gf.MapToGFWith(chunk.Data, mapping))
		indices = append(indices, int(chunk.ChunkIndex))

		if len(vecs) == int(chunks[0].DataChunks) {
			break
		}
	}

	// TODO: skip this if we have the data already
	dataVecs := rs.RecoverData(vecs, indices)

	data := make([]byte, 0, chunks[0].FullLength+chunks[0].DataChunks*4)
	for _, vec := range dataVecs {
		data = append(data, gf.MapFromGF(chunks[0].MappingValue, vec)...)
	}

	r.Data = data[:chunks[0].FullLength]
	r.Length = int64(chunks[0].FullLength)
	copy(r.SHA256[:], chunks[0].SHA256[:])

	return r, nil
}

func trimChunkID(name string) string {
	n := name
	ok := true

	mustTrim := func(cutset string) {
		oldLen := len(n)
		n = strings.TrimRight(n, cutset)
		if len(n) == oldLen {
			ok = false
		}
	}

	// remove .v1dNiN
	mustTrim("0123456789")
	mustTrim("i")
	mustTrim("0123456789")
	mustTrim("d")
	mustTrim("1")
	mustTrim("v")
	mustTrim(".")

	if ok {
		return n
	} else {
		return name
	}
}

func (m *Multi) List(prefix string) ([]store.FileInfo, error) {
	var wg sync.WaitGroup

	// search for chunks on all our targets
	searchResults := make([][]store.FileInfo, len(m.targets))
	for i, tgt := range m.targets {
		wg.Add(1)
		go func(i int, tgt store.Target) {
			defer wg.Done()

			var err error
			searchResults[i], err = tgt.Search(prefix)
			if err != nil {
				log.Printf("Couldn't Search for %v on %v: %v", prefix, tgt.Name(), err)
			}
		}(i, tgt)
	}
	wg.Wait()

	var ret []store.FileInfo
	added := make(map[store.FileInfo]struct{})
	for _, res := range searchResults {
		for _, fi := range res {
			var outFi store.FileInfo
			if fi.IsDir {
				outFi = fi
			} else {
				outFi = store.FileInfo{trimChunkID(fi.Name), false}
			}
			_, ok := added[outFi]
			if !ok {
				ret = append(ret, outFi)
				added[outFi] = struct{}{}
			}
		}
	}

	return ret, nil
}

func (m *Multi) Stat(path string) (Result, error) {
	var r Result

	var foundChunk *chunk.Chunk
	prefix := path + ".v1d"
	for _, tgt := range m.targets {
		fis, err := tgt.Search(prefix)
		if err != nil {
			return r, err
		}

		for _, fi := range fis {
			if !fi.IsDir && trimChunkID(fi.Name) == path {
				data, err := tgt.Get(fi.Name)
				if err != nil {
					log.Printf("Couldn't Get %v from %v: %v", fi.Name, tgt.Name(), err)
					continue
				}

				loadedChunk := new(chunk.Chunk)
				err = loadedChunk.UnmarshalBinary(data)
				if err != nil {
					log.Printf("Couldn't read chunk %v from %v: %v", fi.Name, tgt.Name(), err)
					continue
				}

				foundChunk = loadedChunk
			}
		}
	}

	if foundChunk == nil {
		return r, ErrNotFound
	}

	copy(r.SHA256[:], foundChunk.SHA256[:])
	r.Length = int64(foundChunk.FullLength)

	return r, nil
}
