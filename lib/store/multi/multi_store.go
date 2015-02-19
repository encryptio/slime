package multi

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/retry"
	"git.encryptio.com/slime/lib/rs"
	"git.encryptio.com/slime/lib/rs/gf"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/uuid"

	"git.encryptio.com/kvl"
)

var (
	ErrInsufficientStores = errors.New("not enough stores to match redundancy level")
	ErrInsufficientChunks = errors.New("not enough chunks available")
	ErrBadHash            = errors.New("bad checksum after reconstruction")
	ErrTooManyRetries     = errors.New("too many retries")
)

func localKeyFor(file *meta.File, idx int) string {
	return fmt.Sprintf("%v_%x_%v",
		uuid.Fmt(file.PrefixID), file.SHA256[:8], idx)
}

func (m *Multi) UUID() [16]byte {
	return m.uuid
}

func (m *Multi) Name() string {
	return "multi"
}

func (m *Multi) getFile(key string) (*meta.File, error) {
	ret, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		f, err := layer.GetFile(key)
		if err != nil {
			return nil, err
		}

		if f == nil {
			return nil, nil
		}
		return f, nil
	})
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, err
	}
	return ret.(*meta.File), nil
}

func (m *Multi) Get(key string) ([]byte, [32]byte, error) {
	var zeroes [32]byte

	r := retry.New(5)
	for r.Next() {
		f, err := m.getFile(key)
		if err != nil {
			return nil, zeroes, err
		}

		if f == nil {
			return nil, zeroes, store.ErrNotFound
		}

		data, err := m.reconstruct(f)
		if err != nil {
			f2, err2 := m.getFile(key)
			if err2 != nil {
				return nil, zeroes, err2
			}
			if f2 == nil || f2.PrefixID != f.PrefixID {
				// someone wrote to this file and removed some pieces as we
				// were reading it; retry the read.
				continue
			}
			return nil, zeroes, err
		}

		return data, f.SHA256, err
	}

	return nil, zeroes, ErrTooManyRetries
}

func (m *Multi) reconstruct(f *meta.File) ([]byte, error) {
	chunkData := make([][]byte, len(f.Locations))
	for i := range chunkData {
		st := m.finder.StoreFor(f.Locations[i])
		if st != nil {
			localKey := localKeyFor(f, i)
			chunkData[i], _, _ = st.Get(localKey)
		}
	}

	indicies := make([]int, 0, len(chunkData))
	chunks := make([][]uint32, 0, len(chunkData))
	for i, data := range chunkData {
		if data == nil {
			continue
		}
		chunk := gf.MapToGFWith(data, f.MappingValue)

		indicies = append(indicies, i)
		chunks = append(chunks, chunk)
	}

	if len(chunks) < int(f.DataChunks) {
		return nil, ErrInsufficientChunks
	}

	chunks = chunks[:int(f.DataChunks)]
	indicies = indicies[:int(f.DataChunks)]

	dataVecs := rs.RecoverData(chunks, indicies)
	var data []byte
	for _, vec := range dataVecs {
		data = append(data, gf.MapFromGF(f.MappingValue, vec)...)
	}
	data = data[:int(f.Size)]

	have := sha256.Sum256(data)
	if have != f.SHA256 {
		return nil, ErrBadHash
	}

	return data, nil
}

func (m *Multi) Stat(key string) (*store.Stat, error) {
	var file *meta.File
	_, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		file, err = layer.GetFile(key)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		return nil, err
	}

	if file == nil {
		return nil, store.ErrNotFound
	}

	return &store.Stat{
		SHA256: file.SHA256,
		Size:   int64(file.Size),
	}, nil
}

func splitVector(data []uint32, count int) [][]uint32 {
	perVector := (len(data) + count - 1) / count

	parts := make([][]uint32, count)
	for i := range parts {
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

func (m *Multi) CAS(key string, from, to store.CASV) error {
	var file *meta.File
	if to.Present {
		// pessimistically check before doing work
		_, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
			layer, err := meta.Open(ctx)
			if err != nil {
				return nil, err
			}

			oldFile, err := layer.GetFile(key)
			if err != nil {
				return nil, err
			}

			if !from.Any {
				if from.Present {
					if oldFile == nil {
						return nil, store.ErrCASFailure
					}
					if oldFile.SHA256 != from.SHA256 {
						return nil, store.ErrCASFailure
					}
				} else {
					if oldFile != nil {
						return nil, store.ErrCASFailure
					}
				}
			}

			return nil, nil
		})
		if err != nil {
			return err
		}

		file, err = m.writeChunks(key, to.Data, to.SHA256)
		if err != nil {
			return err
		}
	}

	var oldFile *meta.File
	_, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		oldFile, err = layer.GetFile(key)
		if err != nil {
			return nil, err
		}

		if !from.Any {
			if from.Present {
				if oldFile == nil {
					return nil, store.ErrCASFailure
				}
				if oldFile.SHA256 != from.SHA256 {
					return nil, store.ErrCASFailure
				}
			} else {
				if oldFile != nil {
					return nil, store.ErrCASFailure
				}
			}
		}

		if to.Present {
			err = layer.SetFile(file)
			if err != nil {
				return nil, err
			}
		} else {
			if from.Present || from.Any {
				err = layer.RemoveFilePath(key)
				if err == kvl.ErrNotFound {
					if !from.Any {
						// internal inconsistency
						return nil, store.ErrCASFailure
					}
				} else if err != nil {
					return nil, err
				}
			}
		}

		return nil, nil
	})
	if err != nil {
		m.deleteChunks(file)
		return err
	}

	m.deleteChunks(oldFile)

	return nil
}

func (m *Multi) deleteChunks(file *meta.File) error {
	if file == nil {
		return nil
	}

	storesMap := m.finder.Stores()

	// remove chunks on old stores that are not shared by the new write
	for i, loc := range file.Locations {
		localKey := localKeyFor(file, i)

		st := storesMap[loc]
		if st != nil {
			// TODO: log err
			st.CAS(localKey, store.AnyV, store.MissingV)
		} else {
			// TODO: log delete skip
		}
	}

	return nil
}

func (m *Multi) orderTargets() ([]store.Store, error) {
	m.mu.Lock()
	conf := m.config
	m.mu.Unlock()

	storesMap := m.finder.Stores()

	ret, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		locs, err := layer.AllLocations()
		if err != nil {
			return nil, err
		}
		return locs, nil
	})
	if err != nil {
		return nil, err
	}
	for _, loc := range ret.([]meta.Location) {
		if loc.Dead {
			delete(storesMap, loc.UUID)
		}
	}

	if len(storesMap) < conf.Total {
		return nil, ErrInsufficientStores
	}

	// TODO: better ordering of stores based on allocation preferences
	stores := make([]store.Store, 0, len(storesMap))
	for _, v := range storesMap {
		stores = append(stores, v)
	}
	// shuffle stores
	for i := range stores {
		j := rand.Intn(i + 1)
		stores[i], stores[j] = stores[j], stores[i]
	}

	return stores, nil
}

func (m *Multi) writeChunks(key string, data []byte, sha [32]byte) (*meta.File, error) {
	m.mu.Lock()
	conf := m.config
	m.mu.Unlock()

	stores, err := m.orderTargets()
	if err != nil {
		return nil, err
	}

	mapping, all := gf.MapToGF(data)
	parts := splitVector(all, conf.Need)
	parityParts := make([][]uint32, conf.Total-conf.Need)
	for i := range parityParts {
		parityParts[i] = rs.CreateParity(parts, i+len(parts), nil)
	}
	parts = append(parts, parityParts...)

	file := &meta.File{
		Path:         key,
		Size:         uint64(len(data)),
		WriteTime:    uint64(time.Now().Unix()),
		PrefixID:     uuid.Gen4(),
		DataChunks:   uint16(conf.Need),
		MappingValue: mapping,
		SHA256:       sha,
	}

	file.Locations = make([][16]byte, 0, len(parityParts))
	storeIdx := 0
	for i, part := range parts {
		// TODO: parallelize writes

		partData := gf.MapFromGF(mapping, part)
		localKey := localKeyFor(file, i)

		for {
			if storeIdx >= len(stores) {
				// TODO: cleanup chunks we wrote in earlier iterations
				return nil, ErrInsufficientStores
			}

			st := stores[storeIdx]
			storeIdx++

			err := st.CAS(localKey, store.AnyV, store.DataV(partData))
			if err != nil {
				// TODO: log
				continue
			}

			file.Locations = append(file.Locations, st.UUID())
			break
		}
	}

	return file, nil
}

func (m *Multi) List(after string, limit int) ([]string, error) {
	ret, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		files, err := layer.ListFiles(after, limit)
		if err != nil {
			return nil, err
		}

		return files, nil
	})
	if err != nil {
		return nil, err
	}

	files := ret.([]meta.File)
	names := make([]string, len(files))
	for i, file := range files {
		names[i] = file.Path
	}

	return names, nil
}

// sort.Interface
type int64Slice []int64

func (s int64Slice) Len() int           { return len(s) }
func (s int64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (m *Multi) FreeSpace() (int64, error) {
	m.mu.Lock()
	conf := m.config
	m.mu.Unlock()

	var frees []int64
	for _, st := range m.finder.Stores() {
		free, err := st.FreeSpace()
		if err == nil && free > 0 {
			frees = append(frees, free)
		}
	}

	sort.Sort(int64Slice(frees))

	if len(frees) < conf.Total {
		return 0, nil
	}

	// the minimum of the highest conf.Total free spaces is the
	// space we can fill, including parity
	fillable := frees[len(frees)-conf.Total]

	// removing parity amount
	free := fillable / int64(conf.Total) * int64(conf.Need)

	return free, nil
}
