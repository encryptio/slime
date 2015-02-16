package multi

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/rs"
	"git.encryptio.com/slime/lib/rs/gf"
	"git.encryptio.com/slime/lib/store"

	"git.encryptio.com/kvl"
)

var (
	ErrInsufficientStores = errors.New("not enough stores to match redundancy level")
	ErrInsufficientChunks = errors.New("not enough chunks available")
	ErrBadHash            = errors.New("bad checksum after reconstruction")
)

func localKeyFor(file *meta.File, idx int) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%v %x %v",
		file.Path, file.SHA256, file.Size)))
	return fmt.Sprintf("%x %v", hash, idx)
}

func (m *Multi) UUID() [16]byte {
	return m.uuid
}

func (m *Multi) Name() string {
	return "multi"
}

func (m *Multi) Get(key string) ([]byte, error) {
	d, _, err := m.GetWith256(key)
	return d, err
}

func (m *Multi) GetWith256(key string) ([]byte, [32]byte, error) {
	var h [32]byte

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
		return nil, h, err
	}

	if ret == nil {
		return nil, h, store.ErrNotFound
	}

	f := ret.(*meta.File)

	copy(h[:], f.SHA256[:])

	chunkData := make([][]byte, len(f.Locations))
	for i := range chunkData {
		st := m.finder.StoreFor(f.Locations[i])
		if st != nil {
			localKey := localKeyFor(f, i)
			chunkData[i], _ = st.Get(localKey)
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
		return nil, h, ErrInsufficientChunks
	}

	chunks = chunks[:int(f.DataChunks)]
	indicies = indicies[:int(f.DataChunks)]

	dataVecs := rs.RecoverData(chunks, indicies)
	var data []byte
	for _, vec := range dataVecs {
		data = append(data, gf.MapFromGF(f.MappingValue, vec)...)
	}
	data = data[:int(f.Size)]

	hasher := sha256.New()
	hasher.Write(data)
	var have [32]byte
	hasher.Sum(have[:0])

	if have != f.SHA256 {
		return nil, h, ErrBadHash
	}

	return data, h, nil
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

func (m *Multi) Set(key string, data []byte) error {
	return m.SetWith256(key, data, sha256.Sum256(data))
}

func (m *Multi) SetWith256(key string, data []byte, h [32]byte) error {
	file, err := m.writeChunks(key, data, h)
	if err != nil {
		return err
	}

	var oldFile *meta.File
	_, err = m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		oldFile, err = layer.GetFile(key)
		if err != nil {
			return nil, err
		}

		err = layer.SetFile(file)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		return err
	}

	if oldFile != nil {
		m.deleteOldChunks(oldFile, file)
	}

	return nil
}

func (m *Multi) CASWith256(key string, oldH [32]byte, data []byte, newH [32]byte) error {
	var oldFile *meta.File

	// pessimistically check before doing work
	_, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		oldFile, err = layer.GetFile(key)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		return err
	}
	if oldFile == nil || oldFile.SHA256 != oldH {
		return store.ErrCASFailure
	}

	// write data
	file, err := m.writeChunks(key, data, newH)
	if err != nil {
		return err
	}

	// and then do the actual swap
	_, err = m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		oldFile, err = layer.GetFile(key)
		if err != nil {
			return nil, err
		}

		if oldFile == nil || oldFile.SHA256 != oldH {
			return nil, store.ErrCASFailure
		}

		err = layer.SetFile(file)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		return err
	}

	m.deleteOldChunks(oldFile, file)

	return nil
}

func (m *Multi) deleteOldChunks(oldFile, newFile *meta.File) error {
	storesMap := m.finder.Stores()

	// remove chunks on old stores that are not shared by the new write
	for i, loc := range oldFile.Locations {
		if newFile != nil &&
			oldFile.Path == newFile.Path &&
			oldFile.SHA256 == newFile.SHA256 &&
			oldFile.Size == newFile.Size &&
			len(newFile.Locations) > i &&
			newFile.Locations[i] == loc {
			// this is a shared key between the old and new writes, keep it
			continue
		}

		localKey := localKeyFor(oldFile, i)

		st := storesMap[loc]
		if st != nil {
			// TODO: log err
			st.Delete(localKey)
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

			err := st.Set(localKey, partData)
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

func (m *Multi) Delete(key string) error {
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

		if oldFile != nil {
			err = layer.RemoveFilePath(oldFile.Path)
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	})
	if err != nil {
		return err
	}

	if oldFile == nil {
		return store.ErrNotFound
	}

	m.deleteOldChunks(oldFile, nil)

	return err
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
