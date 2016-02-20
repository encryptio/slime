package multi

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/encryptio/slime/internal/meta"
	"github.com/encryptio/slime/internal/retry"
	"github.com/encryptio/slime/internal/rs"
	"github.com/encryptio/slime/internal/rs/gf"
	"github.com/encryptio/slime/internal/store"
	"github.com/encryptio/slime/internal/uuid"

	"github.com/encryptio/kvl"
)

var (
	ErrInsufficientStores = errors.New("not enough stores to match redundancy level")
	ErrInsufficientChunks = errors.New("not enough chunks available")
	ErrBadHash            = errors.New("bad checksum after reconstruction")
	ErrTooManyRetries     = errors.New("too many retries")

	dataOnlyTimeout = time.Second * 5
)

func localKeyFor(file *meta.File, idx int) string {
	return fmt.Sprintf("%v_%x_%v",
		uuid.Fmt(file.PrefixID), file.SHA256[:8], idx)
}

func prefixIDFromLocalKey(key string) ([16]byte, error) {
	parts := strings.SplitN(key, "_", 2)
	if len(parts) != 2 {
		return [16]byte{}, errors.New("not enough key components")
	}
	return uuid.Parse(parts[0])
}

func (m *Multi) UUID() [16]byte {
	return m.uuid
}

func (m *Multi) Name() string {
	return "multi"
}

func (m *Multi) getFile(key string) (*meta.File, error) {
	var file *meta.File
	err := m.db.RunReadTx(func(ctx kvl.Ctx) error {
		layer, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		file, err = layer.GetFile(key)
		return err
	})
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (m *Multi) Get(key string, opts store.GetOptions) ([]byte, store.Stat, error) {
	r := retry.New(10)
	for r.Next() {
		f, err := m.getFile(key)
		if err != nil {
			return nil, store.Stat{}, err
		}

		if f == nil {
			return nil, store.Stat{}, store.ErrNotFound
		}

		data, err := m.reconstruct(f, opts)
		if err != nil {
			f2, err2 := m.getFile(key)
			if err2 != nil {
				return nil, store.Stat{}, err2
			}
			if f2 == nil || f2.PrefixID != f.PrefixID {
				// someone wrote to this file and removed some pieces as we
				// were reading it; retry the read.
				continue
			}
			return nil, store.Stat{}, err
		}

		return data, store.Stat{
			SHA256:    f.SHA256,
			Size:      int64(f.Size),
			WriteTime: f.WriteTime,
		}, err
	}

	return nil, store.Stat{}, ErrTooManyRetries
}

func (m *Multi) getChunkData(f *meta.File, opts store.GetOptions) [][]byte {
	var wg sync.WaitGroup
	defer wg.Wait()

	localCancel := make(chan struct{})
	defer close(localCancel)

	chunkData := make([][]byte, len(f.Locations))

	type chunkResult struct {
		index int
		data  []byte
	}

	// NB: buffer size is to avoid deadlocks between defer wg.Wait() and results
	// writes during local and upstream cancellation
	results := make(chan chunkResult, len(f.Locations))

	work := func(i int) {
		st := m.finder.StoreFor(f.Locations[i])
		var data []byte
		if st != nil {
			localKey := localKeyFor(f, i)
			data, _, _ = st.Get(localKey, store.GetOptions{
				Cancel:   localCancel,
				NoVerify: opts.NoVerify,
			})
			// TODO: log err?
		}
		results <- chunkResult{i, data}
		wg.Done()
	}

	// try to get data only at first
	for i := 0; i < int(f.DataChunks); i++ {
		wg.Add(1)
		go work(i)
	}

	timer := time.NewTimer(dataOnlyTimeout)
	timeoutChan := timer.C
	defer timer.Stop()

	returned := 0
	got := 0
	for {
		select {
		case res := <-results:
			returned++
			if res.data != nil {
				got++
				chunkData[res.index] = res.data
			} else {
				if timeoutChan != nil {
					// failed to get one of the data chunks, go get the parity
					timeoutChan = nil
					for i := int(f.DataChunks); i < len(f.Locations); i++ {
						wg.Add(1)
						go work(i)
					}
				}
			}

			if got >= int(f.DataChunks) || returned == len(f.Locations) {
				return chunkData
			}
		case <-timeoutChan:
			// data was too slow returning, go get the parity
			timeoutChan = nil
			for i := int(f.DataChunks); i < len(f.Locations); i++ {
				wg.Add(1)
				go work(i)
			}
		case <-opts.Cancel:
			return chunkData
		}
	}
}

func (m *Multi) reconstruct(f *meta.File, opts store.GetOptions) ([]byte, error) {
	chunkData := m.getChunkData(f, opts)

	select {
	case <-opts.Cancel:
		return nil, store.ErrCancelled
	default:
	}

	rawDataAvailable := f.MappingValue == 0
	if rawDataAvailable {
		for i := 0; i < int(f.DataChunks); i++ {
			if chunkData[i] == nil {
				rawDataAvailable = false
				break
			}
		}
	}

	data := make([]byte, 0, int(f.Size)+16)
	if rawDataAvailable {
		// fast path:
		// - chunkData[0..f.DataChunks-1] are non-nil
		// - f.MappingValue == 0

		// TODO: fast path when f.MappingValue != 0
		for i := 0; i < int(f.DataChunks); i++ {
			data = append(data, chunkData[i]...)
		}
		data = data[:int(f.Size)]
	} else {
		// slow path: full reconstruction

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
		for _, vec := range dataVecs {
			data = append(data, gf.MapFromGF(f.MappingValue, vec)...)
		}
		data = data[:int(f.Size)]
	}

	if !opts.NoVerify {
		have := sha256.Sum256(data)
		if have != f.SHA256 {
			return nil, ErrBadHash
		}
	}

	return data, nil
}

func (m *Multi) Stat(key string, cancel <-chan struct{}) (store.Stat, error) {
	file, err := m.getFile(key)
	if err != nil {
		return store.Stat{}, err
	}

	if file == nil {
		return store.Stat{}, store.ErrNotFound
	}

	return store.Stat{
		SHA256:    file.SHA256,
		Size:      int64(file.Size),
		WriteTime: file.WriteTime,
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

func (m *Multi) CAS(key string, from, to store.CASV, cancel <-chan struct{}) error {
	var file *meta.File
	prefixid := uuid.Gen4()

	if to.Present {
		// check before doing work; additionally, add a WAL entry
		err := m.db.RunTx(func(ctx kvl.Ctx) error {
			layer, err := meta.Open(ctx)
			if err != nil {
				return err
			}

			oldFile, err := layer.GetFile(key)
			if err != nil {
				return err
			}

			if !from.Any {
				if from.Present {
					if oldFile == nil {
						return store.ErrCASFailure
					}
					if oldFile.SHA256 != from.SHA256 {
						return store.ErrCASFailure
					}
				} else {
					if oldFile != nil {
						return store.ErrCASFailure
					}
				}
			}

			err = layer.WALMark(prefixid)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			return err
		}

		file, err = m.writeChunks(key, to.Data, to.SHA256, prefixid)
		if err != nil {
			return err
		}
	}

	var oldFile *meta.File
	err := m.db.RunTx(func(ctx kvl.Ctx) error {
		layer, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		oldFile, err = layer.GetFile(key)
		if err != nil {
			return err
		}

		if !from.Any {
			if from.Present {
				if oldFile == nil {
					return store.ErrCASFailure
				}
				if oldFile.SHA256 != from.SHA256 {
					return store.ErrCASFailure
				}
			} else {
				if oldFile != nil {
					return store.ErrCASFailure
				}
			}
		}

		if to.Present {
			err = layer.SetFile(file)
			if err != nil {
				return err
			}

			err = layer.WALClear(prefixid)
			if err != nil {
				return err
			}
		} else {
			if from.Present || from.Any {
				err = layer.RemoveFilePath(key)
				if err == kvl.ErrNotFound {
					if !from.Any {
						// internal inconsistency
						return store.ErrCASFailure
					}
				} else if err != nil {
					return err
				}
			}
		}

		return nil
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

	var wg sync.WaitGroup
	for i, loc := range file.Locations {
		wg.Add(1)
		go func(localKey string, loc [16]byte) {
			defer wg.Done()
			st := m.finder.StoreFor(loc)
			if st != nil {
				// TODO: log err
				st.CAS(localKey, store.AnyV, store.MissingV, nil)
			} else {
				// TODO: log delete skip
			}
		}(localKeyFor(file, i), loc)
	}
	wg.Wait()

	return nil
}

func (m *Multi) getStoreWeights() map[[16]byte]int64 {
	finderEntries := m.finder.Stores()

	weights := make(map[[16]byte]int64, len(finderEntries))
	for id, fe := range finderEntries {
		weights[id] = 10000000000 + fe.Free
	}

	return weights
}

func (m *Multi) orderTargets() ([]store.Store, error) {
	m.mu.Lock()
	conf := m.config
	m.mu.Unlock()

	storesMap := make(map[[16]byte]store.Store)
	for id, fe := range m.finder.Stores() {
		storesMap[id] = fe.Store
	}

	var locs []meta.Location
	err := m.db.RunReadTx(func(ctx kvl.Ctx) error {
		layer, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		locs, err = layer.AllLocations()
		return err
	})
	if err != nil {
		return nil, err
	}

	for _, loc := range locs {
		if loc.Dead {
			delete(storesMap, loc.UUID)
		}
	}

	if len(storesMap) < conf.Total {
		return nil, ErrInsufficientStores
	}

	weights := m.getStoreWeights()

	stores := make([]store.Store, 0, len(storesMap))
	for len(weights) > 0 {
		totalWeight := int64(0)
		for _, w := range weights {
			totalWeight += w
		}

		r := rand.Int63n(totalWeight)
		var chosenID [16]byte
		for id, w := range weights {
			if r <= w {
				chosenID = id
				break
			}
			r -= w
		}

		st := storesMap[chosenID]
		if st != nil {
			stores = append(stores, st)
		}

		delete(weights, chosenID)
	}

	return stores, nil
}

func (m *Multi) writeChunks(key string, data []byte, sha [32]byte, prefixid [16]byte) (*meta.File, error) {
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
		WriteTime:    time.Now().Unix(),
		PrefixID:     prefixid,
		DataChunks:   uint16(conf.Need),
		MappingValue: mapping,
		SHA256:       sha,
	}

	storeCh := make(chan store.Store, len(stores))
	for _, st := range stores {
		storeCh <- st
	}
	close(storeCh)

	file.Locations = make([][16]byte, len(parts))
	errs := make(chan error)
	for i, part := range parts {
		go func(i int, part []uint32) {
			data := gf.MapFromGF(mapping, part)
			localKey := localKeyFor(file, i)
			dataV := store.DataV(data)
			for st := range storeCh {
				err := st.CAS(localKey, store.AnyV, dataV, nil)
				if err != nil {
					// TODO: log
					continue
				}

				file.Locations[i] = st.UUID()
				errs <- nil
				return
			}
			errs <- ErrInsufficientStores
		}(i, part)
	}

	var theError error
	for range parts {
		err := <-errs
		if err != nil && theError == nil {
			theError = err
		}
	}

	if theError != nil {
		// attempt to clean up any parts we wrote
		for i := range parts {
			st := m.finder.StoreFor(file.Locations[i])
			if st != nil {
				localKey := localKeyFor(file, i)
				st.CAS(localKey, store.AnyV, store.MissingV, nil)
			}
		}

		return nil, theError
	}

	return file, nil
}

func (m *Multi) List(after string, limit int, cancel <-chan struct{}) ([]string, error) {
	var files []meta.File
	err := m.db.RunReadTx(func(ctx kvl.Ctx) error {
		layer, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		files, err = layer.ListFiles(after, limit)
		return err
	})
	if err != nil {
		return nil, err
	}

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

func (m *Multi) FreeSpace(cancel <-chan struct{}) (int64, error) {
	m.mu.Lock()
	conf := m.config
	m.mu.Unlock()

	var frees []int64
	for _, fe := range m.finder.Stores() {
		frees = append(frees, fe.Free)
	}
	sort.Sort(int64Slice(frees))

	var free int64
	var lastStoreFree int64
	for i := 0; i <= len(frees)-conf.Total; i++ {
		free += (frees[i] - lastStoreFree) * int64(len(frees)-i+1) * int64(conf.Need) / int64(conf.Total)
		lastStoreFree = frees[i]
	}

	return free, nil
}
