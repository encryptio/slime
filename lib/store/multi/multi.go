// package multi provides a store.Store which redundantly stores information.
//
// It uses Reed-Solomon erasure coding for efficient storage, at the cost of
// having to refer to many inner stores to read and write data.
package multi

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"

	"git.encryptio.com/slime/lib/meta"
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

	loadConfigInterval = time.Minute * 15
)

type BadConfigError string

func (e BadConfigError) Error() string {
	return "bad MultiConfig: " + string(e)
}

type Multi struct {
	db     kvl.DB
	finder *Finder
	uuid   [16]byte

	stop chan struct{}

	mu     sync.Mutex
	config multiConfig
}

type multiConfig struct {
	Need  int
	Total int
}

func checkConfig(config multiConfig) error {
	if config.Need <= 0 {
		return BadConfigError("need is non-positive")
	}
	if config.Total <= 0 {
		return BadConfigError("total is non-positive")
	}
	if config.Need > config.Total {
		return BadConfigError("need is greater than total")
	}
	if config.Total > 100 {
		return BadConfigError("total is too large")
	}
	return nil
}

func NewMulti(db kvl.DB, finder *Finder) (*Multi, error) {
	m := &Multi{
		db:     db,
		finder: finder,
		stop:   make(chan struct{}),
	}

	err := m.loadUUID()
	if err != nil {
		return nil, err
	}

	err = m.loadConfig()
	if err != nil {
		return nil, err
	}

	go m.loadConfigLoop(loadConfigInterval)

	return m, nil
}

func (m *Multi) Stop() {
	m.mu.Lock()

	select {
	case <-m.stop:
	default:
		close(m.stop)
	}

	m.mu.Unlock()
}

func (m *Multi) SetRedundancy(need, total int) error {
	m.mu.Lock()
	conf := m.config
	m.mu.Unlock()

	conf.Need = need
	conf.Total = total

	err := checkConfig(conf)
	if err != nil {
		return err
	}

	_, err = m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		err = layer.SetConfig("need", strconv.AppendInt(nil, int64(conf.Need), 10))
		if err != nil {
			return nil, err
		}
		err = layer.SetConfig("total", strconv.AppendInt(nil, int64(conf.Total), 10))
		if err != nil {
			return nil, err
		}

		return nil, nil
	})

	m.mu.Lock()
	m.config = conf
	m.mu.Unlock()

	return nil
}

func (m *Multi) UUID() [16]byte {
	return m.uuid
}

func (m *Multi) Get(key string) ([]byte, error) {
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
		return nil, store.ErrNotFound
	}

	f := ret.(*meta.File)

	chunkData := make([][]byte, len(f.Locations))
	var wg sync.WaitGroup
	for i := range chunkData {
		st := m.finder.StoreFor(f.Locations[i])
		localKey := fmt.Sprintf("%x %v %v", f.SHA256, f.Size, i)

		wg.Add(1)
		go func(into *[]byte, st store.Store, localKey string) {
			*into, _ = st.Get(localKey)
			wg.Done()
		}(&chunkData[i], st, localKey)
	}
	wg.Wait()

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

	h := sha256.New()
	h.Write(data)
	var have [32]byte
	h.Sum(have[:0])

	if have != f.SHA256 {
		return nil, ErrBadHash
	}

	return data, nil
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
	m.mu.Lock()
	conf := m.config
	m.mu.Unlock()

	storesMap := m.finder.Stores()
	if len(storesMap) < conf.Total {
		return ErrInsufficientStores
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

	mapping, all := gf.MapToGF(data)
	parts := splitVector(all, conf.Need)
	parityParts := make([][]uint32, conf.Total-conf.Need)
	var wg sync.WaitGroup
	for i := range parityParts {
		wg.Add(1)
		go func(i int) {
			parityParts[i] = rs.CreateParity(parts, i+len(parts), nil)
			wg.Done()
		}(i)
	}
	wg.Wait()
	parts = append(parts, parityParts...)

	writeFile := &meta.File{
		Path:         key,
		Size:         uint64(len(data)),
		WriteTime:    uint64(time.Now().Unix()),
		DataChunks:   uint16(conf.Need),
		MappingValue: mapping,
	}

	h := sha256.New()
	h.Write(data)
	h.Sum(writeFile.SHA256[:0])

	writeFile.Locations = make([][16]byte, 0, len(parityParts))
	storeIdx := 0
	for i, part := range parts {
		// TODO: parallelize writes

		partData := gf.MapFromGF(mapping, part)
		localKey := fmt.Sprintf("%x %v %v", writeFile.SHA256, writeFile.Size, i)

		for {
			if storeIdx >= len(stores) {
				// TODO: cleanup chunks we wrote in earlier iterations
				return ErrInsufficientStores
			}

			st := stores[storeIdx]
			storeIdx++

			err := st.Set(localKey, partData)
			if err != nil {
				// TODO: log
				continue
			}

			writeFile.Locations = append(writeFile.Locations, st.UUID())
			break
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

		err = layer.SetFile(writeFile)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		return err
	}

	if oldFile != nil {
		// remove chunks on old stores that are not shared by the new write
		for i, loc := range oldFile.Locations {
			if oldFile.SHA256 == writeFile.SHA256 &&
				oldFile.Size == writeFile.Size &&
				writeFile.Locations[i] == loc {
				// this is a shared key between the old and new writes, keep it
				continue
			}
			localKey := fmt.Sprintf("%x %v %v", oldFile.SHA256, oldFile.Size, i)

			st := storesMap[loc]
			if st != nil {
				// TODO: log err
				st.Delete(localKey)
			}
		}
	}

	return nil
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

	for i, loc := range oldFile.Locations {
		localKey := fmt.Sprintf("%x %v %v", oldFile.SHA256, oldFile.Size, i)

		st := m.finder.StoreFor(loc)
		if st != nil {
			// TODO: log err
			st.Delete(localKey)
		}
	}

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

func (m *Multi) loadUUID() error {
	ret, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		id, err := layer.GetConfig("uuid")
		if err != nil {
			return nil, err
		}

		if len(id) == 0 {
			newId := uuid.Gen4()
			id = newId[:]
			err = layer.SetConfig("uuid", id)
			if err != nil {
				return nil, err
			}
		}

		return id, nil
	})
	if err != nil {
		return err
	}

	copy(m.uuid[:], ret.([]byte))
	return nil
}

func (m *Multi) loadConfig() error {
	_, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		conf := m.config
		m.mu.Unlock()

		needBytes, err := layer.GetConfig("need")
		if err != nil {
			return nil, err
		}
		if needBytes == nil {
			needBytes = []byte("3")
		}
		need, err := strconv.ParseInt(string(needBytes), 10, 0)
		if err != nil {
			return nil, err
		}
		conf.Need = int(need)

		totalBytes, err := layer.GetConfig("total")
		if err != nil {
			return nil, err
		}
		if totalBytes == nil {
			totalBytes = []byte("5")
		}
		total, err := strconv.ParseInt(string(totalBytes), 10, 0)
		if err != nil {
			return nil, err
		}
		conf.Total = int(total)

		err = checkConfig(conf)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		m.config = conf
		m.mu.Unlock()

		return nil, nil
	})
	return err
}

func (m *Multi) loadConfigLoop(interval time.Duration) {
	for {
		select {
		case <-m.stop:
			return
		case <-time.After(interval):
			err := m.loadConfig()
			if err != nil {
				log.Printf("Couldn't load config: %v", err)
			}
		}
	}
}
