package multi

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"git.encryptio.com/kvl"
	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/store/storehttp"
	"git.encryptio.com/slime/lib/uuid"
)

var ScanInterval = time.Minute * 5
var TestIntervalBetween = time.Second * 5

type FinderEntry struct {
	Store     store.Store
	Free      int64
	LastCheck time.Time
	Dead      bool
}

// A Finder keeps track of all currently reachable meta.Locations and their
// store.Stores.
type Finder struct {
	db     kvl.DB
	client *http.Client

	stop chan struct{}

	mu     sync.Mutex
	stores map[[16]byte]FinderEntry
}

func NewFinder(db kvl.DB) (*Finder, error) {
	f := &Finder{
		db: db,
		client: &http.Client{
			Timeout: time.Second * 15,
		},
		stop:   make(chan struct{}),
		stores: make(map[[16]byte]FinderEntry, 16),
	}

	go f.scanLoop()
	go f.testLoop()

	return f, nil
}

func (f *Finder) Stop() {
	f.mu.Lock()

	select {
	case <-f.stop:
	default:
		close(f.stop)
	}

	f.mu.Unlock()
}

func (f *Finder) Stores() map[[16]byte]FinderEntry {
	f.mu.Lock()
	ret := make(map[[16]byte]FinderEntry, len(f.stores))
	for k, v := range f.stores {
		ret[k] = v
	}
	f.mu.Unlock()
	return ret
}

func (f *Finder) StoreFor(uuid [16]byte) store.Store {
	f.mu.Lock()
	ret := f.stores[uuid].Store
	f.mu.Unlock()
	return ret
}

func (f *Finder) scanLoop() {
	for {
		err := f.Rescan()
		if err != nil {
			log.Printf("Couldn't scan for locations: %v", err)
		}

		select {
		case <-f.stop:
			return
		case <-time.After(jitterDuration(ScanInterval)):
		}
	}
}

func (f *Finder) Rescan() error {
	ret, err := f.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		locs, err := layer.AllLocations()
		return locs, err
	})

	if err != nil {
		return err
	}

	locs := ret.([]meta.Location)
	searched := make(map[string]struct{})
	for _, loc := range locs {
		_, found := searched[loc.URL]
		if !found {
			searched[loc.URL] = struct{}{}
			f.Scan(loc.URL) // TODO: how to handle this error return?
		}
	}

	return nil
}

func (f *Finder) Scan(url string) error {
	req, err := http.NewRequest("GET", url+"/uuids", nil)
	if err != nil {
		return err
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	rdr := bufio.NewReader(resp.Body)
	for {
		line, err := rdr.ReadString('\n')
		if line == "" && err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		line = strings.TrimSuffix(line, "\n")

		id, err := uuid.Parse(line)
		if err != nil {
			return fmt.Errorf("bad line in response %#v: %v", line, err)
		}

		f.mu.Lock()
		e, found := f.stores[id]
		f.mu.Unlock()
		if !found {
			st, err := storehttp.NewClient(url + "/" + uuid.Fmt(id) + "/")
			if err != nil {
				return err
			}

			free, err := st.FreeSpace(nil)
			if err != nil {
				return err
			}

			f.mu.Lock()
			e, found = f.stores[id]
			if !found {
				dead, _ := f.checkDead(id)

				e = FinderEntry{
					Store:     st,
					Free:      free,
					LastCheck: time.Now(),
					Dead:      dead,
				}

				f.stores[id] = e
			}
			f.mu.Unlock()
		}

		err = f.markActive(url, e.Store.Name(), id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *Finder) checkDead(id [16]byte) (bool, error) {
	dead, err := f.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		loc, err := layer.GetLocation(id)
		if err != nil {
			return nil, err
		}

		if loc == nil {
			return false, nil
		}
		return loc.Dead, nil
	})
	if err != nil {
		return false, err
	}
	return dead.(bool), nil
}

func (f *Finder) markActive(url, name string, id [16]byte) error {
	_, err := f.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		loc, err := layer.GetLocation(id)
		if err != nil {
			return nil, err
		}

		if loc == nil {
			loc = &meta.Location{
				UUID: id,
				Name: "unnamed",
			}
		}

		loc.URL = url
		loc.Name = name
		loc.LastSeen = time.Now().Unix()

		err = layer.SetLocation(*loc)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	return err
}

func (f *Finder) testLoop() {
	for {
		f.test(TestIntervalBetween)

		select {
		case <-f.stop:
			return
		default:
		}
	}
}

func (f *Finder) test(wait time.Duration) {
	for id, fe := range f.Stores() {
		dead, err := f.checkDead(id)
		if err != nil {
			continue
		}

		free, err := fe.Store.FreeSpace(nil)

		f.mu.Lock()
		if err != nil {
			e, ok := f.stores[id]
			if ok {
				e.Store.Close()
				delete(f.stores, id)
			}
		}

		e, ok := f.stores[id]
		if ok {
			e.Free = free
			e.LastCheck = time.Now()
			e.Dead = dead
			f.stores[id] = e
		}
		f.mu.Unlock()

		select {
		case <-f.stop:
			return
		case <-time.After(jitterDuration(wait)):
		}
	}
}
