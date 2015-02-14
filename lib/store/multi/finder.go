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
	"git.encryptio.com/slime/lib/uuid"
)

var ScanInterval = time.Minute * 5
var TestIntervalBetween = time.Second * 45

// A Finder keeps track of all currently reachable meta.Locations and their
// store.Stores.
type Finder struct {
	db     kvl.DB
	client *http.Client

	stop     chan struct{}
	scanDone chan struct{}

	mu     sync.Mutex
	stores map[[16]byte]store.Store
}

func NewFinder(db kvl.DB) (*Finder, error) {
	f := &Finder{
		db: db,
		client: &http.Client{
			Timeout: time.Second * 15,
		},
		stop:     make(chan struct{}),
		stores:   make(map[[16]byte]store.Store, 16),
		scanDone: make(chan struct{}),
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

func (f *Finder) Stores() map[[16]byte]store.Store {
	f.mu.Lock()
	ret := make(map[[16]byte]store.Store, len(f.stores))
	for k, v := range f.stores {
		ret[k] = v
	}
	f.mu.Unlock()
	return ret
}

func (f *Finder) scanLoop() {
	for {
		err := f.scanStart()
		if err != nil {
			log.Printf("Couldn't start scanning for locations: %v", err)
		}

		select {
		case f.scanDone <- struct{}{}:
		default:
		}

		select {
		case <-f.stop:
			return
		case <-time.After(ScanInterval):
		}
	}
}

func (f *Finder) scanStart() error {
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

	for _, loc := range locs {
		f.mu.Lock()
		_, found := f.stores[loc.UUID]
		f.mu.Unlock()

		if !found {
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
		if err != nil {
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

		err = f.markActive(url, id)
		if err != nil {
			return err
		}

		f.mu.Lock()
		_, found := f.stores[id]
		f.mu.Unlock()
		if !found {
			s, err := store.NewClient(url + "/" + uuid.Fmt(id) + "/")
			if err != nil {
				return err
			}

			f.mu.Lock()
			f.stores[id] = s
			f.mu.Unlock()
		}
	}

	return nil
}

func (f *Finder) markActive(url string, id [16]byte) error {
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
		loc.Dead = false
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
	for id, store := range f.Stores() {
		_, err := store.FreeSpace()
		if err != nil {
			f.mu.Lock()
			delete(f.stores, id)
			f.mu.Unlock()
		}

		select {
		case <-f.stop:
			return
		case <-time.After(wait):
		}
	}
}
