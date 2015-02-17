package multi

import (
	"bytes"
	"math/rand"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"git.encryptio.com/slime/lib/chunkserver"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/store/storedir"
	"git.encryptio.com/slime/lib/store/storetests"

	"git.encryptio.com/kvl/backend/ram"
)

func prepareMultiTest(t *testing.T, need, total, serverCount int) ([]*killHandler, *Multi, []*storedir.Directory, func()) {
	var killers []*killHandler
	var dirstores []*storedir.Directory

	var servers []*httptest.Server
	var chunkServers []*chunkserver.Handler
	var tmpPaths []string
	var finder *Finder
	var multi *Multi

	done := func() {
		if multi != nil {
			multi.Stop()
		}
		if finder != nil {
			finder.Stop()
		}
		for _, srv := range servers {
			srv.Close()
		}
		for _, cs := range chunkServers {
			cs.Stop()
		}
		for _, path := range tmpPaths {
			os.RemoveAll(path)
		}
	}

	for i := 0; i < serverCount; i++ {
		dirstore, tmpPath := storedir.MakeTestingDirectory(t)
		tmpPaths = append(tmpPaths, tmpPath)
		dirstores = append(dirstores, dirstore)

		cs, err := chunkserver.New([]string{tmpPath}, 0, 0)
		if err != nil {
			done()
			t.Fatalf("Couldn't create chunkserver: %v", err)
		}
		chunkServers = append(chunkServers, cs)

		killer := &killHandler{inner: cs}
		srv := httptest.NewServer(killer)

		killers = append(killers, killer)
		servers = append(servers, srv)
	}

	db := ram.New()

	finder, err := NewFinder(db)
	if err != nil {
		done()
		t.Fatalf("Couldn't create new finder: %v", err)
	}

	for _, srv := range servers {
		err = finder.Scan(srv.URL)
		if err != nil {
			done()
			t.Fatalf("Couldn't scan %v: %v", srv.URL, err)
		}
	}

	if len(finder.Stores()) != len(servers) {
		done()
		t.Fatalf("Finder did not find all stores")
	}

	multi, err = NewMulti(db, finder)
	if err != nil {
		done()
		t.Fatalf("Couldn't create multi: %v", err)
	}

	err = multi.SetRedundancy(need, total)
	if err != nil {
		done()
		t.Fatalf("Couldn't set redundancy levels: %v", err)
	}

	return killers, multi, dirstores, done
}

func TestMultiCommon(t *testing.T) {
	_, multi, _, done := prepareMultiTest(t, 3, 4, 5)
	defer done()
	storetests.TestStore(t, multi)
}

func TestMultiRecovery(t *testing.T) {
	for total := 4; total < 8; total++ {
		killers, multi, _, done := prepareMultiTest(t, 3, total, 10)
		defer done()

		for i := 0; i < 50; i++ {
			key := strconv.FormatInt(int64(i), 10)
			var value []byte
			for j := 0; j < i; j++ {
				value = append(value, []byte(key)...)
			}

			err := multi.CAS(key, store.MissingV, store.DataV(value))
			if err != nil {
				t.Fatalf("Couldn't write to multi: %v", err)
			}
		}

		for i := 0; i < total-3; i++ {
			for {
				k := killers[rand.Intn(len(killers))]
				if k.killed {
					continue
				}
				k.killed = true
				break
			}
		}

		for i := 0; i < 50; i++ {
			key := strconv.FormatInt(int64(i), 10)
			var value []byte
			for j := 0; j < i; j++ {
				value = append(value, []byte(key)...)
			}

			gotVal, _, err := multi.Get(key)
			if err != nil {
				t.Fatalf("Couldn't get %v from multi after failing underneath redundancy level: %v", key, err)
			}

			if !bytes.Equal(value, gotVal) {
				t.Fatalf("Value for %v is incorrect (got %#v, wanted %#v)", gotVal, value)
			}
		}
	}
}

func TestMultiScrub(t *testing.T) {
	_, multi, dirstores, done := prepareMultiTest(t, 3, 4, 4)
	defer done()

	data := "hello world! this is some test data."

	err := multi.CAS("key", store.MissingV, store.DataV([]byte(data)))
	if err != nil {
		t.Fatalf("Couldn't write to multi: %v", err)
	}

	names, err := dirstores[0].List("", 1)
	if err != nil {
		t.Fatalf("Couldn't list first dirstore: %v")
	}
	if len(names) != 1 {
		t.Fatalf("Didn't get a name from dirstore")
	}

	err = dirstores[0].CAS(names[0], store.AnyV, store.MissingV)
	if err != nil {
		t.Fatalf("Couldn't delete from dirstore: %v", err)
	}

	multi.scrubAll()

	found := 0
	for _, ds := range dirstores {
		names, err := ds.List("", 1)
		if err != nil {
			t.Fatalf("Couldn't list dirstore: %v", err)
		}
		if len(names) == 1 {
			found++
		}
	}

	if found != 4 {
		t.Fatalf("scrubAll didn't recreate missing chunk")
	}
}

func TestMultiDuplicates(t *testing.T) {
	_, multi, _, done := prepareMultiTest(t, 3, 4, 5)
	defer done()

	data := "this is some test data"

	for i := 0; i < 50; i++ {
		err := multi.CAS(strconv.FormatInt(int64(i), 10),
			store.MissingV, store.DataV([]byte(data)))
		if err != nil {
			t.Fatalf("Couldn't add key %v: %v", i, err)
		}
	}

	for i := 0; i < 50; i++ {
		got, _, err := multi.Get(strconv.FormatInt(int64(i), 10))
		if err != nil {
			t.Fatalf("Couldn't Get key %v: %v", i, err)
		}
		if string(got) != data {
			t.Fatalf("Got corrupt data on key %v: %v", i, got)
		}
	}

	for i := 0; i < 25; i++ {
		err := multi.CAS(strconv.FormatInt(int64(i), 10),
			store.AnyV, store.MissingV)
		if err != nil {
			t.Fatalf("Couldn't Delete %v: %v", i, err)
		}
	}

	for i := 25; i < 50; i++ {
		got, _, err := multi.Get(strconv.FormatInt(int64(i), 10))
		if err != nil {
			t.Fatalf("Couldn't Get key %v after removal of lower half: %v", i, err)
		}
		if string(got) != data {
			t.Fatalf("Got corrupt data on key %v after removal of lower half: %v", i, got)
		}
	}
}

func TestMultiScrubChangeRedundancy(t *testing.T) {
	killers, multi, _, done := prepareMultiTest(t, 2, 3, 5)
	defer done()

	data := "who knows where the wind goes"

	for i := 0; i < 10; i++ {
		err := multi.CAS(strconv.FormatInt(int64(i), 10),
			store.MissingV, store.DataV([]byte(data)))
		if err != nil {
			t.Fatalf("Couldn't add key %v: %v", i, err)
		}
	}

	err := multi.SetRedundancy(2, 5)
	if err != nil {
		t.Fatalf("Couldn't adjust redundancy: %v", err)
	}

	multi.scrubAll()

	killers[0].killed = true
	killers[1].killed = true
	killers[2].killed = true

	for i := 0; i < 10; i++ {
		got, _, err := multi.Get(strconv.FormatInt(int64(i), 10))
		if err != nil {
			t.Fatalf("Couldn't get key %v: %v", i, err)
		}
		if string(got) != data {
			t.Fatalf("Got corrupt data on key %v: %v", i, got)
		}
	}
}
