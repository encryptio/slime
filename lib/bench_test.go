package lib

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"git.encryptio.com/slime/lib/api"
	"git.encryptio.com/slime/lib/multi"
	"git.encryptio.com/slime/lib/store"
)

func makeTestBody() []byte {
	ret := make([]byte, 5000000)
	for i := 0; i < 500000; i++ {
		ret[i] = byte(i)
	}
	return ret
}

func mustNewRequest(method, url string, data []byte) *http.Request {
	buf := &bytes.Buffer{}
	buf.Write(data)

	req, err := http.NewRequest(method, url, buf)
	if err != nil {
		panic(err)
	}

	return req
}

func BenchmarkEndToEndWritesNoSHA(b *testing.B) {
	var targets []store.Target
	for i := 0; i < 10; i++ {
		targets = append(targets, store.NewRAM())
	}

	m, err := multi.New(targets)
	if err != nil {
		b.Fatal(err)
	}
	defer m.Stop()

	cfg := m.GetConfig()
	cfg.ChunksNeed = 6
	cfg.ChunksTotal = 9
	cfg.Version++
	err = m.SetConfig(cfg)
	if err != nil {
		b.Fatal(err)
	}

	h := api.NewHandler(m)

	data := makeTestBody()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		req := mustNewRequest("PUT", "/obj/thing", data)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 200 {
			b.Fatalf("Couldn't request, iteration %v: %v %v", i, w.Code, w.Body.String())
		}
	}
}
