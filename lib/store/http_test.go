package store

import (
	"net/http/httptest"
	"os"
	"testing"
)

func TestHTTPBasics(t *testing.T) {
	ds, tmpDir := makeDirStore(t)
	defer os.RemoveAll(tmpDir)

	srv := httptest.NewServer(NewServer(ds))
	defer srv.Close()

	client := NewClient(srv.URL+"/", ds.UUID())

	testStoreBasics(t, client)
}
