package store

import (
	"net/http/httptest"
	"os"
	"testing"

	"git.encryptio.com/slime/lib/uuid"
)

func TestHTTPBasics(t *testing.T) {
	ds, tmpDir := makeDirectory(t)
	defer os.RemoveAll(tmpDir)

	srv := httptest.NewServer(NewServer(ds))
	defer srv.Close()

	client, err := NewClient(srv.URL + "/")
	if err != nil {
		t.Fatalf("Couldn't initialize client: %v", err)
	}

	testStoreBasics(t, client)

	if client.UUID() != ds.UUID() {
		t.Errorf("client UUID %v does not match directory UUID %v",
			uuid.Fmt(client.UUID()), uuid.Fmt(ds.UUID()))
	}
}
