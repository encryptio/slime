package storehttp

import (
	"net/http/httptest"
	"os"
	"testing"

	"git.encryptio.com/slime/lib/store/storedir"
	"git.encryptio.com/slime/lib/store/storetests"
	"git.encryptio.com/slime/lib/uuid"
)

func TestHTTPCommon(t *testing.T) {
	ds, tmpDir := storedir.MakeTestingDirectory(t)
	defer os.RemoveAll(tmpDir)

	srv := httptest.NewServer(NewServer(ds))
	defer srv.Close()

	client, err := NewClient(srv.URL + "/")
	if err != nil {
		t.Fatalf("Couldn't initialize client: %v", err)
	}

	storetests.TestStore(t, client)

	if client.UUID() != ds.UUID() {
		t.Errorf("client UUID %v does not match directory UUID %v",
			uuid.Fmt(client.UUID()), uuid.Fmt(ds.UUID()))
	}
}
