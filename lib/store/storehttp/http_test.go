package storehttp

import (
	"bytes"
	"net/http"
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

func TestHTTPTooBig(t *testing.T) {
	ds, tmpDir := storedir.MakeTestingDirectory(t)
	defer os.RemoveAll(tmpDir)

	handler := NewServer(ds)

	data := make([]byte, MaxFileSize+1000)

	w := httptest.NewRecorder()
	req, err := http.NewRequest("PUT", "/thing", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Couldn't create request: %v", err)
	}

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("Wanted response code %v, but got %v",
			http.StatusRequestEntityTooLarge, w.Code)
	}
}
