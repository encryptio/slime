package storehttp

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"git.encryptio.com/slime/lib/store/storetests"
	"git.encryptio.com/slime/lib/uuid"
)

func TestHTTPCommon(t *testing.T) {
	mock := storetests.NewMockStore(0)

	srv := httptest.NewServer(NewServer(mock))
	defer srv.Close()

	client, err := NewClient(srv.URL + "/")
	if err != nil {
		t.Fatalf("Couldn't initialize client: %v", err)
	}

	storetests.TestStore(t, client)

	if client.UUID() != mock.UUID() {
		t.Errorf("client UUID %v does not match directory UUID %v",
			uuid.Fmt(client.UUID()), uuid.Fmt(mock.UUID()))
	}
}

func TestHTTPTooBig(t *testing.T) {
	mock := storetests.NewMockStore(0)

	handler := NewServer(mock)

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
