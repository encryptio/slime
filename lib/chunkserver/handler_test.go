package chunkserver

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"git.encryptio.com/slime/lib/store"
)

func shouldRespond(t *testing.T, handler http.Handler, method, url string, requestBody string, code int, responseBody string) {
	var requestBodyReader io.Reader
	if requestBody != "" {
		requestBodyReader = bytes.NewBufferString(requestBody)
	}

	r, err := http.NewRequest(method, url, requestBodyReader)
	if err != nil {
		t.Fatalf("NewRequest(%v, %v, %v) returned %v",
			method, url, requestBody, err)
	}

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if code != w.Code {
		t.Errorf("%v %v returned code %v, wanted %v",
			method, url, w.Code, code)
	}

	if responseBody != w.Body.String() {
		t.Errorf("%v %v responded with %#v, wanted %#v",
			method, url, w.Body.String(), responseBody)
	}
}

func shouldRespondInteger(t *testing.T, handler http.Handler, method, url string, code int) {
	r, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("NewRequest(%v, %v, %v) returned %v", method, url, nil, err)
	}

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if code != 200 {
		t.Errorf("%v %v returned code %v, wanted 200", method, url, w.Code)
	}

	_, err = strconv.ParseInt(w.Body.String(), 10, 64)
	if err != nil {
		t.Errorf("%v %v responded with a non-integer response %#v",
			method, url, w.Body.String())
	}
}

func TestHandlerBasics(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("Couldn't create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpdir)

	err = store.CreateDirectory(tmpdir)
	if err != nil {
		t.Fatalf("Couldn't initialize directory store in %v: %v", tmpdir, err)
	}

	uuidBytes, err := ioutil.ReadFile(filepath.Join(tmpdir, "uuid"))
	if err != nil {
		t.Fatalf("Couldn't read uuid file: %v", err)
	}
	uuid := string(uuidBytes)

	h, err := New([]string{tmpdir}, 0, 0)
	if err != nil {
		t.Fatalf("Couldn't create Handler: %v", err)
	}
	defer h.Stop()

	h.WaitScanDone()

	shouldRespond(t, h, "GET", "/", "",
		200, "Howdy, slime chunk server here!\n")

	shouldRespond(t, h, "GET", "/uuids", "", 200, uuid+"\n")
	shouldRespond(t, h, "GET", "/"+uuid+"/?mode=list", "", 200, "")

	shouldRespond(t, h, "PUT", "/"+uuid+"/file", "contents", 204, "")
	shouldRespond(t, h, "GET", "/"+uuid+"/file", "", 200, "contents")
	shouldRespond(t, h, "GET", "/"+uuid+"/?mode=list", "", 200, "file\n")

	shouldRespond(t, h, "DELETE", "/"+uuid+"/file", "", 204, "")
	shouldRespond(t, h, "GET", "/"+uuid+"/file", "", 404, "not found\n")
	shouldRespond(t, h, "DELETE", "/"+uuid+"/file", "", 404, "not found\n")
	shouldRespond(t, h, "GET", "/"+uuid+"/?mode=list", "", 200, "")

	shouldRespond(t, h, "PUT", "/"+uuid+"/z", "z", 204, "")
	shouldRespond(t, h, "PUT", "/"+uuid+"/y", "y", 204, "")
	shouldRespond(t, h, "PUT", "/"+uuid+"/a", "a", 204, "")
	shouldRespond(t, h, "PUT", "/"+uuid+"/c", "c", 204, "")
	shouldRespond(t, h, "PUT", "/"+uuid+"/x", "x", 204, "")
	shouldRespond(t, h, "PUT", "/"+uuid+"/b", "b", 204, "")
	shouldRespond(t, h, "GET", "/"+uuid+"/?mode=list", "",
		200, "a\nb\nc\nx\ny\nz\n")
	shouldRespond(t, h, "GET", "/"+uuid+"/?mode=list&limit=3", "",
		200, "a\nb\nc\n")
	shouldRespond(t, h, "GET", "/"+uuid+"/?mode=list&limit=2&after=b", "",
		200, "c\nx\n")
	shouldRespond(t, h, "GET", "/"+uuid+"/?mode=list&limit=3&after=y", "",
		200, "z\n")

	shouldRespondInteger(t, h, "GET", "/"+uuid+"/?mode=free", 200)
}

func TestHandlerMultipleStopsDontPanic(t *testing.T) {
	h, err := New(nil, 0, 0)
	if err != nil {
		t.Fatalf("Couldn't create Handler: %v", err)
	}
	h.Stop()
	h.Stop()
}

func TestHandlerErrors(t *testing.T) {
	h, err := New(nil, 0, 0)
	if err != nil {
		t.Fatalf("Couldn't create Handler: %v", err)
	}
	defer h.Stop()

	shouldRespond(t, h, "GET", "/nonexistent", "", 400, "bad url\n")
	shouldRespond(t, h, "GET", "/baduuid/", "", 400, "bad uuid\n")
	shouldRespond(t, h, "GET", "/39447fcd-0f36-4e6a-8400-66111f4275b3/", "",
		404, "no such uuid\n")
}
