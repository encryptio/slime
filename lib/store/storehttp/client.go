package storehttp

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"git.encryptio.com/slime/lib/httputil"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/uuid"
)

// Client is a Store which interfaces with the standard HTTP interface.
type Client struct {
	url    string
	uuid   [16]byte
	name   string
	client *http.Client
}

// NewClient creates a Client. The URL passed should end with a trailing slash.
func NewClient(url string) (*Client, error) {
	c := &Client{
		url: url,
		client: &http.Client{
			Timeout: time.Second * 15,
		},
	}

	err := c.loadStatics()
	if err != nil {
		return nil, fmt.Errorf("Couldn't load statics from store at %v: %v",
			url, err)
	}

	return c, nil
}

// Close closes the Client and any HTTP connections it has to the server.
func (cc *Client) Close() error {
	var rt http.RoundTripper
	if cc.client.Transport == nil {
		rt = http.DefaultTransport
	} else {
		rt = cc.client.Transport
	}

	if transport, ok := rt.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}

	return nil
}

func (cc *Client) UUID() [16]byte {
	return cc.uuid
}

func (cc *Client) Name() string {
	return cc.name
}

func (cc *Client) Get(key string, cancel <-chan struct{}) ([]byte, [32]byte, error) {
	var h [32]byte

	resp, err := cc.startReq("GET", cc.url+url.QueryEscape(key), nil, cancel)
	if err != nil {
		return nil, h, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, h, store.ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, h, httputil.ReadResponseAsError(resp)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, h, err
	}

	h = sha256.Sum256(data)

	if should := resp.Header.Get("x-content-sha256"); should != "" {
		shouldBytes, err := hex.DecodeString(should)
		if err != nil || len(shouldBytes) != 32 {
			return nil, h, ErrUnparsableSHAResponse
		}

		var shouldH [32]byte
		copy(shouldH[:], shouldBytes)

		if h != shouldH {
			return nil, h, HashMismatchError{Got: h, Want: shouldH}
		}
	}

	return data, h, nil
}

func (cc *Client) CAS(key string, from, to store.CASV, cancel <-chan struct{}) error {
	var req *http.Request
	var err error

	if to.Present {
		req, err = http.NewRequest("PUT", cc.url+url.QueryEscape(key),
			bytes.NewBuffer(to.Data))
		if err != nil {
			return err
		}
		req.Header.Set("x-content-sha256", hex.EncodeToString(to.SHA256[:]))
	} else {
		req, err = http.NewRequest("DELETE", cc.url+url.QueryEscape(key), nil)
		if err != nil {
			return err
		}
	}

	if !from.Any {
		if from.Present {
			req.Header.Set("If-Match",
				`"`+hex.EncodeToString(from.SHA256[:])+`"`)
		} else {
			req.Header.Set("If-Match", `"nonexistent"`)
		}
	}

	resp, err := cc.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	if resp.StatusCode == http.StatusPreconditionFailed {
		return store.ErrCASFailure
	}

	if resp.StatusCode == http.StatusNotFound {
		if from.Any || !from.Present {
			return nil
		}
		return store.ErrCASFailure
	}

	return httputil.ReadResponseAsError(resp)
}

func (cc *Client) List(after string, limit int, cancel <-chan struct{}) ([]string, error) {
	args := make(url.Values)
	args.Add("mode", "list")
	if after != "" {
		args.Add("after", after)
	}
	if limit > 0 {
		args.Add("limit", strconv.FormatInt(int64(limit), 10))
	}

	resp, err := cc.startReq("GET", cc.url+"?"+args.Encode(), nil, cancel)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, httputil.ReadResponseAsError(resp)
	}

	r := bufio.NewReader(resp.Body)

	size := limit
	if size <= 0 {
		size = 50
	}
	strs := make([]string, 0, size)

	for {
		line, err := r.ReadString('\n')
		if line == "" && err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		strs = append(strs, strings.TrimSuffix(line, "\n"))
	}

	return strs, nil
}

func (cc *Client) FreeSpace(cancel <-chan struct{}) (int64, error) {
	resp, err := cc.startReq("GET", cc.url+"?mode=free", nil, cancel)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, httputil.ReadResponseAsError(resp)
	}

	data, err := ioutil.ReadAll(io.LimitReader(resp.Body, 32))
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(string(data), 10, 64)
}

func (cc *Client) Stat(key string, cancel <-chan struct{}) (*store.Stat, error) {
	resp, err := cc.startReq("HEAD", cc.url+url.QueryEscape(key), nil, cancel)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, store.ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, httputil.ReadResponseAsError(resp)
	}

	st := &store.Stat{}

	if sha := resp.Header.Get("x-content-sha256"); sha != "" {
		shaBytes, err := hex.DecodeString(sha)
		if err != nil || len(shaBytes) != 32 {
			return nil, ErrUnparsableSHAResponse
		}
		copy(st.SHA256[:], shaBytes)
	}

	st.Size = resp.ContentLength

	return st, nil
}

func (cc *Client) loadStatics() error {
	err := cc.loadUUID()
	if err != nil {
		return err
	}

	err = cc.loadName()
	if err != nil {
		return err
	}

	return nil
}

func (cc *Client) loadUUID() error {
	resp, err := cc.startReq("GET", cc.url+"?mode=uuid", nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httputil.ReadResponseAsError(resp)
	}

	// 37 is the length of a formatted UUID plus one (to avoid false positives)
	data, err := ioutil.ReadAll(io.LimitReader(resp.Body, 37))
	if err != nil {
		return err
	}

	cc.uuid, err = uuid.Parse(string(data))
	if err != nil {
		return err
	}

	return nil
}

func (cc *Client) loadName() error {
	resp, err := cc.startReq("GET", cc.url+"?mode=name", nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httputil.ReadResponseAsError(resp)
	}

	data, err := ioutil.ReadAll(io.LimitReader(resp.Body, 1000))
	if err != nil {
		return err
	}

	cc.name = string(data)

	return nil
}

func (cc *Client) startReq(method, url string, body io.Reader, cancel <-chan struct{}) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-cancel:
			var rt http.RoundTripper
			if cc.client.Transport == nil {
				rt = http.DefaultTransport
			} else {
				rt = cc.client.Transport
			}
			if tr, ok := rt.(interface {
				CancelRequest(*http.Request)
			}); ok {
				tr.CancelRequest(req)
			}
		}
	}()

	resp, err := cc.client.Do(req)
	close(done)

	select {
	case <-cancel:
		return nil, store.ErrCancelled
	default:
		return resp, err
	}
}
