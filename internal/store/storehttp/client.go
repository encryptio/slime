package storehttp

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/encryptio/slime/internal/httputil"
	"github.com/encryptio/slime/internal/store"
	"github.com/encryptio/slime/internal/uuid"
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

func (cc *Client) Get(key string, opts store.GetOptions) ([]byte, store.Stat, error) {
	var headers http.Header
	if opts.NoVerify {
		headers = make(http.Header, 1)
		headers.Set("X-Slime-Noverify", "true")
	}

	resp, err := cc.startReq("GET", cc.url+url.QueryEscape(key), nil, headers, opts.Cancel)
	if err != nil {
		return nil, store.Stat{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, store.Stat{}, store.ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, store.Stat{}, httputil.ReadResponseAsError(resp)
	}

	var writeTime int64
	if timeStr := resp.Header.Get("Last-Modified"); timeStr != "" {
		t, err := time.Parse(http.TimeFormat, timeStr)
		if err == nil {
			writeTime = t.Unix()
		}
	}

	var shouldH [32]byte
	var shouldHSet bool
	if should := resp.Header.Get("X-Content-Sha256"); should != "" {
		shouldBytes, err := hex.DecodeString(should)
		if err != nil || len(shouldBytes) != 32 {
			return nil, store.Stat{}, ErrUnparsableSHAResponse
		}

		copy(shouldH[:], shouldBytes)
		shouldHSet = true
	}

	var rdr io.Reader
	var hasher hash.Hash
	if opts.NoVerify && shouldHSet {
		rdr = resp.Body
	} else {
		hasher = sha256.New()
		rdr = io.TeeReader(resp.Body, hasher)
	}

	data, err := ioutil.ReadAll(rdr)
	if err != nil {
		return nil, store.Stat{}, err
	}

	var h [32]byte
	if opts.NoVerify && shouldHSet {
		h = shouldH
	} else {
		copy(h[:], hasher.Sum(nil))

		if h != shouldH {
			return nil, store.Stat{}, HashMismatchError{Got: h, Want: shouldH}
		}
	}

	return data, store.Stat{
		SHA256:    h,
		Size:      int64(len(data)),
		WriteTime: writeTime,
	}, nil
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

	resp, err := cc.startReq("GET", cc.url+"?"+args.Encode(), nil, nil, cancel)
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
	resp, err := cc.startReq("GET", cc.url+"?mode=free", nil, nil, cancel)
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

func (cc *Client) Stat(key string, cancel <-chan struct{}) (store.Stat, error) {
	resp, err := cc.startReq("HEAD", cc.url+url.QueryEscape(key), nil, nil, cancel)
	if err != nil {
		return store.Stat{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return store.Stat{}, store.ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return store.Stat{}, httputil.ReadResponseAsError(resp)
	}

	st := store.Stat{}

	if sha := resp.Header.Get("X-Content-Sha256"); sha != "" {
		shaBytes, err := hex.DecodeString(sha)
		if err != nil || len(shaBytes) != 32 {
			return store.Stat{}, ErrUnparsableSHAResponse
		}
		copy(st.SHA256[:], shaBytes)
	}

	if timeStr := resp.Header.Get("Last-Modified"); timeStr != "" {
		t, err := time.Parse(http.TimeFormat, timeStr)
		if err == nil {
			st.WriteTime = t.Unix()
		}
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
	resp, err := cc.startReq("GET", cc.url+"?mode=uuid", nil, nil, nil)
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
	resp, err := cc.startReq("GET", cc.url+"?mode=name", nil, nil, nil)
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

func (cc *Client) startReq(
	method, url string,
	body io.Reader,
	headers http.Header,
	cancel <-chan struct{}) (*http.Response, error) {

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	if headers != nil {
		for k, v := range headers {
			req.Header[k] = v
		}
	}

	req.Cancel = cancel

	resp, err := cc.client.Do(req)

	select {
	case <-cancel:
		return nil, store.ErrCancelled
	default:
		return resp, err
	}
}
