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

// A Client is a Store which interfaces with the standard HTTP interface.
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

func (cc *Client) UUID() [16]byte {
	return cc.uuid
}

func (cc *Client) Name() string {
	return cc.name
}

func (cc *Client) Get(key string) ([]byte, error) {
	d, _, err := cc.GetWith256(key)
	return d, err
}

func (cc *Client) GetWith256(key string) ([]byte, [32]byte, error) {
	var h [32]byte

	resp, err := cc.startReq("GET", cc.url+url.QueryEscape(key), nil)
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

func (cc *Client) Set(key string, data []byte) error {
	return cc.SetWith256(key, data, sha256.Sum256(data))
}

func (cc *Client) SetWith256(key string, data []byte, h [32]byte) error {
	req, err := http.NewRequest("PUT", cc.url+url.QueryEscape(key),
		bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("x-content-sha256", hex.EncodeToString(h[:]))

	resp, err := cc.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httputil.ReadResponseAsError(resp)
	}

	return nil
}

func (cc *Client) CASWith256(key string, oldH [32]byte, data []byte, newH [32]byte) error {
	req, err := http.NewRequest("PUT", cc.url+url.QueryEscape(key),
		bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("x-content-sha256", hex.EncodeToString(newH[:]))
	req.Header.Set("if-match", `"`+hex.EncodeToString(oldH[:])+`"`)

	resp, err := cc.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return store.ErrCASFailure
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httputil.ReadResponseAsError(resp)
	}

	return nil
}

func (cc *Client) Delete(key string) error {
	resp, err := cc.startReq("DELETE", cc.url+url.QueryEscape(key), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return store.ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httputil.ReadResponseAsError(resp)
	}

	return nil
}

func (cc *Client) List(after string, limit int) ([]string, error) {
	args := make(url.Values)
	args.Add("mode", "list")
	if after != "" {
		args.Add("after", after)
	}
	if limit > 0 {
		args.Add("limit", strconv.FormatInt(int64(limit), 10))
	}

	resp, err := cc.startReq("GET", cc.url+"?"+args.Encode(), nil)
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

func (cc *Client) FreeSpace() (int64, error) {
	resp, err := cc.startReq("GET", cc.url+"?mode=free", nil)
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

func (cc *Client) Stat(key string) (*store.Stat, error) {
	resp, err := cc.startReq("HEAD", cc.url+url.QueryEscape(key), nil)
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
	resp, err := cc.startReq("GET", cc.url+"?mode=uuid", nil)
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
	resp, err := cc.startReq("GET", cc.url+"?mode=name", nil)
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

func (cc *Client) startReq(method, url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	return cc.client.Do(req)
}
