package store

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"git.encryptio.com/slime/lib/httputil"
)

// A Client is a Store which interfaces with the standard HTTP interface.
type Client struct {
	url    string
	uuid   [16]byte
	client *http.Client
}

// NewClient creates a Client. The URL passed should end with a trailing slash.
func NewClient(url string, uuid [16]byte) *Client {
	return &Client{
		url:  url,
		uuid: uuid,
		client: &http.Client{
			Timeout: time.Second * 15,
		},
	}
}

func (cc *Client) UUID() [16]byte {
	return cc.uuid
}

func (cc *Client) Get(key string) ([]byte, error) {
	resp, err := cc.startReq("GET", cc.url+url.QueryEscape(key), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, httputil.ReadResponseAsError(resp)
	}

	return ioutil.ReadAll(resp.Body)
}

func (cc *Client) Set(key string, data []byte) error {
	resp, err := cc.startReq("PUT", cc.url+url.QueryEscape(key),
		bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

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
		return ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httputil.ReadResponseAsError(resp)
	}

	return nil
}

func (cc *Client) List(after string, limit int) ([]string, error) {
	args := make(url.Values)
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
		if err != nil {
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
	resp, err := cc.startReq("GET", cc.url+"?free=1", nil)
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

func (cc *Client) startReq(method, url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	return cc.client.Do(req)
}
