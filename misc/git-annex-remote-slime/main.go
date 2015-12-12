package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/encryptio/go-git-annex-external/external"
)

type SlimeExt struct {
	baseURL       string
	oldPrefixMode bool
}

func (s *SlimeExt) keyURL(key string) string {
	if s.oldPrefixMode {
		sha := sha256.Sum256([]byte(strings.ToLower(key)))
		hexed := hex.EncodeToString(sha[0:3])
		return s.baseURL + hexed[0:3] + "/" + hexed[3:6] + "/" + key
	}

	return s.baseURL + key
}

func (s *SlimeExt) configure(e *external.External) error {
	var err error
	s.baseURL, err = e.GetConfig("baseurl")
	if err != nil {
		return err
	}
	if s.baseURL == "" {
		return errors.New("You must set baseurl to the URL (ending in /) that you want to use\n")
	}

	old, err := e.GetConfig("oldprefixmode")
	if err != nil {
		return err
	}
	s.oldPrefixMode = old == "true"

	return nil
}

func (s *SlimeExt) InitRemote(e *external.External) error {
	return s.configure(e)
}

func (s *SlimeExt) Prepare(e *external.External) error {
	return s.configure(e)
}

func (s *SlimeExt) Store(e *external.External, key, file string) error {
	fh, err := os.Open(file)
	if err != nil {
		return err
	}
	defer fh.Close()

	shaDone := make(chan struct{})
	var sha []byte
	var length int64
	var shaError error
	go func() {
		defer close(shaDone)
		hash := sha256.New()
		length, shaError = io.Copy(hash, fh)
		if shaError != nil {
			return
		}

		sha = hash.Sum(nil)

		_, shaError = fh.Seek(0, 0)
	}()

	req, err := http.NewRequest("HEAD", s.keyURL(key), nil)
	if err != nil {
		return fmt.Errorf("Couldn't create request for %s: %v",
			s.keyURL(key), err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("Couldn't HEAD %v: %v", req.URL, err)
	}
	resp.Body.Close()

	<-shaDone

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if oldSHA := resp.Header.Get("X-Content-SHA256"); oldSHA != "" {
			if strings.ToLower(oldSHA) == hex.EncodeToString(sha) {
				// Key already exists with the correct data
				return nil
			}
		}
	}

	req, err = http.NewRequest("PUT", s.keyURL(key),
		external.NewProgressReader(fh, e))
	if err != nil {
		return fmt.Errorf("Couldn't create request for %s: %v",
			s.keyURL(key), err)
	}
	req.Header.Set("X-Content-SHA256", hex.EncodeToString(sha))
	req.Header.Set("Content-Length", strconv.FormatInt(length, 10))

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("Couldn't PUT to %v: %v", req.URL, err)
	}
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Couldn't PUT to %v: %v", req.URL, resp.Status)
	}

	return nil
}

func (s *SlimeExt) Retrieve(e *external.External, key, file string) error {
	resp, err := http.Get(s.keyURL(key))
	if err != nil {
		return fmt.Errorf("Couldn't GET %v: %v", s.keyURL(key), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Couldn't GET %v: %v", resp.Request.URL, resp.Status)
	}

	fh, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	sha := sha256.New()
	reader := io.TeeReader(external.NewProgressReader(resp.Body, e), sha)
	_, err = io.Copy(fh, reader)
	if err != nil {
		fh.Close()
		return err
	}

	err = fh.Close()
	if err != nil {
		return err
	}

	if resp.Header.Get("X-Content-SHA256") != "" {
		have := sha.Sum(nil)
		want, _ := hex.DecodeString(resp.Header.Get("X-Content-SHA256"))
		if !bytes.Equal(have, want) {
			return errors.New("bad checksum of response")
		}
	}

	return nil
}

func (s *SlimeExt) CheckPresent(e *external.External, key string) (bool, error) {
	req, err := http.NewRequest("HEAD", s.keyURL(key), nil)
	if err != nil {
		return false, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true, nil
	} else if resp.StatusCode == 404 {
		return false, nil
	}

	return false, fmt.Errorf("unexpected response code %v", resp.StatusCode)
}

func (s *SlimeExt) Remove(e *external.External, key string) error {
	req, err := http.NewRequest("DELETE", s.keyURL(key), nil)
	if err != nil {
		return fmt.Errorf("Couldn't create request for %s: %v",
			s.keyURL(key), err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("Couldn't DELETE %v: %v", req.URL, err)
	}
	defer resp.Body.Close()

	if (resp.StatusCode >= 200 && resp.StatusCode < 300) || resp.StatusCode == 404 {
		return nil
	}

	return fmt.Errorf("unexpected response code %v", resp.StatusCode)
}

func (s *SlimeExt) GetCost(e *external.External) (int, error) {
	return 0, external.ErrUnsupportedRequest
}

func (s *SlimeExt) GetAvailability(e *external.External) (external.Availability, error) {
	return external.AvailabilityGlobal, nil
}

func (s *SlimeExt) WhereIs(e *external.External, key string) (string, error) {
	return "", external.ErrUnsupportedRequest
}

func main() {
	err := external.RunLoop(os.Stdin, os.Stdout, &SlimeExt{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
