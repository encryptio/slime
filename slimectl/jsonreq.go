package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

func jsonRequest(req *http.Request, responseInto interface{}) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// try to decode an "error" json object
		var errObj struct {
			Error string `json:"error"`
		}
		err = json.NewDecoder(resp.Body).Decode(&errObj)
		if err == nil {
			return fmt.Errorf("got error %#v from %v %v",
				errObj.Error, req.Method, req.URL)
		}

		return fmt.Errorf("got response code %v from %v %v",
			resp.StatusCode, req.Method, req.URL)
	}

	return json.NewDecoder(resp.Body).Decode(responseInto)
}

func jsonGet(url string, responseInto interface{}) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	return jsonRequest(req, responseInto)
}

func jsonPost(url string, postBody interface{}, responseInto interface{}) error {
	body, err := json.Marshal(postBody)
	if err != nil {
		panic(err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("content-type", "application/json; charset=utf-8")

	return jsonRequest(req, responseInto)
}
