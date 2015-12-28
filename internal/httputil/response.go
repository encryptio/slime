package httputil

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

type ResponseError struct {
	Status string
	Body   string
}

func (e ResponseError) Error() string {
	return fmt.Sprintf("Couldn't request from remote server: %v %#v",
		e.Status, e.Body)
}

func ReadResponseAsError(resp *http.Response) error {
	data, err := ioutil.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return err
	}

	return ResponseError{resp.Status, string(data)}
}
