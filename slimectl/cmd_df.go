package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

func handleDF(args []string) error {
	if len(args) != 0 {
		return errors.New("df does not take arguments")
	}

	res, err := http.Get(conf.Base + "data/?mode=free")
	if err != nil {
		return err
	}

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	free, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return err
	}

	fmt.Printf("%.1f GiB unused space in cluster\n",
		float64(free)/(1024*1024*1024))

	return nil
}
