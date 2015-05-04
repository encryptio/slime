package main

import (
	"errors"
	"fmt"
	"strconv"
)

type redundancy struct {
	Need  int `json:"need"`
	Total int `json:"total"`
}

func handleRedundancy(args []string) error {
	if len(args) == 0 {
		return handleRedundancyGet()
	}

	switch args[0] {
	case "get":
		return handleRedundancyGet()

	case "set":
		if len(args) != 3 {
			return errors.New("redundancy set takes two arguments")
		}

		return handleRedundancySet(args[1], args[2])

	default:
		return fmt.Errorf("bad redundancy subcommand %v", args[0])
	}
}

func handleRedundancyGet() error {
	var r redundancy

	err := jsonGet(conf.BaseURL+"redundancy", &r)
	if err != nil {
		return err
	}

	fmt.Printf("Redundancy is set to need %v of %v\n", r.Need, r.Total)
	return nil
}

func handleRedundancySet(needStr, totalStr string) error {
	need, err := strconv.ParseInt(needStr, 10, 0)
	if err != nil {
		return fmt.Errorf(`bad format for "need": %v`, err)
	}

	total, err := strconv.ParseInt(totalStr, 10, 0)
	if err != nil {
		return fmt.Errorf(`bad format for "total": %v`, err)
	}

	var r redundancy
	err = jsonPost(conf.BaseURL+"redundancy", redundancy{
		Need:  int(need),
		Total: int(total),
	}, &r)
	if err != nil {
		return err
	}

	fmt.Printf("Redundancy sucessfully changed to %v of %v\n", r.Need, r.Total)

	return nil
}
