package main

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/encryptio/slime/internal/uuid"
)

func handleStore(args []string) error {
	if len(args) == 0 {
		return errors.New("store subcommand requires another subcommand")
	}

	switch args[0] {
	case "list":
		if len(args) != 1 {
			return errors.New("store list does not take any arguments")
		}
		return handleStoreList()
	case "dead", "undead", "delete":
		if len(args) == 1 {
			return fmt.Errorf("store %v requires a storeid argument", args[0])
		}
		if len(args) > 2 {
			return fmt.Errorf("too many arguments to store %v", args[0])
		}
		return handleStoreStoreOperation(args[0], args[1])
	case "scan":
		if len(args) == 1 {
			return fmt.Errorf("store scan requires a url argument")
		}
		if len(args) > 2 {
			return fmt.Errorf("too many arguments to store scan")
		}
		return handleStoreScan(args[1])
	case "rescan":
		if len(args) != 1 {
			return errors.New("store rescan does not take any arguments")
		}
		return handleStoreRescan()
	default:
		return fmt.Errorf("unknown store subcommand %v", args[0])
	}
}

type storeResponse struct {
	UUID      string    `json:"uuid"`
	URL       string    `json:"url"`
	Name      string    `json:"name"`
	Dead      bool      `json:"dead"`
	Connected bool      `json:"connected"`
	LastSeen  time.Time `json:"last_seen"`
	Free      int64     `json:"free,omitempty"`
	Error     string    `json:"error,omitempty"`
}

type storeResponseByName []storeResponse

func (l storeResponseByName) Len() int           { return len(l) }
func (l storeResponseByName) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l storeResponseByName) Less(i, j int) bool { return l[i].Name < l[j].Name }

func resolveStoreUUID(query string) (string, error) {
	query = strings.ToLower(query)

	var list []storeResponse
	err := jsonGet(conf.Base+"stores", &list)
	if err != nil {
		return "", err
	}

	var candidates []storeResponse
	for _, res := range list {
		if strings.Contains(strings.ToLower(res.Name), query) ||
			strings.Contains(strings.ToLower(res.UUID), query) {

			candidates = append(candidates, res)
		}
	}

	if len(candidates) > 1 {
		return "", fmt.Errorf("store id is ambiguous (matches %v stores)",
			len(candidates))
	}

	if len(candidates) == 0 {
		return "", errors.New("no stores match query")
	}

	return candidates[0].UUID, nil
}

func handleStoreList() error {
	var list []storeResponse
	err := jsonGet(conf.Base+"stores", &list)
	if err != nil {
		return err
	}

	sort.Sort(storeResponseByName(list))

	table := [][]string{
		[]string{"Name", "UUID", "Status", "Free"},
	}

	for _, st := range list {
		var status string
		if st.Connected {
			status = "connected"
		} else {
			status = "disconnected"
		}
		if st.Dead {
			status = fmt.Sprintf("dead (%v)", status)
		}

		var free string
		if st.Free > 0 {
			free = fmt.Sprintf("%.1f GiB", float64(st.Free)/1024/1024/1024)
		}

		table = append(table, []string{st.Name, st.UUID, status, free})
	}

	widthLimit := 0
	if !conf.Wide {
		widthLimit = getTTYWidth()
	}
	printTable(os.Stdout, table, widthLimit)
	return nil
}

func handleStoreStoreOperation(operation string, target string) error {
	_, err := uuid.Parse(target)
	if err != nil {
		target, err = resolveStoreUUID(target)
		if err != nil {
			return err
		}
	}

	var list []storeResponse
	return jsonPost(conf.Base+"stores", map[string]string{
		"operation": operation,
		"uuid":      target,
	}, &list)
}

func handleStoreScan(url string) error {
	var list []storeResponse
	return jsonPost(conf.Base+"stores", map[string]string{
		"operation": "scan",
		"url":       url,
	}, &list)
}

func handleStoreRescan() error {
	var list []storeResponse
	return jsonPost(conf.Base+"stores", map[string]string{
		"operation": "rescan",
	}, &list)
}
