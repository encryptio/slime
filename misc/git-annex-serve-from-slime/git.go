package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"
)

func gitAnnexUUID(remote string) (string, error) {
	var ret struct {
		UUID string `json:"uuid"`
	}

	data, err := run("git", "annex", "info", "--fast", "--json", remote)
	if err != nil {
		return "", err
	}

	err = json.Unmarshal(data, &ret)
	if err != nil {
		return "", err
	}

	if ret.UUID == "" {
		return "", errors.New("no uuid in output of git annex info")
	}

	return ret.UUID, nil
}

func parseRemoteLogLine(line string) map[string]string {
	line = strings.TrimSuffix(line, "\n")
	ret := make(map[string]string, 10)
	for _, part := range strings.Split(line, " ") {
		if len(ret) == 0 {
			ret["uuid"] = part
			continue
		}

		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			ret[kv[0]] = kv[1]
		}
	}
	return ret
}

func getSlimeBaseURL(uuid string) (string, error) {
	data, err := run("git", "ls-tree", "git-annex", "remote.log")
	if err != nil {
		return "", err
	}

	// like "100644 blob 36cb657065d4bca8a9b958562c9c6f8dc65cd5ce\tremote.log\n"
	re := regexp.MustCompile("[0-9]+ [a-z]+ ([0-9a-f]+)\t.*")
	match := re.FindStringSubmatch(string(data))
	if len(match) != 2 {
		return "", errors.New("invalid response from git ls-tree")
	}

	objectHash := match[1]

	data, err = run("git", "show", "--raw", objectHash)
	if err != nil {
		return "", err
	}

	var mostRecent map[string]string
	var mostRecentTime float64
	r := bufio.NewReader(bytes.NewReader(data))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}

		m := parseRemoteLogLine(line)

		if m["uuid"] != uuid {
			continue
		}

		ts := strings.TrimSuffix(m["timestamp"], "s")
		t, err := strconv.ParseFloat(ts, 64)
		if err != nil {
			continue
		}

		if t > mostRecentTime {
			mostRecent = m
			mostRecentTime = t
		}
	}

	if mostRecent == nil {
		return "", errors.New("no entry for uuid in remote.log")
	}

	if mostRecent["type"] != "external" {
		return "", errors.New("not an external remote")
	}

	if mostRecent["externaltype"] != "slime" {
		return "", errors.New("not a slime external remote")
	}

	if mostRecent["encryption"] != "none" {
		return "", errors.New(
			"unsupported encryption type " + mostRecent["encryption"])
	}

	url, ok := mostRecent["baseurl"]
	if !ok {
		return "", errors.New("no base url in remote.log")
	}

	return url, nil
}
