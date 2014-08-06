package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

var (
	in      = bufio.NewReader(os.Stdin)
	out     = bufio.NewWriter(os.Stdout)
	baseURL string
)

func addPrefix(key string) string {
	sha := sha256.Sum256([]byte(strings.ToLower(key)))
	hexed := hex.EncodeToString(sha[0:3])
	return hexed[0:3] + "/" + hexed[3:6] + "/" + key
}

func getConfig(name string) string {
	out.WriteString("GETCONFIG ")
	out.WriteString(name)
	out.WriteString("\n")
	out.Flush()

	line, err := in.ReadString('\n')
	if err != nil {
		log.Fatal("Couldn't get config variable %s: %s", name, err)
	}
	line = strings.TrimSuffix(line, "\n")

	if strings.HasPrefix(line, "VALUE ") {
		return strings.TrimPrefix(line, "VALUE ")
	} else {
		return ""
	}
}

func initRemote() {
	baseURL = getConfig("baseurl")
	if baseURL == "" {
		out.WriteString("INITREMOTE-FAILURE You must set baseurl to the slimed URL and path you want to use\n")
		return
	}

	out.WriteString("INITREMOTE-SUCCESS\n")
}

func prepare() {
	baseURL = getConfig("baseurl")
	if baseURL == "" {
		out.WriteString("PREPARE-FAILURE You must set baseurl to the slimed URL and path you want to use\n")
		return
	}

	out.WriteString("PREPARE-SUCCESS\n")
}

func store(key, file string) {
	ok := false
	defer func() {
		if ok {
			out.WriteString("TRANSFER-SUCCESS STORE ")
		} else {
			out.WriteString("TRANSFER-FAILURE STORE ")
		}
		out.WriteString(key)
		out.WriteString("\n")
	}()

	fh, err := os.Open(file)
	if err != nil {
		log.Printf("Couldn't open %v for reading: %v", file, err)
		return
	}
	defer fh.Close()

	req, err := http.NewRequest("PUT", baseURL+addPrefix(key), fh)
	if err != nil {
		log.Printf("Couldn't create request for %s: %v", baseURL+addPrefix(key), err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Couldn't PUT to %v: %v", req.URL, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("Couldn't PUT to %v: %v", req.URL, resp.Status)
		return
	}

	ok = true
}

func retrieve(key, file string) {
	ok := false
	defer func() {
		if ok {
			out.WriteString("TRANSFER-SUCCESS RETRIEVE ")
		} else {
			out.WriteString("TRANSFER-FAILURE RETRIEVE ")
		}
		out.WriteString(key)
		out.WriteString("\n")
	}()

	resp, err := http.Get(baseURL + addPrefix(key))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("Couldn't GET %v: %v", resp.Request.URL, resp.Status)

	}

	fh, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("Couldn't open %v for writing: %v", file, err)
		return
	}

	total := int64(0)
	for {
		n, err := io.CopyN(fh, resp.Body, 131072)
		total += n
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("Couldn't copy data from response: %v", err)
			fh.Close()
			return
		}

		fmt.Fprintf(out, "PROGRESS %d\n", total)
		out.Flush()
	}

	err = fh.Close()
	if err != nil {
		log.Printf("Couldn't close %v after writing: %v", file, err)
		return
	}

	ok = true
}

func checkPresent(key string) {
	resp, err := http.Get(baseURL + addPrefix(key))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		out.WriteString("CHECKPRESENT-SUCCESS ")
	} else if resp.StatusCode == 404 {
		out.WriteString("CHECKPRESENT-FAILURE ")
	} else {
		out.WriteString("CHECKPRESENT-UNKNOWN ")
	}
	out.WriteString(key)
	out.WriteString("\n")
}

func remove(key string) {
	req, err := http.NewRequest("DELETE", baseURL+addPrefix(key), nil)
	if err != nil {
		log.Printf("Couldn't create request for %s: %v", baseURL+addPrefix(key), err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 || resp.StatusCode == 404 {
		out.WriteString("REMOVE-SUCCESS ")
	} else {
		out.WriteString("REMOVE-FAILURE ")
	}
	out.WriteString(key)
	out.WriteString("\n")
}

func main() {
	out.Write([]byte("VERSION 1\n"))

	for {
		out.Flush()

		line, err := in.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			log.Fatal(err)
		}
		line = strings.TrimSuffix(line, "\n")

		switch {
		case strings.HasPrefix(line, "INITREMOTE"):
			initRemote()

		case strings.HasPrefix(line, "PREPARE"):
			prepare()

		case strings.HasPrefix(line, "TRANSFER "):
			line = strings.TrimPrefix(line, "TRANSFER ")

			fields := strings.SplitN(line, " ", 3)
			for len(fields) < 3 {
				fields = append(fields, "")
			}

			switch fields[0] {
			case "STORE":
				store(fields[1], fields[2])
			case "RETRIEVE":
				retrieve(fields[1], fields[2])
			default:
				out.WriteString("UNSUPPORTED-REQUEST\n")
			}

		case strings.HasPrefix(line, "CHECKPRESENT "):
			key := strings.TrimPrefix(line, "CHECKPRESENT ")
			checkPresent(key)

		case strings.HasPrefix(line, "REMOVE "):
			key := strings.TrimPrefix(line, "REMOVE ")
			remove(key)

		default:
			out.WriteString("UNSUPPORTED-REQUEST\n")
		}
	}
}
