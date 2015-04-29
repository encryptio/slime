package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

var (
	in            = bufio.NewReader(os.Stdin)
	out           = bufio.NewWriter(os.Stdout)
	baseURL       string
	oldPrefixMode bool
)

func getConfig(name string) string {
	out.WriteString("GETCONFIG ")
	out.WriteString(name)
	out.WriteString("\n")
	out.Flush()

	line, err := in.ReadString('\n')
	if err != nil {
		log.Fatalf("Couldn't get config variable %s: %v", name, err)
	}
	line = strings.TrimSuffix(line, "\n")

	if strings.HasPrefix(line, "VALUE ") {
		return strings.TrimPrefix(line, "VALUE ")
	}

	return ""
}

func keyURL(key string) string {
	if oldPrefixMode {
		sha := sha256.Sum256([]byte(strings.ToLower(key)))
		hexed := hex.EncodeToString(sha[0:3])
		return baseURL + hexed[0:3] + "/" + hexed[3:6] + "/" + key
	}

	return baseURL + key
}

func initRemote() {
	baseURL = getConfig("baseurl")
	if baseURL == "" {
		out.WriteString("INITREMOTE-FAILURE You must set baseurl to the URL (ending in /) that you want to use\n")
		return
	}

	old := getConfig("oldprefixmode")
	oldPrefixMode = old == "true"

	out.WriteString("INITREMOTE-SUCCESS\n")
}

func prepare() {
	baseURL = getConfig("baseurl")
	if baseURL == "" {
		out.WriteString("PREPARE-FAILURE You must set baseurl to the URL (ending in /) that you want to use\n")
		return
	}

	old := getConfig("oldprefixmode")
	oldPrefixMode = old == "true"

	out.WriteString("PREPARE-SUCCESS\n")
}

type positionPrinter struct {
	n         int64
	lastPrint int64
	rdr       io.Reader
}

func (p *positionPrinter) Read(buf []byte) (int, error) {
	n, err := p.rdr.Read(buf)
	p.n += int64(n)
	if p.n-p.lastPrint > 131072 {
		fmt.Fprintf(out, "PROGRESS %d\n", p.n)
		out.Flush()
		p.lastPrint = p.n
	}
	return n, err
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

	hash := sha256.New()
	_, err = io.Copy(hash, fh)
	if err != nil {
		log.Printf("Couldn't read from %s: %v", file, err)
		return
	}
	sha := hash.Sum(nil)

	length, err := fh.Seek(0, 2)
	if err != nil {
		log.Printf("Couldn't seek in %s: %v", file, err)
		return
	}

	_, err = fh.Seek(0, 0)
	if err != nil {
		log.Printf("Couldn't seek in %s: %v", file, err)
		return
	}

	req, err := http.NewRequest("HEAD", keyURL(key), nil)
	if err != nil {
		log.Printf("Couldn't create request for %s: %v",
			keyURL(key), err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Couldn't HEAD %v: %v", req.URL, err)
		return
	}
	resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if oldSHA := resp.Header.Get("X-Content-SHA256"); oldSHA != "" {
			if strings.ToLower(oldSHA) == hex.EncodeToString(sha) {
				ok = true
				return
			}
		}
	}

	req, err = http.NewRequest("PUT", keyURL(key),
		&positionPrinter{rdr: fh})
	if err != nil {
		log.Printf("Couldn't create request for %s: %v",
			keyURL(key), err)
		return
	}
	req.Header.Set("X-Content-SHA256", hex.EncodeToString(sha))
	req.Header.Set("Content-Length", strconv.FormatInt(length, 10))

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Couldn't PUT to %v: %v", req.URL, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
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

	resp, err := http.Get(keyURL(key))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("Couldn't GET %v: %v", resp.Request.URL, resp.Status)
		return
	}

	fh, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("Couldn't open %v for writing: %v", file, err)
		return
	}

	sha := sha256.New()
	reader := io.TeeReader(resp.Body, sha)

	total := int64(0)
	for {
		n, err := io.CopyN(fh, reader, 131072)
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

	if resp.Header.Get("X-Content-SHA256") != "" {
		have := sha.Sum(nil)
		want, _ := hex.DecodeString(resp.Header.Get("X-Content-SHA256"))
		if !bytes.Equal(have, want) {
			log.Printf("Bad checksum of response")
			return
		}
	}

	ok = true
}

func checkPresent(key string) {
	req, err := http.NewRequest("HEAD", keyURL(key), nil)
	if err != nil {
		log.Printf("Couldn't create request for %s: %v",
			keyURL(key), err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
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
	req, err := http.NewRequest("DELETE", keyURL(key), nil)
	if err != nil {
		log.Printf("Couldn't create request for %s: %v",
			keyURL(key), err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if (resp.StatusCode >= 200 && resp.StatusCode < 300) || resp.StatusCode == 404 {
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
