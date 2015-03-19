package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type FileSystem struct {
	BaseURL string
	UUID    string
}

func (fs *FileSystem) Open(name string) (http.File, error) {
	fi, err := os.Lstat(*repo + "/" + name)
	if err != nil {
		return nil, err
	}

	if fi.IsDir() {
		return &Dir{
			name: name,
			fs:   fs,
		}, nil
	}

	f := &File{
		fs:   fs,
		name: name,
	}

	return f, nil
}

type Dir struct {
	name string
	fs   *FileSystem
	fh   *os.File
}

func (d *Dir) Read(p []byte) (int, error) {
	return 0, errors.New("(*Dir).Read() is not a valid operation")
}

func (d *Dir) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("(*Dir).Seek() is not a valid operation")
}

func (d *Dir) Readdir(count int) ([]os.FileInfo, error) {
	if d.fh == nil {
		var err error
		d.fh, err = os.Open(*repo + "/" + d.name)
		if err != nil {
			return nil, err
		}
	}

	var outInfo []os.FileInfo
	for len(outInfo) < count {
		want := count - len(outInfo)
		fis, err := d.fh.Readdir(want)
		if err != nil {
			return nil, err
		}

		for _, fi := range fis {
			if strings.HasPrefix(fi.Name(), ".") {
				continue
			}

			if fi.IsDir() {
				outInfo = append(outInfo, &Dir{
					name: d.name + "/" + fi.Name(),
					fs:   d.fs,
				})
			} else {
				outInfo = append(outInfo, &File{
					fs:   d.fs,
					name: d.name + "/" + fi.Name(),
				})
			}
		}

		if len(fis) < want {
			break
		}
	}

	return outInfo, nil
}

func (d *Dir) Close() error {
	if d.fh != nil {
		err := d.fh.Close()
		d.fh = nil
		return err
	}
	return nil
}

func (d *Dir) Stat() (os.FileInfo, error) {
	return d, nil
}

func (d *Dir) Name() string {
	return path.Base(d.name)
}

func (d *Dir) Size() int64 {
	return 0
}

func (d *Dir) Mode() os.FileMode {
	return os.ModeDir
}

func (d *Dir) ModTime() time.Time {
	return time.Time{}
}

func (d *Dir) IsDir() bool {
	return true
}

func (d *Dir) Sys() interface{} {
	return nil
}

type File struct {
	fs   *FileSystem
	name string

	pos int64

	// filled in from git-annex by readInfo
	infoLoaded   bool
	key          string
	size         int64
	hashdirlower string
	chunkSize    int64
}

func (f *File) readInfo() error {
	if f.infoLoaded {
		return nil
	}

	data, err := run("git", "annex", "find",
		"--include", "*",
		"--format", `${key}\n${bytesize}\n${hashdirlower}\n`,
		strings.TrimPrefix(f.name, "/"))
	if err != nil {
		return fmt.Errorf("couldn't run git annex find: %v", err)
	}

	lines := strings.Split(string(data), "\n")
	if len(lines) != 4 {
		return errors.New("bad output from git annex find")
	}

	f.key = lines[0]

	f.size, err = strconv.ParseInt(lines[1], 10, 64)
	if err != nil {
		return fmt.Errorf("bad size from git-annex %#v: %v", lines[1], err)
	}

	f.hashdirlower = lines[2]

	data, err = run("git", "ls-tree", "git-annex", f.hashdirlower)
	if err != nil {
		return fmt.Errorf("couldn't run git ls-tree: %v", err)
	}

	var chunkDataBlob string
	for _, line := range strings.Split(string(data), "\n") {
		if len(line) == 0 {
			continue
		}

		// format: <mode:int> SP <type:str> SP <hash:str> TAB <file:str>
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}

		file := parts[1]

		if !strings.HasSuffix(file, f.key+".log.cnk") {
			continue
		}

		parts = strings.SplitN(parts[0], " ", 3)
		if len(parts) != 3 {
			continue
		}

		if parts[1] != "blob" {
			continue
		}

		chunkDataBlob = parts[2]
	}

	if chunkDataBlob != "" {
		data, err = run("git", "show", "--raw", chunkDataBlob)
		if err != nil {
			return fmt.Errorf("couldn't run git show: %v", err)
		}

		var lastChunkSize int64
		var lastTime float64
		for _, line := range strings.Split(string(data), "\n") {
			if len(line) == 0 {
				continue
			}

			parts := strings.Split(line, " ")
			if len(parts) != 3 {
				continue
			}

			time, err := strconv.ParseFloat(
				strings.TrimSuffix(parts[0], "s"), 64)
			if err != nil {
				continue
			}

			parts = strings.Split(parts[1], ":")
			if len(parts) != 2 {
				continue
			}

			uuid := parts[0]

			if uuid != f.fs.UUID {
				continue
			}

			chunkSize, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				continue
			}

			if time < lastTime {
				continue
			}

			lastTime = time
			lastChunkSize = chunkSize
		}

		f.chunkSize = lastChunkSize
	}

	f.infoLoaded = true

	return nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case os.SEEK_SET:
		f.pos = offset
	case os.SEEK_CUR:
		f.pos += offset
	case os.SEEK_END:
		err := f.readInfo()
		if err != nil {
			return 0, err
		}

		f.pos = f.size + offset
	default:
		return f.pos, errors.New("bad whence argument in seek")
	}
	return f.pos, nil
}

func (f *File) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *File) ReadAt(p []byte, off int64) (int, error) {
	err := f.readInfo()
	if err != nil {
		return 0, err
	}

	if off > f.size {
		return 0, io.EOF
	}

	end := off + int64(len(p))
	var clippedErr error
	if end > f.size {
		end = f.size
		clippedErr = io.EOF
	}

	var offsetInChunk int64
	var startChunkIndex int64
	if f.chunkSize > 0 {
		startChunkIndex = off / f.chunkSize
		endChunkIndex := (end - 1) / f.chunkSize

		offsetInChunk = off - startChunkIndex*f.chunkSize

		if startChunkIndex != endChunkIndex {
			// split reads into chunks
			totalN := 0
			for chunk := startChunkIndex; chunk <= endChunkIndex; chunk++ {
				readSize := int(f.chunkSize - offsetInChunk)
				if readSize > len(p) {
					readSize = len(p)
				}
				n, err := f.ReadAt(p[:readSize], off)
				off += int64(n)
				totalN += n
				offsetInChunk = 0
				p = p[n:]
				if err != nil {
					return totalN, err
				}
			}
			return totalN, clippedErr
		}
	} else {
		offsetInChunk = off
	}

	var url string
	if f.chunkSize > 0 {
		url = f.chunkURL(int(startChunkIndex))
	} else {
		url = f.fs.BaseURL + f.key
	}

	if data, ok := getCached(url); ok {
		n := copy(p, data[offsetInChunk:])
		return n, nil
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v",
		offsetInChunk, offsetInChunk+int64(len(p))))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("unexpected http response %v",
			resp.StatusCode)
	}

	if resp.StatusCode == 206 {
		// partial response, no caching
		return io.ReadFull(resp.Body, p)
	}

	// full response; write the whole thing to the cache
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	setCached(url, data)

	n := copy(p, data[offsetInChunk:])
	return n, nil
}

func (f *File) chunkURL(idx int) string {
	repl := fmt.Sprintf("-S%v-C%v--", f.chunkSize, idx+1)
	return f.fs.BaseURL + strings.Replace(f.key, "--", repl, 1)
}

func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	return nil, errors.New("(*File).Readdir() is an invalid operation")
}

func (f *File) Close() error {
	return nil
}

func (f *File) Stat() (os.FileInfo, error) {
	return f, nil
}

func (f *File) Name() string {
	return path.Base(f.name)
}

func (f *File) Size() int64 {
	f.readInfo()
	return f.size
}

func (f *File) IsDir() bool {
	return false
}

func (f *File) Mode() os.FileMode {
	return 0
}

func (f *File) ModTime() time.Time {
	return time.Time{}
}

func (f *File) Sys() interface{} {
	return nil
}
