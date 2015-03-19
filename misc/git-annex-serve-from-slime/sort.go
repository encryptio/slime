package main

import (
	"os"
)

type fileInfoByName []os.FileInfo

func (f fileInfoByName) Len() int           { return len(f) }
func (f fileInfoByName) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f fileInfoByName) Less(i, j int) bool { return f[i].Name() < f[j].Name() }
