// +build windows

package store

import (
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

var getDiskFreeSpaceExW = windows.MustLoadDLL("Kernel32.dll").MustFindProc("GetDiskFreeSpaceExW")

func (ds *Directory) FreeSpace() (int64, error) {
	dir, err := filepath.Abs(ds.Dir)
	if err != nil {
		return 0, err
	}

	var avail, total, free int64
	r1, _, err := getDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(dir))),
		uintptr(unsafe.Pointer(&avail)),
		uintptr(unsafe.Pointer(&total)),
		uintptr(unsafe.Pointer(&free)))
	if r1 == 0 {
		return 0, err
	}

	return free, nil
}
