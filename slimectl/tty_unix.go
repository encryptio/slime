// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package main

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

func getTTYWidth() int {
	var fd uintptr

	fh, err := os.Open("/dev/tty")
	if err == nil {
		fd = fh.Fd()
		defer fh.Close()
	} else {
		fd = uintptr(unix.Stdin)
	}

	var dimensions [4]uint16

	if _, _, err := unix.Syscall6(unix.SYS_IOCTL, fd, uintptr(unix.TIOCGWINSZ), uintptr(unsafe.Pointer(&dimensions)), 0, 0, 0); err != 0 {
		return 0
	}

	return int(dimensions[1])
}
