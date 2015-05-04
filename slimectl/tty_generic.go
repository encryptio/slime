// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris

package main

// TODO: windows version

func getTTYSize() int {
	return 0
}
