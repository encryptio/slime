// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris

package main

// TODO: windows version

func getTTYWidth() int {
	return 0
}
