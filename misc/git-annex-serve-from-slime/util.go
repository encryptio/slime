package main

import (
	"os/exec"
)

func run(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = *repo
	return cmd.Output()
}
