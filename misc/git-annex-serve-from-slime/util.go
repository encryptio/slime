package main

import (
	"log"
	"os/exec"
)

func run(name string, args ...string) ([]byte, error) {
	log.Printf("Running %v with args %v", name, args)
	cmd := exec.Command(name, args...)
	cmd.Dir = *repo
	return cmd.Output()
}
