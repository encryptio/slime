package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/naoina/toml"
)

var conf struct {
	Wide    bool   `toml:"wide"`
	BaseURL string `toml:"base_url"`
}

func setOptions() {
	// first set default values
	conf.BaseURL = "baseurl default"

	// then read slimectl.toml, if available
	data, err := ioutil.ReadFile(filepath.Join(os.Getenv("HOME"), ".config", "slimectl.toml"))
	if err == nil {
		err = toml.Unmarshal(data, &conf)
		if err != nil {
			fmt.Printf("Couldn't parse slimectl.toml: %v", err)
			os.Exit(1)
		}
	}

	// then add flags
	flag.BoolVar(&conf.Wide, "w", conf.Wide, "never ellipsize columns")
	flag.StringVar(&conf.BaseURL, "base", conf.BaseURL, "slime proxy base url")
}

func showUsage() {
	prog := os.Args[0]
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", prog)
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Flags (use before subcommand):\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Subcommands:\n")
	fmt.Fprintf(os.Stderr, "    %s store list\n", prog)
	fmt.Fprintf(os.Stderr, "    %s store dead <storeid>\n", prog)
	fmt.Fprintf(os.Stderr, "    %s store undead <storeid>\n", prog)
	fmt.Fprintf(os.Stderr, "    %s store delete <storeid>\n", prog)
	fmt.Fprintf(os.Stderr, "    %s store scan <url>\n", prog)
	fmt.Fprintf(os.Stderr, "    %s store rescan\n", prog)
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "    %s redundancy\n", prog)
	fmt.Fprintf(os.Stderr, "    %s redundancy set <need> <total>\n", prog)
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "A \"storeid\" may be a uuid or a unique substring of a store's name\n")
}

func main() {
	flag.Usage = showUsage
	setOptions()
	flag.Parse()

	args := flag.Args()

	if len(args) < 1 {
		showUsage()
		os.Exit(1)
	}

	var err error
	switch args[0] {
	case "store":
		err = handleStore(args[1:])
	case "redundancy":
		err = handleRedundancy(args[1:])
	default:
		err = fmt.Errorf("unknown subcommand %v", args[0])
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
