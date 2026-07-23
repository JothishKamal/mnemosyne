// Command mnemodev is the Mnemosyne developer harness. It prints recorded
// events as JSON so every implementation step can be exercised by eye.
package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "0.0.1-dev"

func versionString() string {
	return "mnemodev " + version
}

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(versionString())
		os.Exit(0)
	}

	fmt.Fprintln(os.Stderr, "no input: pass -version (event printing lands in Task 1)")
	os.Exit(2)
}
