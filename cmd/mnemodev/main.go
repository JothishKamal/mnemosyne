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
	demoEvent := flag.Bool("demo-event", false, "print a fully-populated demo event as JSON")
	httpDemo := flag.Bool("http-demo", false, "run a two-service HTTP trace and print every event")
	scenario := flag.String("scenario", "", "record the acceptance scenario to a collector endpoint")
	flag.Parse()

	switch {
	case *showVersion:
		fmt.Println(versionString())
	case *demoEvent:
		if err := PrintEvent(os.Stdout, demo()); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case *httpDemo:
		if err := runHTTPDemo(os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case *scenario != "":
		if err := runScenario(os.Stdout, *scenario); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "no input: pass -version, -demo-event, -http-demo, or -scenario")
		os.Exit(2)
	}
}
