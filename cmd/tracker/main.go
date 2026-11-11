  // Command tracker starts the tracker server.
package main

import (
	"flag"
	"log"

	"cs4390/tracker"
)

func main() {
	cfgPath := flag.String("config", "sconfig", "path to the tracker config file")
	keep := flag.Bool("keep", false, "keep existing .track files at start (default: delete them)")
	flag.Parse()

	if err := tracker.Run(*cfgPath, *keep); err != nil {
		log.Fatal(err)
	}
}
