package main

import (
	"cs4390/peer"
	"fmt"
	"log"
	"os"
	"sync"
)

func main() {
	// default config, can be set with a command line arg
	configPath := "peerconfig"

	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	config, err := peer.LoadConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Port: ", config.Port)
	fmt.Println("Shared Folder: ", config.SharedFolder)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := peer.StartServer(config); err != nil {
			log.Fatal(err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := peer.StartClient(config); err != nil {
			log.Fatal(err)
		}
	}()

	wg.Wait()
}
