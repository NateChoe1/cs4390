package main

import (
	"cs4390/peer"
	"fmt"
	"log"
)

func main() {
	config, err := peer.LoadServerConfig("serverThreadConfig.cfg")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Port: ", config.Port)
	fmt.Println("Shared Folder: ", config.SharedFolder)

	if err := peer.StartServer(config); err != nil {
		log.Fatal(err)
	}
}
