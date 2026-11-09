package main

import (
	"cs4390/peer"
	"fmt"
	"log"
)

func main() {
	err := peer.DownloadFile(
		"127.0.0.1:9001",
		"large-example.txt",
		"downloaded-large-example.txt",
		2500,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("File transfer finished")
}
