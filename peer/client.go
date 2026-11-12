// client.go will be the client part of the peer that downloads a packet from other peers
package peer

import (
	"bufio"
	"crypto/md5"
	"cs4390/tracker"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func StartClient(config PeerConfig) error {
	var wg sync.WaitGroup

	root, err := os.OpenRoot(config.SharedFolder)

	if err != nil {
		return err
	}

	for _, file := range config.Files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := peerFile(config, file, root); err != nil {
				log.Fatal(err)
			}
		}()
	}

	wg.Wait()

	return nil
}

func peerFile(config PeerConfig, file string, root *os.Root) error {
	if stat, err := root.Stat(file); err == nil {
		// the file does exist and we have the full thing. create the
		// tracker, update the tracker file, exit
		makeTracker(config.TrackerAddress, file, root, config)
		updateTracker(config, file, 0, stat.Size())

		return nil
	}

	trackerFile, err := getTracker(config.TrackerAddress, file)
	if err != nil {
		log.Fatal(err)
	}

	writer, err := root.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		log.Fatal(err)
	}

	// the file does not exist, i.e. we don't have it
	flen := trackerFile.Size
	numBlocks := (flen + 1023) / 1024
	startBlock := rand.Int63n(numBlocks)

	writer.Truncate(flen)

	// start=end, i.e. we don't have any data. we start at some
	// random block-aligned location so that different nodes get
	// different sections of the file
	start := startBlock * 1024
	end := startBlock * 1024
	updateTracker(config, file, start, end)

	lastRefresh := time.Now()

	for {
		if start <= 0 && end >= flen {
			return nil
		}

		if time.Since(lastRefresh) >= 5*time.Second {
			updatedTracker, err := getTracker(config.TrackerAddress, file)
			if err != nil {
				fmt.Println("Tracker refresh failed:", err)
			} else {
				trackerFile = updatedTracker
			}
			lastRefresh = time.Now()
		}

		var blockStart, blockEnd int64
		forwards := rand.Int()%2 == 0

		if start == 0 {
			forwards = true
		} else if end == flen {
			forwards = false
		}
		if forwards {
			blockStart = end
			blockEnd = min(end+1024, flen)
		} else {
			blockStart = start - 1024
			blockEnd = start
		}

		peersWithChunk := make([]string, 0)
		for _, peer := range trackerFile.Peers {
			if peer.Start <= blockStart && peer.End >= blockEnd {
				peersWithChunk = append(peersWithChunk, fmt.Sprintf("%s:%d", peer.IP, peer.Port))
			}
		}

		chosenPeer := rand.Intn(len(peersWithChunk))

		data, err := DownloadChunk(peersWithChunk[chosenPeer], file, blockStart, blockEnd)
		if err != nil {
			fmt.Println(err)
			continue
		}

		n, err := writer.WriteAt(data, blockStart)
		if n < len(data) {
			fmt.Println("Short write")
			continue
		}
		if err != nil {
			fmt.Println(err)
			continue
		}

		start = min(start, blockStart)
		end = max(end, blockEnd)

		updateTracker(config, file, start, end)
	}
}

func getTracker(trackerAddr string, file string) (*tracker.Tracker, error) {
	data, err := request(trackerAddr, []string{"GET", file})
	if err != nil {
		return nil, err
	}

	fmt.Println(data)
	if strings.ToLower(data[0]) != "<rep get begin>" {
		return nil, fmt.Errorf("GET returned wrong header")
	}

	lastLine := strings.ToLower(data[len(data)-1])
	if !strings.HasPrefix(lastLine, "<rep get end") || !strings.HasSuffix(lastLine, ">") {
		return nil, fmt.Errorf("GET returned wrong footer")
	}

	data = data[1 : len(data)-1]
	rawFile := strings.Join(data, "\n")

	return tracker.ParseTracker(rawFile)
}

func makeTracker(tracker string, file string, root *os.Root, config PeerConfig) error {
	hasher := md5.New()
	reader, err := root.Open(file)

	if err != nil {
		return err
	}

	var sum []byte
	size := 0

	for {
		buf := make([]byte, 1024)
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		sum = hasher.Sum(buf[:n])
		size += n
		if err == io.EOF {
			break
		}
	}

	marshaledSum := hex.EncodeToString(sum)

	_, err = request(tracker, []string{"CREATETRACKER", file, strconv.Itoa(size),
		marshaledSum, config.MyIP.String(), strconv.Itoa(config.Port)})

	return err
}

func updateTracker(config PeerConfig, file string, start, end int64) {
	request(config.TrackerAddress, []string{"UPDATETRACKER", file,
		strconv.FormatInt(start, 10), strconv.FormatInt(end, 10),
		config.MyIP.String(), strconv.Itoa(config.Port)})
}

func request(tracker string, request []string) ([]string, error) {
	conn, err := net.DialTimeout("tcp", tracker, 5*time.Second)
	if err != nil {
		return nil, err
	}

	_, err = fmt.Fprintf(conn, "<%s>\n", strings.Join(request, " "))
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	ret := make([]string, 0)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			return ret, nil
		}
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			return ret, nil
		}

		ret = append(ret, line)
	}

	return nil, nil
}

// requests bytes in the format [start, end) from another peer
func DownloadChunk(address string, filename string, start int64, end int64) ([]byte, error) {
	// check that the chunk range is valid
	if start < 0 || end <= start || end-start > 1024 {
		return nil, fmt.Errorf("invalid chunk range")
	}

	// check that the filename is a valid name (cannot have whitespace, angle brackets, newline, or return)
	if filename == "" || strings.ContainsAny(" \t\r\n<>", filename) {
		return nil, fmt.Errorf("invalid filename")
	}

	// connect to the peer; wait 5 seconds, produce error if no connection happens within the time
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("Connect to peer: %w", err)
	}
	defer conn.Close() // wait until end of function to close connection

	// limit time for sending and receiving to 10 seconds
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, fmt.Errorf("Set connection deadline: %w", err)
	}

	// send the request in the correct format (<GET filename start end>)
	_, err = fmt.Fprintf(conn, "<GET %s %d %d>\n", filename, start, end)
	if err != nil {
		fmt.Errorf("Send request: %w", err)
	}

	// create a reader to read the response from the peer (look for how many bytes are being sent)
	reader := bufio.NewReader(conn)

	// read the header sent from the server peer (<REP GET numBytes>\n)
	header, err := reader.ReadString('\n') // read up to the \n
	if err != nil {
		return nil, fmt.Errorf("Read sender's reply: %w", err)
	}

	// get rid of whitespace and \n
	header = strings.TrimSpace(header)

	// check if the header was <GET invalid>
	if header == "<GET invalid>" {
		return nil, fmt.Errorf("peer rejected request")
	}

	// check that the received header has correct format
	if !strings.HasPrefix(header, "<") || !strings.HasSuffix(header, ">") {
		return nil, fmt.Errorf("Received header does not have correct format")
	}

	//get rid of the "<" and ">" and turn remaining strings into an array
	fields := strings.Fields(header[1 : len(header)-1])

	// check that fields is the correct size (must have 3 characters) and each field is correct
	if len(fields) != 3 || fields[0] != "REP" || fields[1] != "GET" {
		return nil, fmt.Errorf("Unexpected response: %s", header)
	}

	// turn the bytes field into an int64
	size, err := strconv.ParseInt(fields[2], 10, 64)

	// check that the size is valid
	if err != nil || size != end-start {
		return nil, fmt.Errorf("Response size does not match requested chunk")
	}

	// create a dynamically allocated chunk buffer to read the chunk
	chunk := make([]byte, int(size))

	// read everything from the reader created earlier into the chunk buffer
	_, err = io.ReadFull(reader, chunk)

	// check if it was read correctly
	if err != nil {
		return nil, fmt.Errorf("Receive chunk: %w", err)
	}

	return chunk, nil
}
