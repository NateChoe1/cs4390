// client.go will be the client part of the peer that downloads a packet from other peers
package peer

import (
	"log"
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
	"sync"
	"os"
	"crypto/md5"
	"encoding/hex"
)

func StartClient(config PeerConfig) error {
	var wg sync.WaitGroup

	root, err := os.OpenRoot(config.SharedFolder)

	if err != nil {
		return err
	}

	for _, file := range(config.Files) {
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
	var start, end, flen int64

	if stat, err := root.Stat(file); err != nil {
		// the file does not exist, i.e. we don't have it
		start = 0
		end = 0
	} else {
		// the file does exist and we have the full thing
		start = 0
		end = stat.Size()
		flen = end
		makeTracker(config.TrackerAddress, file, root, config)
	}

	fmt.Println(start)
	fmt.Println(flen)

	for {
	}
	return nil
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

	_, err = request(tracker, []string{"CREATETRACKER", string(size),
		marshaledSum, config.MyIP.String(), string(config.Port)})

	return err
}

func request(tracker string, request []string) ([][]string, error) {
	conn, err := net.DialTimeout("tcp", tracker, 5*time.Second)
	if err != nil {
		return nil, err
	}

	_, err = fmt.Fprintf(conn, "<%s>\n", strings.Join(request, " "))
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	ret := make([][]string, 0)
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

		if !strings.HasPrefix(line, "<") || !strings.HasSuffix(line, ">") {
			return nil, fmt.Errorf("Received line from tracker server does not have correct format")
		}

		fields := strings.Fields(line[1 : len(line)-1])
		ret = append(ret, fields)
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
