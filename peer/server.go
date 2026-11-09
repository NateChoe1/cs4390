// server.go is the server part of the peer that uploads packets to other peers
package peer

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// listen for connections from other peers
func StartServer(config ServerConfig) error {
	// create a listening address (defined which IP address and port this application binds to)
	address := net.JoinHostPort("", strconv.Itoa(config.Port))

	// listen for any other peers trying to connect (on a TCP connection)
	listener, err := net.Listen("tcp", address)

	// check if there was a problem starting the peer server (%w wraps the original error with the given message)
	if err != nil {
		return fmt.Errorf("start peer server: %w", err)
	}

	// close the listener when the function ends/returns
	defer listener.Close()

	// display the port this peer is listening on
	fmt.Println("peer listening on port: ", config.Port)

	// create an infinite loop to always listen for connections to other peers (until the P2P server closes)
	for {
		// accept a connection to another peer to start uploading
		conn, err := listener.Accept()

		// check if there was an error connecting to the peer
		if err != nil {
			return fmt.Errorf("accept connection: %w", err)
		}

		// go to the function that handles uploading (handleConnection)
		go handleConnection(conn, config.SharedFolder)
	}
}

// handles one connected peer
func handleConnection(conn net.Conn, sharedFolder string) {
	// close the connection when this function exits/returns
	defer conn.Close()

	// get the address of the peer that is trying to connect
	fmt.Println("connection from: ", conn.RemoteAddr())

	// create a buffer to hold one request from a peer
	reader := bufio.NewReader(conn)

	// read the reader's data up to (and includintg) a new line (\n)
	request, err := reader.ReadString('\n')

	// check for any error in reading the request
	if err != nil {
		fmt.Println("Read request: ", err)
		return
	}

	// check that the format of the request is correct (<GET fileName start end>)
	// start by checking for angled brackets
	request = strings.TrimSpace(request)
	if !strings.HasPrefix(request, "<") || !strings.HasSuffix(request, ">") {
		// send an error message to the connected peer
		sendInvalid(conn)
		return
	}

	// split the string on white space after removing brackets
	fields := strings.Fields(request[1 : len(request)-1])

	// check if there are the correct number of fields (4) in fields and that the first word in the request is GET
	if len(fields) != 4 || fields[0] != "GET" {
		sendInvalid(conn)
		return
	}

	// separate and convert the fields to the correct type (string for filename and int64 for start and end bytes)
	filename := fields[1]

	// ParseInt converts the first argument into an integer from a string; the integer must be base 10 (the second argument) and fit a 64 bit integer (the second argument)
	start, startErr := strconv.ParseInt(fields[2], 10, 64)
	end, endErr := strconv.ParseInt(fields[3], 10, 64)

	// check that start and end fall in a valid range of bytes (cannot be more than 1024 bytes or less than 0)
	if startErr != nil || endErr != nil || start < 0 || end <= start || end-start > 1024 {
		sendInvalid(conn)
		return
	}

	// open the sharedFolder directory and restrict access to only that directory
	root, err := os.OpenRoot(sharedFolder)

	// check if there was an error opening the folder
	if err != nil {
		fmt.Println("Open requested file: ", err)
		sendInvalid(conn)
		return
	}

	// close the root folder at the end of the function
	defer root.Close()

	// open the file from the root directory and store the file object in "file"
	file, err := root.Open(filename)

	// check for an error in opening the file
	if err != nil {
		fmt.Println("Open requested file: ", err)
		sendInvalid(conn)
		return
	}

	// close file at the end of the function
	defer file.Close()

	// make a buffer chunk large enough (dynamic allocation) to hold the requested file chunk (size = end-start)
	// the elements of the chunk are bytes
	chunk := make([]byte, int(end-start))

	// read the file portion into the chunk; create a reader to read the chunk (NewSectionReader) and then read into chunk (ReadFull)
	// discard the byte count (the number of bytes read) with _ (blank identifier)
	_, err = io.ReadFull(io.NewSectionReader(file, start, end-start), chunk)

	// check for an error when reading the chunk
	if err != nil {
		fmt.Println("Read chunk: ", err)
		sendInvalid(conn)
		return
	}

	// send a text header to the requesting peer stating the number of bytes it is recieving; discard the number of bytes written
	if _, err := fmt.Fprintf(conn, "<REP GET %d>\n", len(chunk)); err != nil {
		fmt.Println("Send header: ", err)
		return
	}

	// send the chunk to the requesting peer and check if the send failed
	if _, err := io.Copy(conn, strings.NewReader(string(chunk))); err != nil {
		fmt.Println("Send chunk: ", err)
		return
	}

	// state the bytes sent and to where
	fmt.Printf("Sent %s bytes [%d, %d)\n", filename, start, end)
}

// sends the requesting peer a message stating the request was invalid in some way
func sendInvalid(conn net.Conn) {
	// write the message to the requesting peer and check for any error; ignore the number of bytes written
	if _, err := io.WriteString(conn, "<GET invalid>\n"); err != nil {
		fmt.Println("Send error to peer: ", err)
	}
}
