// part of the peer package; combined with all other files of the folder "peer"
package peer

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"net"
)

type PeerConfig struct {
	Port             int
	SharedFolder     string
	TrackerAddress   string
	Files            []string
	MyIP             net.IP
}

// read the settings file (check for any errors as well)
func LoadConfig(filename string) (PeerConfig, error) {
	// read file
	data, err := os.ReadFile(filename)
	if err != nil {
		return PeerConfig{}, err
	}

	// split into lines
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")

	// check if the correct number of lines are present (need at least 2)
	if len(lines) < 3 {
		return PeerConfig{}, fmt.Errorf("config must have at least 3 lines")
	}

	// get port
	port, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil || port < 1 || port > 65535 {
		return PeerConfig{}, fmt.Errorf("port number must be between 1 and 65535")
	}

	// get folder name
	folder := strings.TrimSpace(lines[1])
	if folder == "" {
		return PeerConfig{}, fmt.Errorf("shared folder must have a name")
	}

	// get tracker address
	trackerAddress := strings.TrimSpace(lines[2])
	if trackerAddress == "" {
		return PeerConfig{}, fmt.Errorf("tracker address must not be empty")
	}

	/* https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
	 *
	 * this doesn't actually send any packets, it's just used to get the
	 * local address
	 */
	conn, err := net.Dial("udp", "1.1.1.1:53")
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	defer conn.Close()

	files := make([]string, 0)
	for i := 3; i < len(lines); i += 1 {
		file := strings.TrimSpace(lines[i])
		if file == "" {
			continue
		}
		files = append(files, file)
	}

	// everything after line 2 is a file to receive/share as a peer
	return PeerConfig{
		Port:           port,
		SharedFolder:   folder,
		TrackerAddress: trackerAddress,
		Files:          files,
		MyIP:           localAddr.IP,
	}, nil
}
