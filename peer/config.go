// part of the peer package; combined with all other files of the folder "peer"
package peer

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// create a struct to contain all the settings of the server (read from the config file serverThreadConfig)
type ServerConfig struct {
	Port         int // since this is a capital letter, it is exported and can be used by other packaged (i.e., main)
	SharedFolder string
}

// read the settings file (check for any errors as well)
func LoadServerConfig(filename string /*params*/) (ServerConfig, error /*returns*/) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return ServerConfig{}, err
	}

	// convert the data from the config file into a string and split it (on the \n) to create a slice (an array) of strings
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")

	// check if the correct number of lines are present (need 2)
	if len(lines) != 2 {
		return ServerConfig{}, fmt.Errorf("config must have exactly 2 lines")
	}

	// convert the port string from the config file to an integer
	port, err := strconv.Atoi(strings.TrimSpace(lines[0]))

	// check if the port number is within th ecorrect range and Atoi did not fail
	if err != nil || port < 1 || port > 65535 {
		return ServerConfig{}, fmt.Errorf("port number must be between 1 and 65535")
	}

	// get the folder name
	folder := strings.TrimSpace(lines[1])

	// check if the folder name is empty
	if folder == "" {
		return ServerConfig{}, fmt.Errorf("shared folder must have a name")
	}

	// return with no errors and correct port/folder name
	return ServerConfig{
		Port:         port,
		SharedFolder: folder,
	}, nil
}
