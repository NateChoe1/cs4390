package peer

import (
	"fmt"
	"os"
)

// downloads a file given by a separate peer into a local file
func DownloadFile(address, filename, outputPath string, filesize int64) error {
	// check if filesize is negative
	if filesize < 0 {
		return fmt.Errorf("filsize cannot be negative")
	}

	// create a new file at the outputPath; return an error if the file already exists
	output, err := os.OpenFile(outputPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)

	// check if the OpenFile succeeded
	if err != nil {
		return fmt.Errorf("Could not create file: %w", err)
	}

	// close the file at the end of this function
	defer output.Close()

	// loop through the entire uploaded file and download all possible chunks
	for start := int64(0); start < filesize; {
		// can request at most 1024 bytes
		length := int64(1024)

		// if the remaining filesize (after removing start) is less than 1024 bytes, make the max length equal to the number of remaining bytes
		if filesize-start < length {
			length = filesize - start
		}

		// end is the end of the requested chunk
		end := start + length

		// download the chunk
		chunk, err := DownloadChunk(address, filename, start, end)

		// check for any errors when downloading the chunk
		if err != nil {
			return fmt.Errorf("Could not download the chunk: %w", err)
		}

		// write to the file ad check if the Writing was successful
		_, err = output.Write(chunk)
		if err != nil {
			return fmt.Errorf("Could not write to file: %w", err)
		}

		fmt.Printf("Downloaded bytes [%d, %d) of %s\n", start, end, filename)

		// make start = end to move up the chunk of the file being downloaded
		start = end
	}

	// close the output file and check if closing worked
	err = output.Close()
	if err != nil {
		return fmt.Errorf("Could not close file: %w", err)
	}

	return nil
}
