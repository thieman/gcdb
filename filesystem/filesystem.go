package filesystem

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gamechanger/gcdb/constants"
)

func EnsureCurrentDataFile() (*os.File, error) {
	path, err := latestDataFilePath()
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if fileInfo.Size() == 0 {
		log.Printf("Expanding data file %s to initial size %d", path, constants.DataFileSize)
		err = file.Truncate(constants.DataFileSize)
		if err != nil {
			return nil, err
		}
	}
	return file, nil
}

// Return the file path of the latest created data file,
// or the path for an initial data.0 file if none have
// yet been created
func latestDataFilePath() (string, error) {
	files, err := ioutil.ReadDir(constants.DataDir)
	if err != nil {
		return "", err
	}
	var latest *int
	for idx := range files {
		if strings.HasPrefix(files[idx].Name(), "data.") {
			pieces := strings.Split(files[idx].Name(), ".")
			fileNum, err := strconv.Atoi(pieces[1])
			if err != nil {
				return "", err
			}
			if latest == nil || fileNum > *latest {
				latest = &fileNum
			}
		}
	}
	if latest == nil {
		return filepath.Join(constants.DataDir, "data.0"), nil
	}
	return filepath.Join(constants.DataDir, fmt.Sprintf("data.%d", *latest)), nil
}
