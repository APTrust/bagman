package bagman

import (
	"archive/tar"
	"path/filepath"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"github.com/APTrust/bagins"
)

type TarResult struct {
	InputFile       string
	OutputDir       string
	Error           error
	Warnings        []string
	FilesUnpacked   []string
}


// Untars the file at the specified path and returns a list
// of files that were untarred from the archive. Check
// result.Error to ensure there were no errors.
func Untar(path string) (result *TarResult) {

	// Set up our result
	tarResult := new(TarResult)
	absInputFile, err := filepath.Abs(path)
	if err != nil {
		tarResult.Error = err
		return tarResult
	}
	tarResult.InputFile = absInputFile

	// Open the tar file for reading.
	file, err := os.Open(path)
	if err != nil {
		tarResult.Error = err
		return tarResult
	}
	defer file.Close()

	// Untar the file and record the results.
	tarReader := tar.NewReader(file)
	for {
		header, err := tarReader.Next();
		if err != nil && err.Error() == "EOF" {
			break  // end of archive
		}
		if err != nil {
			tarResult.Error = err
			return tarResult
		}
		outputPath := filepath.Join(filepath.Dir(absInputFile), header.Name)
		tarDirectory := strings.Split(header.Name, "/")[0]
		if tarResult.OutputDir == "" {
			tarResult.OutputDir = filepath.Join(filepath.Dir(absInputFile), tarDirectory)
		}

		// Make sure the directory that we're about to write into exists.
		err = os.MkdirAll(filepath.Dir(outputPath), 0755)
		if err != nil {
			tarResult.Error = err
			return tarResult
		}

		// Copy the file, if it's an actual file. Otherwise, ignore it and record
		// a warning. The bag library does not deal with items like symlinks.
		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
			outputWriter, err := os.OpenFile(outputPath, os.O_CREATE | os.O_WRONLY, 0644)
			if err != nil {
				tarResult.Error = err
				return tarResult
			}
			defer outputWriter.Close()
			io.Copy(outputWriter, tarReader);
			outputRelativePath := strings.Replace(outputPath, tarResult.OutputDir + "/", "", 1)
			tarResult.FilesUnpacked = append(tarResult.FilesUnpacked, outputRelativePath)
		} else if header.Typeflag != tar.TypeDir {
			tarResult.Warnings = append(tarResult.Warnings,
				fmt.Sprintf("Ignoring item %s of type %c because it's neither a file nor a directory",
					header.Name, header.Typeflag))
		}
	}
	sort.Strings(tarResult.FilesUnpacked)
	return tarResult
}


type BagReadResult struct {
	Path             string
	Files            []string
	Error            error
	Tags             []bagins.TagField
	ChecksumErrors   []error
}


// Reads an untarred bag. The path parameter should be a path to
// a directory that contains the bag, info and manifest files.
// The bag content should be in the data directory under path.
// Check result.Error to ensure there were no errors.
func ReadBag(path string) (result *BagReadResult) {
	bagReadResult := new(BagReadResult)
	bagReadResult.Path = path

	bag, err := bagins.ReadBag(path, []string{"bagit.txt", "bag-info.txt", "aptrust-info.txt"}, "") //"manifest-md5.txt")
	if err != nil {
		bagReadResult.Error = err
		return bagReadResult
	}

	fileNames, err := bag.ListFiles()
	if err!= nil {
		bagReadResult.Error = err
		return bagReadResult
	}
	bagReadResult.Files = make([]string, len(fileNames))
	copy(bagReadResult.Files, fileNames)

	bagInfo, err := bag.BagInfo()
	if err != nil {
		bagReadResult.Error = err
		return bagReadResult
	}
	tagFields := bagInfo.Data.Fields()
	bagReadResult.Tags = make([]bagins.TagField, len(tagFields))
	copy(bagReadResult.Tags, tagFields)

	checksumErrors := bag.Manifest.RunChecksums()
	if len(checksumErrors) > 0 {
		bagReadResult.Error = errors.New("One or more checksums were invalid.")
		bagReadResult.ChecksumErrors = make([]error, len(checksumErrors))
		copy(bagReadResult.ChecksumErrors, checksumErrors)
	}
	return bagReadResult
}
