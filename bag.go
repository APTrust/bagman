package bagman

import (
	"archive/tar"
	"path/filepath"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"github.com/APTrust/bagins"
	"github.com/nu7hatch/gouuid"
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

// This Tag struct is essentially the same as the bagins
// TagField struct, but its properties are public and can
// be easily serialized to / deserialized from JSON.
type Tag struct {
	Label string
	Value string
}

type GenericFile struct {
	Path             string
	Md5              string
	Sha256           string
	Uuid             string
}

type BagReadResult struct {
	Path             string
	Files            []string
	GenericFiles     []*GenericFile
	Error            error
	Tags             []Tag
	ChecksumErrors   []error
}


// Reads an untarred bag. The path parameter should be a path to
// a directory that contains the bag, info and manifest files.
// The bag content should be in the data directory under path.
// Check result.Error to ensure there were no errors.
func ReadBag(path string) (result *BagReadResult) {
	bagReadResult := new(BagReadResult)
	bagReadResult.Path = path

	// Final param to bagins.ReadBag is the name of the checksum file.
	// That param defaults to manifest-md5.txt, which is what it
	// should be for bags we're fetching from the S3 receiving buckets.
	bag, err := bagins.ReadBag(path, []string{"bagit.txt", "bag-info.txt", "aptrust-info.txt"}, "")
	if err != nil {
		bagReadResult.Error = err
		return bagReadResult
	}

	fileNames, err := bag.ListFiles()
	if err!= nil {
		bagReadResult.Error = err
		return bagReadResult
	}

	errMsg := ""
	bagReadResult.Files = make([]string, len(fileNames))
	hasBagit := false
	hasMd5Manifest := false
	hasDataFiles := false
	for index, fileName := range fileNames {
		bagReadResult.Files[index] = fileName
		if fileName == "bagit.txt" {
			hasBagit = true
		} else if fileName == "manifest-md5.txt" {
			hasMd5Manifest = true
		} else if strings.HasPrefix(fileName, "data/") {
			// This is a data file!
			hasDataFiles = true
			genericFile, err := buildGenericFile(path, fileName, bag)
			if err != nil {
				errMsg += fmt.Sprintf("Error creating GenericFile record for %s: %v. ", fileName, err)
			} else {
				bagReadResult.GenericFiles = append(bagReadResult.GenericFiles, genericFile)
			}
		}
	}
	if !hasBagit { errMsg += "Bag is missing bagit.txt file. " }
	if !hasMd5Manifest { errMsg += "Bag is missing manifest-md5.txt file. " }
	if !hasDataFiles { errMsg += "Bag's data directory is missing or empty. " }

	bagInfo, err := bag.BagInfo()
	if err != nil {
		bagReadResult.Error = err
		return bagReadResult
	}
	tagFields := bagInfo.Data.Fields()
	bagReadResult.Tags = make([]Tag, len(tagFields))

	for index, tagField := range tagFields {
		tag := Tag{ tagField.Label(), tagField.Value() }
		bagReadResult.Tags[index] = tag
	}

	checksumErrors := bag.Manifest.RunChecksums()
	if len(checksumErrors) > 0 {
		errMsg += "One or more checksums are invalid."
		bagReadResult.ChecksumErrors = make([]error, len(checksumErrors))
		copy(bagReadResult.ChecksumErrors, checksumErrors)
	}

	if errMsg != "" {
		bagReadResult.Error = errors.New(errMsg)
	}

	return bagReadResult
}

// Returns a struct with data we'll need to construct the
// GenericFile object in Fedora later. Note that we are
// generating an identifier here (uuid) and calculating the
// md5 and sha256 hashes. There's some redundancy here, because
// bagins has already calculated an md5 sum on this file when
// it validated checksums. However, we can't capture the md5
// sum bagins calculated, so we have to do it again.
// TODO: Calculate md5 only once!
func buildGenericFile(path string, fileName string, bag *bagins.Bag) (gf *GenericFile, err error) {
	gf = &GenericFile{}
	gf.Path, err = filepath.Abs(filepath.Join(path, fileName))
	if err != nil {
		return nil, err
	}
	uuid, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	gf.Uuid = uuid.String()
	md5Hash := md5.New()
	shaHash := sha256.New()
	multiWriter := io.MultiWriter(md5Hash, shaHash)
	file, err := os.Open(gf.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	io.Copy(multiWriter, file)

	gf.Md5 = fmt.Sprintf("%x", md5Hash.Sum(nil))
	gf.Sha256 = fmt.Sprintf("%x", shaHash.Sum(nil))

	return gf, nil
}
