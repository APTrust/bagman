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
	"time"
	"github.com/APTrust/bagins"
	"github.com/nu7hatch/gouuid"
	"github.com/rakyll/magicmime"
)

// magicMime is the MimeMagic database. We want
// just one copy of this open at a time.
var magicMime *magicmime.Magic

// GenericFile contains information about a generic
// data file within the data directory of bag or tar archive.
type GenericFile struct {
	Path             string
	Size             int64
	Created          time.Time  // we currently have no way of getting this
	Modified         time.Time
	Md5              string
	Sha256           string
	Uuid             string
	MimeType         string
	Error            error
}

// TarResult contains information about the attempt to untar
// a bag.
type TarResult struct {
	InputFile       string
	OutputDir       string
	Error           error
	Warnings        []string
	FilesUnpacked   []string
	GenericFiles    []*GenericFile
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
			if strings.Contains(header.Name, "data/") {
				genericFile := buildGenericFile(tarReader, filepath.Dir(absInputFile), header.Name,
					header.Size, header.ModTime)
				tarResult.GenericFiles = append(tarResult.GenericFiles, genericFile)
			} else {
				err = saveFile(outputPath, tarReader)
				if err != nil {
					tarResult.Error = err
					return tarResult
				}
			}

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

// BagReadResult contains data describing the result of
// processing a single bag. If there were any processing
// errors, this structure should tell us exactly what
// happened and where.
type BagReadResult struct {
	Path             string
	Files            []string
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
			hasDataFiles = true
		}
	}
	if !hasBagit { errMsg += "Bag is missing bagit.txt file. " }
	if !hasMd5Manifest { errMsg += "Bag is missing manifest-md5.txt file. " }
	if !hasDataFiles { errMsg += "Bag's data directory is missing or empty. " }

	extractTags(bag, bagReadResult)

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

// Extract all of the tags from tag files "bagit.txt", "bag-info.txt",
// and "aptrust-info.txt", and put those tags into the Tags member
// of the BagReadResult structure.
func extractTags(bag *bagins.Bag, bagReadResult *BagReadResult) {
	tagFiles := []string{"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	for _, file := range tagFiles {
		tagFile, err := bag.TagFile(file)
		if err != nil {
			bagReadResult.Error = err
			return
		}
		tagFields := tagFile.Data.Fields()

		for _, tagField := range tagFields {
			tag := Tag{ tagField.Label(), tagField.Value() }
			bagReadResult.Tags = append(bagReadResult.Tags, tag)
		}
	}
}

// Saves a file from the tar archive to local disk. This function
// used to save non-data files (manifests, tag files, etc.)
func saveFile(destination string, tarReader *tar.Reader) (error) {
	outputWriter, err := os.OpenFile(destination, os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer outputWriter.Close()
	_, err = io.Copy(outputWriter, tarReader);
	if err != nil {
		return err
	}
	return nil
}

// buildGenericFile saves a data file from the tar archive to disk,
// then returns a struct with data we'll need to construct the
// GenericFile object in Fedora later.
func buildGenericFile(tarReader *tar.Reader, path string, fileName string, size int64, modTime time.Time) (gf *GenericFile) {
	gf = &GenericFile{}
	gf.Path = fileName[strings.Index(fileName, "/data/") + 1:len(fileName)]
	absPath, err := filepath.Abs(filepath.Join(path, fileName))
	if err != nil {
		gf.Error = fmt.Errorf("Path error: %v", err)
		return gf
	}
	uuid, err := uuid.NewV4()
	if err != nil {
		gf.Error = fmt.Errorf("UUID error: %v", err)
		return gf
	}
	gf.Uuid = uuid.String()
	gf.Size = size
	gf.Modified = modTime

	// Set up a MultiWriter to stream data ONCE to file,
	// md5 and sha256. We don't want to process the stream
	// three separate times.
	outputWriter, err := os.OpenFile(absPath, os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		gf.Error = fmt.Errorf("Error opening writing to %s: %v", absPath, err)
		return gf
	}
	defer outputWriter.Close()
	md5Hash := md5.New()
	shaHash := sha256.New()
	multiWriter := io.MultiWriter(md5Hash, shaHash, outputWriter)

	io.Copy(multiWriter, tarReader)

	gf.Md5 = fmt.Sprintf("%x", md5Hash.Sum(nil))
	gf.Sha256 = fmt.Sprintf("%x", shaHash.Sum(nil))

	// Open the Mime Magic DB only once.
	if magicMime == nil {
		magicMime, err = magicmime.New()
		if err != nil {
			gf.Error = fmt.Errorf("Error opening MimeMagic database: %v", err)
			return gf
		}
	}

	mimetype, err := magicMime.TypeByFile(absPath)
	if err != nil {
		gf.Error = fmt.Errorf("Error determining mime type: %v", err)
		return gf
	}
	if mimetype == "" {
		gf.MimeType = "application/binary"
	} else {
		gf.MimeType = mimetype
	}

	return gf
}
