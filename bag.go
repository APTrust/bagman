package bagman

import (
	"archive/tar"
	"path/filepath"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/fluctus/models"
	"github.com/nu7hatch/gouuid"
	"github.com/rakyll/magicmime"
)

// magicMime is the MimeMagic database. We want
// just one copy of this open at a time.
var magicMime *magicmime.Magic

// Untars the file at the specified path and returns a list
// of files that were untarred from the archive. Check
// result.Error to ensure there were no errors.
// path is the path to the tar file that you want to unpack.
// instDomain is the domain name of the institution that owns the bag.
// bagName is the name of the tar file, minus the ".tar" extension.
func Untar(path, instDomain, bagName string) (result *TarResult) {

	// Set up our result
	tarResult := new(TarResult)
	absInputFile, err := filepath.Abs(path)
	if err != nil {
		tarResult.ErrorMessage = fmt.Sprintf("Before untarring, could not determine " +
			"absolute path to downloaded file: %v", err)
		return tarResult
	}
	tarResult.InputFile = absInputFile

	// Open the tar file for reading.
	file, err := os.Open(path)
	if file != nil {
		defer file.Close()
	}
	if err != nil {
		tarResult.ErrorMessage = fmt.Sprintf("Could not open file %s for untarring: %v",
			path, err)
		return tarResult
	}

	// Untar the file and record the results.
	tarReader := tar.NewReader(file)

	for {
		header, err := tarReader.Next();
		if err != nil && err.Error() == "EOF" {
			break  // end of archive
		}
		if err != nil {
			tarResult.ErrorMessage = fmt.Sprintf("Error reading tar file header: %v", err)
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
			tarResult.ErrorMessage = fmt.Sprintf("Could not create destination file '%s' " +
				"while unpacking tar archive: %v", outputPath, err)
			return tarResult
		}

		// Copy the file, if it's an actual file. Otherwise, ignore it and record
		// a warning. The bag library does not deal with items like symlinks.
		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
			if strings.Contains(header.Name, "data/") {
				genericFile := buildGenericFile(tarReader, filepath.Dir(absInputFile), header.Name,
					header.Size, header.ModTime)
				genericFile.Identifier = fmt.Sprintf("%s.%s/%s", instDomain, bagName, genericFile.Path)
				genericFile.IdentifierAssigned = time.Now()
				tarResult.GenericFiles = append(tarResult.GenericFiles, genericFile)
			} else {
				err = saveFile(outputPath, tarReader)
				if err != nil {
					tarResult.ErrorMessage = fmt.Sprintf("Error copying file from tar archive " +
						"to '%s': %v", outputPath, err)
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
		bagReadResult.ErrorMessage = fmt.Sprintf("Error unpacking bag: %v", err)
		return bagReadResult
	}

	fileNames, err := bag.ListFiles()
	if err!= nil {
		bagReadResult.ErrorMessage = fmt.Sprintf("Could not list bag files: %v", err)
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
		errMsg += "The following checksums could not be verified: "
		bagReadResult.ChecksumErrors = make([]error, len(checksumErrors))
		for i, err := range checksumErrors {
			bagReadResult.ChecksumErrors[i] = err
			errMsg += err.Error() + ". "
		}
	}

	if errMsg != "" {
		bagReadResult.ErrorMessage += fmt.Sprintf(errMsg)
	}

	return bagReadResult
}

// Extract all of the tags from tag files "bagit.txt", "bag-info.txt",
// and "aptrust-info.txt", and put those tags into the Tags member
// of the BagReadResult structure.
func extractTags(bag *bagins.Bag, bagReadResult *BagReadResult) {
	tagFiles := []string{"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	accessRights := ""
	for _, file := range tagFiles {
		tagFile, err := bag.TagFile(file)
		if err != nil {
			bagReadResult.ErrorMessage = fmt.Sprintf("Error reading tags from bag: %v", err)
			return
		}
		tagFields := tagFile.Data.Fields()

		for _, tagField := range tagFields {
			tag := Tag{ tagField.Label(), tagField.Value() }
			bagReadResult.Tags = append(bagReadResult.Tags, tag)

			lcLabel := strings.ToLower(tag.Label)
			if lcLabel == "access" {
				accessRights = strings.ToLower(tag.Value)
			} else if accessRights == "" && lcLabel == "rights" {
				accessRights = strings.ToLower(tag.Value)
			}
		}
	}

	// Make sure access rights are valid, or Fluctus will reject
	// this data when we try to register it.
	accessValid := false
	for _, value := range(models.AccessRights) {
		if accessRights == value {
			accessValid = true
		}
	}
	if false == accessValid {
		bagReadResult.ErrorMessage += fmt.Sprintf("Access (rights) value '%s' is not valid. ", accessRights)
	}
}

// Saves a file from the tar archive to local disk. This function
// used to save non-data files (manifests, tag files, etc.)
func saveFile(destination string, tarReader *tar.Reader) (error) {
	outputWriter, err := os.OpenFile(destination, os.O_CREATE | os.O_WRONLY, 0644)
	if outputWriter != nil {
		defer outputWriter.Close()
	}
	if err != nil {
		return err
	}
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
		gf.ErrorMessage = fmt.Sprintf("Path error: %v", err)
		return gf
	}
	uuid, err := uuid.NewV4()
	if err != nil {
		gf.ErrorMessage = fmt.Sprintf("UUID error: %v", err)
		return gf
	}
	gf.Uuid = uuid.String()
	gf.UuidGenerated = time.Now().UTC()
	gf.Size = size
	gf.Modified = modTime

	// Set up a MultiWriter to stream data ONCE to file,
	// md5 and sha256. We don't want to process the stream
	// three separate times.
	outputWriter, err := os.OpenFile(absPath, os.O_CREATE | os.O_WRONLY, 0644)
	if outputWriter != nil {
		defer outputWriter.Close()
	}
	if err != nil {
		gf.ErrorMessage = fmt.Sprintf("Error opening writing to %s: %v", absPath, err)
		return gf
	}
	md5Hash := md5.New()
	shaHash := sha256.New()
	multiWriter := io.MultiWriter(md5Hash, shaHash, outputWriter)
	io.Copy(multiWriter, tarReader)

	gf.Md5 = fmt.Sprintf("%x", md5Hash.Sum(nil))
	gf.Sha256 = fmt.Sprintf("%x", shaHash.Sum(nil))
	gf.Sha256Generated = time.Now().UTC()

	// Open the Mime Magic DB only once.
	if magicMime == nil {
		magicMime, err = magicmime.New()
		if err != nil {
			gf.ErrorMessage = fmt.Sprintf("Error opening MimeMagic database: %v", err)
			return gf
		}
	}

	mimetype, err := magicMime.TypeByFile(absPath)
	if err != nil {
		gf.ErrorMessage = fmt.Sprintf("Error determining mime type: %v", err)
		return gf
	}
	gf.MimeType = mimetype
	if gf.MimeType == "" {
		gf.MimeType = "application/binary"
	}

	return gf
}
