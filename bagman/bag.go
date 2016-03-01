package bagman

import (
	"archive/tar"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/satori/go.uuid"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// Untars the file at the specified tarFilePath and returns a list
// of files that were untarred from the archive. Check
// result.Error to ensure there were no errors.
// tarFilePath is the tarFilePath to the tar file that you want to unpack.
// instDomain is the domain name of the institution that owns the bag.
// bagName is the name of the tar file, minus the ".tar" extension.
//
// If param buildIngestData is true, the untar operation will run
// md5 and sha256 checksums on all of the data files in the archive,
// and it will use MimeMagic to figure out each file's mime type.
// We must do this during ingest, so buildIngestData must be true
// when we're running this on our servers.
//
// If we're running in a client environment--that is, an APTrust partner
// is running the bag validation application on their laptop or desktop--
// buildIngestData should be false. There's no need to calculate sha256
// checksums in that case, and we cannot assume that the client would have
// the external MimeMagic C libraries installed. The lack of those libraries
// would cause the application to crash. So buildIngestData == false on
// the client!!
func Untar(tarFilePath, instDomain, bagName string, buildIngestData bool) (result *TarResult) {

	// Set up our result
	tarResult := new(TarResult)
	absInputFile, err := filepath.Abs(tarFilePath)
	if err != nil {
		tarResult.ErrorMessage = fmt.Sprintf("Before untarring, could not determine "+
			"absolute path to downloaded file: %v", err)
		return tarResult
	}
	tarResult.InputFile = absInputFile

	_, err = GetInstitutionFromBagName(filepath.Base(absInputFile))
	if err != nil {
		tarResult.ErrorMessage = err.Error()
		return tarResult
	}

	// Open the tar file for reading.
	file, err := os.Open(tarFilePath)
	if file != nil {
		defer file.Close()
	}
	if err != nil {
		tarResult.ErrorMessage = fmt.Sprintf("Could not open file %s for untarring: %v",
			tarFilePath, err)
		return tarResult
	}

	// Record the name of the top-level directory in the tar
	// file. Our spec says that the name of the directory into
	// which the file untars should be the same as the tar file
	// name, minus the .tar extension. So uva-123.tar should
	// untar into a directory called uva-123. This is required
	// so that IntellectualObject and GenericFile identifiers
	// in Fedora can be traced back to the named bag from which
	// they came. Other parts of bagman, such as the file cleanup
	// routines, assume that the untarred directory name will
	// match the tar file name, as the spec demands. When the names
	// do not match, the cleanup routines will not clean up the
	// untarred files, and we'll end up losing a lot of disk space.
	topLevelDir := ""

	// Untar the file and record the results.
	tarReader := tar.NewReader(file)

	for {
		header, err := tarReader.Next()
		if err != nil && err.Error() == "EOF" {
			break // end of archive
		}
		if err != nil {
			tarResult.ErrorMessage = fmt.Sprintf(
				"Error reading tar file header: %v. " +
					"Either this is not a tar file, or the file is corrupt.", err)
			return tarResult
		}

		// Top-level dir will be the first header entry.
		if header.Typeflag == tar.TypeDir && topLevelDir == "" {
			topLevelDir = strings.Replace(header.Name, "/", "", 1)
			// Fix for Windows
			systemNormalizedPath := tarFilePath
			if runtime.GOOS == "windows" && strings.Contains(tarFilePath, "\\") {
				systemNormalizedPath = strings.Replace(tarFilePath, "\\", "/", -1)
			}
			expectedDir := path.Base(systemNormalizedPath)
			if strings.HasSuffix(expectedDir, ".tar") {
				expectedDir = expectedDir[0 : len(expectedDir)-4]
			}
			if topLevelDir != expectedDir {
				tarResult.ErrorMessage = fmt.Sprintf(
					"Bag '%s' should untar to a folder named '%s', but "+
						"it untars to '%s'. Please repackage this bag and try again.",
					path.Base(tarFilePath), expectedDir, topLevelDir)
				return tarResult
			}
		}

		outputPath := filepath.Join(filepath.Dir(absInputFile), header.Name)
		// PT #114804083 - Make sure there's a slash in the path before setting
		// tarfile.OutputDir, or we'll set it incorrectly and when we call
		// bagman.ReadBag(tarResult.OutputDir), it will fail with "file not found".
		pathParts := strings.Split(header.Name, "/")
		tarDirectory := pathParts[0]
		if tarResult.OutputDir == "" && len(pathParts) > 1 {
			tarResult.OutputDir = filepath.Join(filepath.Dir(absInputFile), tarDirectory)
		}

		// Make sure the directory that we're about to write into exists.
		err = os.MkdirAll(filepath.Dir(outputPath), 0755)
		if err != nil {
			tarResult.ErrorMessage = fmt.Sprintf("Could not create destination file '%s' "+
				"while unpacking tar archive: %v", outputPath, err)
			return tarResult
		}

		// Copy the file, if it's an actual file. Otherwise, ignore it and record
		// a warning. The bag library does not deal with items like symlinks.
		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
			if strings.Contains(header.Name, "/data/") {
				var dataFile *File = nil
				dataFile = buildFile(tarReader, filepath.Dir(absInputFile), header.Name,
					header.Size, header.ModTime, buildIngestData)
				cleanBagName, _ := CleanBagName(bagName)
				dataFile.Identifier = fmt.Sprintf("%s/%s", cleanBagName, dataFile.Path)
				dataFile.IdentifierAssigned = time.Now()
				tarResult.Files = append(tarResult.Files, dataFile)
			} else {
				err = saveFile(outputPath, tarReader)
				if err != nil {
					tarResult.ErrorMessage = fmt.Sprintf("Error copying file from tar archive "+
						"to '%s': %v", outputPath, err)
					return tarResult
				}
			}

			outputRelativePath := strings.Replace(outputPath, tarResult.OutputDir+"/", "", 1)
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

// Reads an untarred bag. The tarFilePath parameter should be a path to
// a directory that contains the bag, info and manifest files.
// The bag content should be in the data directory under tarFilePath.
// Check result.Error to ensure there were no errors.
func ReadBag(tarFilePath string) (result *BagReadResult) {
	bagReadResult := new(BagReadResult)
	bagReadResult.Path = tarFilePath

	// Final param to bagins.ReadBag is the name of the checksum file.
	// That param defaults to manifest-md5.txt, which is what it
	// should be for bags we're fetching from the S3 receiving buckets.
	bag, err := bagins.ReadBag(tarFilePath, []string{"bagit.txt", "bag-info.txt", "aptrust-info.txt"}, "")
	if err != nil {
		bagReadResult.ErrorMessage = fmt.Sprintf("Error unpacking bag: %v", err)
		return bagReadResult
	}

	fileNames, err := bag.ListFiles()
	if err != nil {
		bagReadResult.ErrorMessage = fmt.Sprintf("Could not list bag files: %v ", err)
		return bagReadResult
	}

	dataDirPrefix := "data/"
	if runtime.GOOS == "windows" {
		dataDirPrefix = "data\\"
	}

	errMsg := ""
	bagReadResult.Files = make([]string, len(fileNames))
	hasBagit := false
	hasAPTrustInfo := false
	hasMd5Manifest := false
	hasDataFiles := false
	for index, fileName := range fileNames {
		bagReadResult.Files[index] = fileName
		if fileName == "bagit.txt" {
			hasBagit = true
		} else if fileName == "aptrust-info.txt" {
			hasAPTrustInfo = true
		} else if fileName == "manifest-md5.txt" {
			hasMd5Manifest = true
		} else if strings.HasPrefix(fileName, dataDirPrefix) {
			hasDataFiles = true
		}
	}
	if !hasBagit {
		errMsg += " Bag is missing bagit.txt file.\n"
	}
	if !hasAPTrustInfo {
		errMsg += " Bag is missing aptrust-info.txt file.\n"
	}
	if !hasMd5Manifest {
		errMsg += " Bag is missing manifest-md5.txt file.\n"
	}
	if !hasDataFiles {
		errMsg += " Bag's data directory is missing or empty.\n"
	}

	extractTags(bag, bagReadResult)

	checksumErrors := bag.Manifest.RunChecksums()
	if len(checksumErrors) > 0 {
		errMsg += "The following checksums could not be verified:\n"
		bagReadResult.ChecksumErrors = make([]error, len(checksumErrors))
		for i, err := range checksumErrors {
			bagReadResult.ChecksumErrors[i] = err
			errMsg += "  " + err.Error() + ".\n"
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
	bagTitle := ""
	for _, file := range tagFiles {
		tagFile, err := bag.TagFile(file)
		if err != nil {
			bagReadResult.ErrorMessage = fmt.Sprintf("Error reading tags from bag: %v", err)
			return
		}
		tagFields := tagFile.Data.Fields()

		for _, tagField := range tagFields {
			tag := Tag{tagField.Label(), strings.TrimSpace(tagField.Value())}
			bagReadResult.Tags = append(bagReadResult.Tags, tag)

			lcLabel := strings.ToLower(tag.Label)
			if lcLabel == "access" {
				accessRights = strings.TrimSpace(strings.ToLower(tag.Value))
			} else if accessRights == "" && lcLabel == "rights" {
				accessRights = strings.TrimSpace(strings.ToLower(tag.Value))
			} else if lcLabel == "title" {
				bagTitle = strings.TrimSpace(tag.Value)
			}
		}
	}

	// Make sure access rights are valid, or Fluctus will reject
	// this data when we try to register it.
	accessValid := false
	for _, value := range AccessRights {
		if accessRights == value {
			accessValid = true
		}
	}
	if false == accessValid {
		bagReadResult.ErrorMessage += fmt.Sprintf(
			"In tag file, access (rights) value '%s' is not valid.\n", accessRights)
	}

	// Fluctus will reject IntellectualObjects that don't have a title.
	if bagTitle == "" {
		bagReadResult.ErrorMessage +=
			"Required field Title is missing from tag file.\n"
	}
}

// Saves a file from the tar archive to local disk. This function
// used to save non-data files (manifests, tag files, etc.)
func saveFile(destination string, tarReader *tar.Reader) error {
	outputWriter, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY, 0644)
	if outputWriter != nil {
		defer outputWriter.Close()
	}
	if err != nil {
		return err
	}
	_, err = io.Copy(outputWriter, tarReader)
	if err != nil {
		return err
	}
	return nil
}

// buildFile saves a data file from the tar archive to disk,
// then returns a struct with data we'll need to construct the
// GenericFile object in Fedora later.
func buildFile(tarReader *tar.Reader, tarDirectory string, fileName string, size int64, modTime time.Time, buildIngestData bool) (file *File) {
	file = NewFile()
	file.Path = fileName[strings.Index(fileName, "/data/")+1 : len(fileName)]
	absPath, err := filepath.Abs(filepath.Join(tarDirectory, fileName))
	if err != nil {
		file.ErrorMessage = fmt.Sprintf("Path error: %v", err)
		return file
	}
	uuid := uuid.NewV4()
	file.Uuid = uuid.String()
	file.UuidGenerated = time.Now().UTC()
	file.Size = size
	file.Modified = modTime

	// Set up a MultiWriter to stream data ONCE to file,
	// md5 and sha256. We don't want to process the stream
	// three separate times.
	outputWriter, err := os.OpenFile(absPath, os.O_CREATE|os.O_WRONLY, 0644)
	if outputWriter != nil {
		defer outputWriter.Close()
	}
	if err != nil {
		file.ErrorMessage = fmt.Sprintf("Error opening writing to %s: %v", absPath, err)
		return file
	}

	// If we're just validating the bag format, don't bother
	// doing the checksums or figuring out mime-type.
	// Partners run our basic validators on their machines,
	// which probably don't even have the required MagicMime C
	// libraries. If we're doing full ingest, we'll calculate
	// the additional sums & take a guess at mime type.
	if buildIngestData == false {
		io.Copy(outputWriter, tarReader)
	} else {
		md5Hash := md5.New()
		shaHash := sha256.New()
		multiWriter := io.MultiWriter(md5Hash, shaHash, outputWriter)
		io.Copy(multiWriter, tarReader)

		file.Md5 = fmt.Sprintf("%x", md5Hash.Sum(nil))
		file.Sha256 = fmt.Sprintf("%x", shaHash.Sum(nil))
		file.Sha256Generated = time.Now().UTC()

		file.MimeType, err = GuessMimeType(absPath)
	}

	return file
}
