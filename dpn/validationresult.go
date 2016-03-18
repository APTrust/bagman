package dpn

import (
	"archive/tar"
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)


// BAGIT_TAGS contains a list of tags required in the bagit file.
var BAGIT_TAGS = []string {
	"BagIt-Version",
	"Tag-File-Character-Encoding",
}

// BAG_INFO_TAGS contains a list of tags required in the bag-info file.
var BAG_INFO_TAGS = []string {
	"Source-Organization",
	"Organization-Address",
	"Contact-Name",
	"Contact-Phone",
	"Contact-Email",
	"Bagging-Date",
	"Bag-Size",
	"Bag-Group-Identifier",
	"Bag-Count",
}

// DPN_INFO_TAGS contains a list tags required in the dpn-info file.
var DPN_INFO_TAGS = []string {
	"DPN-Object-ID",
	"Local-ID",
	"Ingest-Node-Name",
	"Ingest-Node-Address",
	"Ingest-Node-Contact-Name",
	"Ingest-Node-Contact-Email",
	"Version-Number",
	"First-Version-Object-ID",
	// "Brightening-Object-ID",
	"Rights-Object-ID",
	"Bag-Type",
}

// TAGS_FOR_FILE maps a tag file to the list of tags it should contain.
var TAGS_FOR_FILE = map[string][]string {
	"bagit.txt": BAGIT_TAGS,
	"bag-info.txt": BAG_INFO_TAGS,
	filepath.Join("dpn-tags", "dpn-info.txt"): DPN_INFO_TAGS,
}

// TagFiles() returns a list of tag files we should check while
// performing validation.
func TagFiles() ([]string) {
	tagFiles := make([]string, 0)
	for key := range TAGS_FOR_FILE {
		tagFiles = append(tagFiles, key)
	}
	return tagFiles
}


// ValidationResult stores information about whether a DPN
// bag is valid.
type ValidationResult struct {
	// TarFilePath is the path to the tarred bag we'll be validating.
	TarFilePath          string

	// UntarredPath is the path to the untarred version of this bag.
	UntarredPath         string

	// The NSQ message we're currently working on. This will be nil
	// outside of production. In production, we need to touch the
	// message periodically to keep it from timing out, especially
	// on very large bags.
	NsqMessage           *nsq.Message    `json:"-"`

	// TagManifestChecksum is the sha256 digest (calculated with a nonce)
	// that we need to send back to the originating node as a receipt
	// when we're fulfilling replication requests. Outside of fulfilling
	// replication requests, we don't need to even calculate this value.
	TagManifestChecksum  string

	// ErrorMessages contains a list of everything that's wrong with the
	// bag. If this list is empty, the bag is valid.
	ErrorMessages        []string

	// Warning messages about non-fatal issues we might want to look into.
	Warnings             []string
}

func NewValidationResult(pathToFile string, nsqMessage *nsq.Message) (*ValidationResult, error) {
	absPath, err := filepath.Abs(pathToFile)
	if err != nil {
		return nil, fmt.Errorf("Cannot determine absolute path from '%s': %v",
			pathToFile, err)
	}
	var validator *ValidationResult
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("File does not exist at %s", absPath)
	}
	if strings.HasSuffix(absPath, ".tar") {
		validator = &ValidationResult{
			TarFilePath: absPath,
			NsqMessage: nsqMessage,
		}
	} else {
		validator = &ValidationResult{
			UntarredPath: absPath,
			NsqMessage: nsqMessage,
		}
	}
	return validator, nil
}

// IsValid() returns true if the bag is valid.
func (validator *ValidationResult) IsValid() (bool) {
	return len(validator.ErrorMessages) == 0
}

// AddError adds a message to the list of validation errors.
func (validator *ValidationResult) AddError(message string) () {
	validator.ErrorMessages = append(validator.ErrorMessages, message)
}

// AddWarning adds a message to the list of validation errors.
func (validator *ValidationResult) AddWarning(message string) () {
	validator.Warnings = append(validator.Warnings, message)
}

// PathToFileInBag returns the path the to file within a bag.
// If your bag is untarred to /mnt/data/my_bag and you call
// this function with param 'dpn-tags/dpn-info.txt', you'll
// get /mnt/data/my_bag/dpn-tags/dpn-info.txt
func (validator *ValidationResult) PathToFileInBag(relativePath string) (string) {
	return filepath.Join(validator.UntarredPath, relativePath)
}


// Run all validation checks on the bag.
func (validator *ValidationResult) ValidateBag()  {
	if validator.BagNameValid() == false {
		validator.AddError("Bag name is not valid. It should be a UUID.")
		return
	}
	if validator.NsqMessage != nil {
		validator.NsqMessage.Touch()
	}
	if validator.TarFilePath != "" && validator.UntarredPath == "" {
		if validator.untar() == false {
			return
		}
	}
	// Untar can take a long time on large bags.
	// Let NSQ know we're still working on it.
	if validator.NsqMessage != nil {
		validator.NsqMessage.Touch()
	}

	if validator.tagManifestPresent() == false {
		validator.AddError("Tag manifest file 'tagmanifest-sha256.txt' is missing.")
		return
	}

	if validator.sha256ManifestPresent() == false {
		validator.AddError("Manifest file 'manifest-sha256.txt' is missing.")
		return
	}
	// OK, the name is good, we untarred it and the tag manifest is valid.
	// Now do the heavy work... and there can be a lot to do on bags
	// over 100GB in size.
	bag, err := bagins.ReadBag(validator.UntarredPath, TagFiles())
	if err != nil {
		validator.AddError(fmt.Sprintf("Error unpacking bag: %v", err))
		return
	}

	fileNames, err := bag.ListFiles()
	if err != nil {
		validator.AddError(fmt.Sprintf("Could not list bag files: %v ", err))
	}

	dataDirPrefix := "data/"
	if runtime.GOOS == "windows" {
		dataDirPrefix = "data\\"
	}

	hasBagit := false
	hasDPNInfo := false
	hasManifest := false
	hasDataFiles := false
	for _, fileName := range fileNames {
		if fileName == "bagit.txt" {
			hasBagit = true
		} else if fileName == filepath.Join("dpn-tags", "dpn-info.txt") {
			hasDPNInfo = true
		} else if fileName == "manifest-sha256.txt" {
			hasManifest = true
		} else if strings.HasPrefix(fileName, dataDirPrefix) {
			hasDataFiles = true
		}
	}
	if !hasBagit {
		validator.AddError("Bag is missing bagit.txt file.")
	}
	if !hasDPNInfo {
		validator.AddError("Bag is missing dpn-info.txt file.")
	}
	if !hasManifest {
		validator.AddError("Bag is missing manifest-md5.txt file.")
	}
	if !hasDataFiles {
		validator.AddError("Bag's data directory is missing or empty.")
	}

	// Make sure the tag files have the required tags.
	// They can be empty, but they have to be present.
	validator.checkRequiredTags(bag)

	// Run all the checksums on all files.
	for _, manifest := range bag.Manifests {
		checksumErrors := manifest.RunChecksums()
		if len(checksumErrors) > 0 {
			for _, err := range checksumErrors {
				validator.AddError(fmt.Sprintf("In %s %s", manifest.Name(), err.Error()))
			}
		}
	}

	// Running the checksums takes a long time on
	// large bags, so let NSQ know we're still working.
	if validator.NsqMessage != nil {
		validator.NsqMessage.Touch()
	}
}

// Extract all of the tags from tag files "bagit.txt", "bag-info.txt",
// and "dpn-info.txt" and make sure the required tags are present.
// There may be other tag files, but since they're optional, we don't
// have to check their content.
func (validator *ValidationResult) checkRequiredTags(bag *bagins.Bag) {
	for _, file := range TagFiles() {
		tagFile, err := bag.TagFile(file)
		if err != nil {
			validator.AddError(fmt.Sprintf("Error reading tags from file '%s': %v", file, err))
			return
		}
		tagFields := tagFile.Data.Fields()
		requiredTags := TAGS_FOR_FILE[file]
		tagsFound := make(map[string]bool)
		for _, tagField := range tagFields {
			tagsFound[tagField.Label()] = true
		}
		for _, tagName := range requiredTags {
			if _, ok := tagsFound[tagName]; !ok {
				validator.AddError(fmt.Sprintf("Required tag '%s' is missing from %s", tagName, file))
			}
		}
	}
}

func (validator *ValidationResult) BagNameValid() (bool) {
	bagPath := validator.TarFilePath
	if bagPath == "" {
		bagPath = validator.UntarredPath
	}
	basename := strings.Replace(filepath.Base(bagPath), ".tar", "", 1)
	return bagman.LooksLikeUUID(basename)
}

// If the tag manifest is present, bagins will validate it.
// We have to make sure it's here, and bagins will do the rest.
func (validator *ValidationResult) tagManifestPresent() (bool) {
	fullPath := filepath.Join(validator.UntarredPath, "tagmanifest-sha256.txt")
	return bagman.FileExists(fullPath)
}

// bagins will validate only the manifests it finds.
// The DPN spec requires manifest-sha256.txt
func (validator *ValidationResult) sha256ManifestPresent() (bool) {
	fullPath := filepath.Join(validator.UntarredPath, "manifest-sha256.txt")
	return bagman.FileExists(fullPath)
}

func (validator *ValidationResult) CalculateTagManifestDigest(nonce string)  {
	filePath := validator.PathToFileInBag("tagmanifest-sha256.txt")
	src, err := os.Open(filePath)
	if err != nil {
		validator.AddError(fmt.Sprintf("Error reading tag manifest: %v", err))
	}
	defer src.Close()

	shaHash := sha256.New()
	_, err = io.Copy(shaHash, src)
	if err != nil {
		validator.AddError(fmt.Sprintf("Error calculating checksum on tag manifest: %v", err))
	}
	if nonce == "" {
		validator.TagManifestChecksum = fmt.Sprintf("%x", shaHash.Sum(nil))
	} else {
		validator.TagManifestChecksum = fmt.Sprintf("%x", shaHash.Sum([]byte(nonce)))
	}
}


func (validator *ValidationResult) untar() (bool) {
	absInputFile, err := filepath.Abs(validator.TarFilePath)
	if err != nil {
		validator.AddError(fmt.Sprintf("Before untarring, could not determine "+
			"absolute path to tar file: %v", err))
		return false
	}

	// Open the tar file for reading.
	file, err := os.Open(validator.TarFilePath)
	if file != nil {
		defer file.Close()
	}
	if err != nil {
		validator.AddError(fmt.Sprintf("Could not open file %s for untarring: %v",
			validator.TarFilePath, err))
		return false
	}

	// Untar the file and record the validators.
	tarReader := tar.NewReader(file)

	for {
		header, err := tarReader.Next()
		if err != nil && err.Error() == "EOF" {
			break // end of archive
		}
		if err != nil {
			validator.AddError(fmt.Sprintf(
				"Error reading tar file header: %v. " +
					"Either this is not a tar file, or the file is corrupt.", err))
			return false
		}

		// Set the untarred path, which will usually be the depositor's
		// bag identifier.
		if validator.UntarredPath == "" {
			nameParts := strings.Split(header.Name, string(os.PathSeparator))
			validator.UntarredPath = filepath.Join(filepath.Dir(absInputFile), nameParts[0])
		}

		outputPath := filepath.Join(filepath.Dir(absInputFile), header.Name)

		// Make sure the directory that we're about to write into exists.
		err = os.MkdirAll(filepath.Dir(outputPath), 0755)
		if err != nil {
			validator.AddError(fmt.Sprintf("Could not create destination file '%s' "+
				"while unpacking tar archive: %v", outputPath, err))
			return false
		}

		// Copy the file, if it's an actual file. Otherwise, ignore it and record
		// a warning. The bag library does not deal with items like symlinks.
		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
			validator.saveFile(outputPath, tarReader)
		} else if header.Typeflag != tar.TypeDir {
			validator.AddWarning(
				fmt.Sprintf("Ignoring item %s of type %c because it's neither a file nor a directory",
					header.Name, header.Typeflag))
		}
	}
	return true
}

func (validator *ValidationResult) saveFile (destination string, tarReader *tar.Reader) {
	outputWriter, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY, 0644)
	if outputWriter != nil {
		defer outputWriter.Close()
	}
	if err != nil {
		validator.AddError(fmt.Sprintf("Error opening file '%s': %v", destination, err))
		return
	}
	_, err = io.Copy(outputWriter, tarReader)
	if err != nil {
		validator.AddError(fmt.Sprintf("Error copying file to '%s': %v", destination, err))
	}
}

// We had to untar the bag to validate it, but once validation
// is done, all we need is the tarred bag, which we'll send to
// storage. Delete the untarred dir if we're not in test mode.
// We know we're in test mode if there's no validator.NsqMessage.
func (validator *ValidationResult) DeleteUntarredBag () {
	if validator.NsqMessage != nil && validator.UntarredPath != "" {
		//fmt.Println(validator.UntarredPath)
		os.RemoveAll(validator.UntarredPath)
	}
}
