package dpn

import (
	"archive/tar"
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/bagman"
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
	"First-Node-Name",
	"First-Node-Address",
	"First-Node-Contact-Name",
	"First-Node-Contact-Email",
	"Version-Number",
	"Previous-Version-Object-ID",
	"First-Version-Object-ID",
	"Brightening-Object-ID",
	"Rights-Object-ID",
	"Object-Type",
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


// Validator stores information about whether a DPN
// bag is valid.
type Validator struct {
	// TarFilePath is the path to the tarred bag we'll be validating.
	TarFilePath          string

	// UntarredPath is the path to the untarred version of this bag.
	UntarredPath         string

	// TagManifestChecksum is the sha256 digest (calculated with a nonce)
	// that we need to send back to the originating node as a receipt
	// when we're fulfilling replication requests. Outside of fulfilling
	// replication requests, we don't need to even calculate this value.
	TagManifestChecksum  string

	// Nonce value to use when calculating the TagManifestChecksum. This
	// may be an empty string.
	ChecksumNonce        string

	// ErrorMessages contains a list of everything that's wrong with the
	// bag. If this list is empty, the bag is valid.
	ErrorMessages        []string

	// Warning messages about non-fatal issues we might want to look into.
	Warnings             []string
}

func NewValidator(pathToFile string) (*Validator, error) {
	absPath, err := filepath.Abs(pathToFile)
	if err != nil {
		return nil, fmt.Errorf("Cannot determine absolute path from '%s': %v",
			pathToFile, err)
	}
	var validator *Validator
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("File does not exist at %s", absPath)
	}
	if strings.HasSuffix(absPath, ".tar") {
		validator = &Validator{
			TarFilePath: absPath,
		}
	} else {
		validator = &Validator{
			UntarredPath: absPath,
		}
	}
	return validator, nil
}

// IsValid() returns true if the bag is valid.
func (validator *Validator) IsValid() (bool) {
	return len(validator.ErrorMessages) == 0
}

// AddError adds a message to the list of validation errors.
func (validator *Validator) AddError(message string) () {
	validator.ErrorMessages = append(validator.ErrorMessages, message)
}

// AddWarning adds a message to the list of validation errors.
func (validator *Validator) AddWarning(message string) () {
	validator.Warnings = append(validator.Warnings, message)
}

// PathToFileInBag returns the path the to file within a bag.
// If your bag is untarred to /mnt/data/my_bag and you call
// this function with param 'dpn-tags/dpn-info.txt', you'll
// get /mnt/data/my_bag/dpn-tags/dpn-info.txt
func (validator *Validator) PathToFileInBag(relativePath string) (string) {
	return filepath.Join(validator.UntarredPath, relativePath)
}


// Run all validation checks on the bag.
func (validator *Validator) ValidateBag()  {
	if validator.BagNameValid() == false {
		validator.AddError("Bag name is not valid. It should be a UUID.")
		return
	}
	if validator.TarFilePath != "" && validator.UntarredPath == "" {
		if validator.untar() == false {
			return
		}
	}
	if validator.validateTagManifest() == false {
		return
	}

	// OK, the name is good, we untarred it and the tag manifest is valid.
	// Now do the heavy work... and there can be a lot to do on bags
	// over 100GB in size.
	bag, err := bagins.ReadBag(validator.UntarredPath, TagFiles(), "manifest-sha256.txt")
	if err != nil {
		validator.AddError(fmt.Sprintf("Error unpacking bag: %v", err))
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

	validator.checkRequiredTags(bag)

	checksumErrors := bag.Manifest.RunChecksums()
	for _, err := range checksumErrors {
		validator.AddError(err.Error())
	}
}

// Extract all of the tags from tag files "bagit.txt", "bag-info.txt",
// and "dpn-info.txt" and make sure the required tags are present.
// There may be other tag files, but since they're optional, we don't
// have to check their content.
func (validator *Validator) checkRequiredTags(bag *bagins.Bag) {
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
				validator.AddError(fmt.Sprintf("Required tag '%s' is missing from %f", tagName, file))
			}
		}
	}
}

func (validator *Validator) BagNameValid() (bool) {
	bagPath := validator.TarFilePath
	if bagPath == "" {
		bagPath = validator.UntarredPath
	}
	basename := strings.Replace(filepath.Base(bagPath), ".tar", "", 1)
	//DEBUG
	fmt.Printf("'%s'\n",basename)
	return bagman.LooksLikeUUID(basename)
}

func (validator *Validator) validateTagManifest() (bool) {
	valid := true
	manifest, errors := bagins.ReadManifest(validator.PathToFileInBag("tagmanifest-sha256.txt"))
	if errors != nil {
		for i := range errors {
			validator.AddError(errors[i].Error())
			valid = false
		}
	}
	errors = manifest.RunChecksums()
	if errors != nil {
		for i := range errors {
			validator.AddError(errors[i].Error())
			valid = false
		}
	}
	return valid
}

func (validator *Validator) CalculateTagManifestDigest(nonce string)  {
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


func (validator *Validator) untar() (bool) {
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

		// Top-level dir will be the first header entry.
		if header.Typeflag == tar.TypeDir && validator.UntarredPath == "" {
			validator.UntarredPath = filepath.Join(filepath.Dir(absInputFile), header.Name)
			// DEBUG
			fmt.Println("***** UNTARRED PATH *****", validator.UntarredPath)
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

func (validator *Validator) saveFile (destination string, tarReader *tar.Reader) {
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

func (validator *Validator) DeleteUntarredBag () {
	fmt.Println(validator.UntarredPath)
	//os.RemoveAll(validator.UntarredPath)
}
