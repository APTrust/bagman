// validator.go contains functions to allow partner institutions
// to validate bags before they send them. This code is run by
// users at our partner institutions, on their desktops and laptops.
// It's not intended to run on APTrust servers.
package bagman

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	// Item to validate is a tar file
	VAL_TYPE_TAR = 1
	// Item to validate is a directory
	VAL_TYPE_DIR = 2
	// Item is something we can't validate
	VAL_TYPE_ERR = 3
)

type Validator struct {
	PathToFile     string
	TarResult      *TarResult
	BagReadResult  *BagReadResult
	ErrorMessage   string
}

// Returns a new Validator suitable for partners to validate
// bags before sending. For server-side use, use IngestHelper.
func NewValidator(pathToFile string) (*Validator, error) {
	absPath, err := filepath.Abs(pathToFile)
	if err != nil {
		return nil, fmt.Errorf("Cannot determine absolute path from '%s': %v",
			pathToFile, err)
	}
	return  &Validator{
		PathToFile: absPath,
	}, nil
}

func (validator *Validator) IsValid() (bool) {
	domain, err := validator.InstitutionDomain()
	if err != nil {
		validator.ErrorMessage = err.Error()
		return false
	}
	if validator.LooksLikeMultipart() && !validator.IsValidMultipartName() {
		validator.ErrorMessage = "This looks like a multipart bag, but it does not conform to " +
			"naming conventions. Multipart bags should end with a suffix like '.b01.of12.tar'. " +
			"See the APTrust BagIt specification for details."
		return false
	}
	fileType, err := validator.FileType()
	if err != nil {
		validator.ErrorMessage = err.Error()
		return false
	}

	untarredDirExisted := FileExists(validator.UntarredDir())
	weUntarredThisFile := false
	if fileType == VAL_TYPE_TAR {
		validator.TarResult = Untar(validator.PathToFile, domain,
			validator.TarFileName(), false)
		if validator.TarResult.ErrorMessage != "" {
			if untarredDirExisted == false {
				// Untar failed, but we just created a directory and possibly
				// several files inside it. We don't want to leave a bunch of
				// trash hanging around, so clean up!
				os.RemoveAll(validator.UntarredDir())
			}
			validator.ErrorMessage = validator.TarResult.ErrorMessage
			return false
		}
		weUntarredThisFile = true
	}


	validator.BagReadResult = ReadBag(validator.UntarredDir())
	if weUntarredThisFile == true && untarredDirExisted == false {
		// Clean up the files we untarred.
		os.RemoveAll(validator.UntarredDir())
	}
	if validator.BagReadResult.ErrorMessage != "" {
		validator.ErrorMessage = validator.BagReadResult.ErrorMessage
		// Augment some of the more vague messages from the underlying
		// bag parsing library.
		if strings.Contains(validator.BagReadResult.ErrorMessage, "Unable to parse a manifest") {
			validator.ErrorMessage = "Required checksum file manifest-md5.txt is missing."
		} else if strings.Contains(validator.BagReadResult.ErrorMessage, "Payload directory does not exist") {
			validator.ErrorMessage = "Bag is missing the data directory, which should contain the payload files."
		}
		return false
	}
	return true
}

// Returns the path to the directory that holds the untarred
// contents of the bag.
func (validator *Validator) UntarredDir() (string) {
	re := regexp.MustCompile("\\.tar$")
	return re.ReplaceAllString(validator.PathToFile, "")
}

// Get the instution domain from the file, or return a descriptive
// error if the file doesn't include the institution name.
func (validator *Validator) InstitutionDomain() (string, error) {
	if validator.PathToFile == "" {
		return "", fmt.Errorf("You must specify the tar file or directory to validate.")
	}
	base := filepath.Base(validator.PathToFile)
	parts := strings.Split(base, ".")
	if len(parts) < 3 || len(parts) == 3 && parts[2] == "tar" {
		message := fmt.Sprintf(
			"Bag name '%s' should start with your institution's " +
				"domain name,\n followed by a period and the object name.\n" +
				"For example, 'university.edu.my_archive.tar' " +
				"for a tar file,\n" +
				"or 'university.edu.my_archive' for a directory.",
			base)
		return "", fmt.Errorf(message)
	}
	instName := fmt.Sprintf("%s.%s", parts[0], parts[1])
	return instName, nil
}

// Returns true if the bag name looks like a multipart bag.
// This catches both correct multipart bag names and some
// common incorrect variants, such as "bag1of2"
func (validator *Validator) LooksLikeMultipart() (bool) {
	reMisnamedMultiPartBag := regexp.MustCompile(`\.b\d+\.?of\d+$|\.bag\d+\.?of\d+$`)
	return reMisnamedMultiPartBag.MatchString(validator.UntarredDir())
}

// Returns true if the bag has a valid multipart bag name.
func (validator *Validator) IsValidMultipartName() (bool) {
	_, err := validator.InstitutionDomain()
	return err == nil && MultipartSuffix.MatchString(validator.UntarredDir())
}


// Returns the name of the tar file that the user wants to validate.
// If this is a directory, returns the name of the directory with a
// .tar suffix.
func (validator *Validator) TarFileName() (string) {
	base := filepath.Base(validator.PathToFile)
	if !strings.HasSuffix(base, ".tar") {
		base += ".tar"
	}
	return base
}

// Returns either VAL_TYPE_TAR, VAL_TYPE_DIR or VAL_TYPE_ERR
// to describe what type of item the user wants to validate.
func (validator *Validator) FileType() (int, error) {
	if validator.PathToFile == "" {
		return VAL_TYPE_ERR, fmt.Errorf("You must specify the tar file or directory to validate.")
	}
	f, err := os.Open(validator.PathToFile)
	if err != nil {
		return VAL_TYPE_ERR, err
	}
	fileInfo, err := f.Stat()
    if err != nil {
		return VAL_TYPE_ERR, err
    }
	mode := fileInfo.Mode()
	if mode.IsDir() {
		return VAL_TYPE_DIR, nil
	}
	base := filepath.Base(validator.PathToFile)
	if strings.HasSuffix(base, ".tar") {
		return VAL_TYPE_TAR, nil
	}
	return VAL_TYPE_ERR, fmt.Errorf(
		"Bag '%s' must be either a tar file or a directory",
		validator.PathToFile)
}
