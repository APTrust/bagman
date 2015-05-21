package bagman

import (
	"archive/tar"
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/op/go-logging"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
)

// Returns the domain name of the institution that owns the specified bucket.
// For example, if bucketName is 'aptrust.receiving.unc.edu' the return value
// will be 'unc.edu'.
func OwnerOf(bucketName string) (institution string) {
	if strings.HasPrefix(bucketName, ReceiveTestBucketPrefix) {
		institution = strings.Replace(bucketName, ReceiveTestBucketPrefix, "", 1)
	} else if strings.HasPrefix(bucketName, ReceiveBucketPrefix) {
		institution = strings.Replace(bucketName, ReceiveBucketPrefix, "", 1)
	} else if strings.HasPrefix(bucketName, RestoreBucketPrefix) {
		institution = strings.Replace(bucketName, RestoreBucketPrefix, "", 1)
	}
	return institution
}

// Returns the name of the specified institution's restoration bucket.
// E.g. institution 'unc.edu' returns bucketName 'aptrust.restore.unc.edu'
func RestorationBucketFor(institution string) (bucketName string) {
	return RestoreBucketPrefix + institution
}

// Given the name of a tar file, returns the clean bag name. That's
// the tar file name minus the tar extension and any ".bagN.ofN" suffix.
func CleanBagName(bagName string) (string, error) {
	if len(bagName) < 5 {
		return "", fmt.Errorf("'%s' is not a valid tar file name", bagName)
	}
	// Strip the .tar suffix
	nameWithoutTar := bagName[0:len(bagName)-4]
	// Now get rid of the .b001.of200 suffix if this is a multi-part bag.
	cleanName := MultipartSuffix.ReplaceAll([]byte(nameWithoutTar), []byte(""))
	return string(cleanName), nil
}


// BagmanHome returns the absolute path to the bagman root directory,
// which contains source, config and test files. This will usually be
// something like /home/xxx/go/src/github.com/APTrust/bagman. You can
// set this explicitly by defining an environment variable called
// BAGMAN_HOME. Otherwise, this function will try to infer the value
// by appending to the environment variable GOPATH. If neither of
// those variables is set, this returns an error.
func BagmanHome() (bagmanHome string, err error) {
	bagmanHome = os.Getenv("BAGMAN_HOME")
	if bagmanHome == "" {
		goHome := os.Getenv("GOPATH")
		if goHome != "" {
			bagmanHome = filepath.Join(goHome, "src", "github.com", "APTrust", "bagman")
		} else {
			err = fmt.Errorf("Cannot determine bagman home because neither " +
				"BAGMAN_HOME nor GOPATH is set in environment.")
		}
	}
	if bagmanHome != "" {
		bagmanHome, err = filepath.Abs(bagmanHome)
	}
	return bagmanHome, err
}

// LoadRelativeFile reads the file at the specified path
// relative to BAGMAN_HOME and returns the contents as a byte array.
func LoadRelativeFile(relativePath string) ([]byte, error) {
	absPath, err := RelativeToAbsPath(relativePath)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadFile(absPath)
}

// Converts a relative path within the bagman directory tree
// to an absolute path.
func RelativeToAbsPath(relativePath string) (string, error) {
	bagmanHome, err := BagmanHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(bagmanHome, relativePath), nil
}

// Loads a result from the test data directory.
// This is used primarily for tests.
func LoadResult(filename string) (result *ProcessResult, err error) {
	data, err := LoadRelativeFile(filename)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Loads an IntellectualObject fixture (a JSON file) from
// the testdata directory for testing.
func LoadIntelObjFixture(filename string) (*IntellectualObject, error) {
	data, err := LoadRelativeFile(filename)
	if err != nil {
		return nil, err
	}
	intelObj := &IntellectualObject{}
	err = json.Unmarshal(data, intelObj)
	if err != nil {
		return nil, err
	}
	return intelObj, nil
}

// Returns true if the file at path exists, false if not.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

// Loads environment variables from the file at the specified
// absolute path. The variables are expected to be in the format
// typically seen in .bashrc and .bash_profile files:
//
// export VARNAME=VALUE
//
// with optional quotes. This function is here because supervisord
// doesn't provide an easy way of loading environment vars from
// an external file, and we have some sensitive environment vars
// that we want to keep in only one file on the system.
//
// Returns a map of the vars that were loaded from the file,
// and sets them in the program's environment.
func LoadEnv(path string) (vars map[string]string, err error) {
	vars = make(map[string]string)
	if path == "" {
		return vars, err
	}
	if FileExists(path) == false {
		return vars, fmt.Errorf("File '%s' does not exist", path)
	}
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return vars, err
	}
	reExport := regexp.MustCompile(`^export\s+(\w+)\s*=\s*(.*)`)
	data := string(bytes)
	lines := strings.Split(data, "\n")
	for i := range lines {
		line := strings.TrimSpace(lines[i])
		matches := reExport.FindAllStringSubmatch(line, -1)
		if matches != nil && len(matches) > 0 && len(matches[0]) > 2 {
			key := matches[0][1]
			value := strings.TrimSpace(strings.Trim(matches[0][2], "\" "))
			os.Setenv(key,value)
			vars[key] = value
		}
	}
	return vars, err
}

// Loads enviroment vars from a custom file or dies.
// If param customEnvFile is nil or points to an empty string,
// this loads nothing and proceeds without error. If customEnvFile
// specifies a file that does not exist or cannot be read, this
// causes the program to exit. Param logger is optional. Pass nil
// if you don't have a logger.
func LoadCustomEnvOrDie(customEnvFile *string, logger *logging.Logger) {
	if customEnvFile != nil && *customEnvFile != "" {
		vars, err := LoadEnv(*customEnvFile)
		if err != nil {
			message := fmt.Sprintf("Cannot load custom environment file '%s'. " +
				"Is that an absolute file path? Error: %v",
				*customEnvFile, err)
			if logger != nil {
				logger.Fatalf(message)
			}
			fmt.Fprintf(os.Stderr, message)
			os.Exit(1)
		} else {
			message := fmt.Sprintf("Loaded environment vars from '%s'", *customEnvFile)
			if logger != nil {
				logger.Info(message)
			}
			fmt.Println(message)
			for key, _ := range vars {
				if logger != nil {
					logger.Info("Loaded env var %s", key)
				}
				fmt.Printf("Loaded env var %s\n", key)
			}
		}
	}
}

// Min returns the minimum of x or y. The Math package has this function
// but you have to cast to floats.
func Min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

// Returns a base64-encoded md5 digest. The is the format S3 wants.
func Base64EncodeMd5(md5Digest string) (string, error) {
	// We'll get error if md5 contains non-hex characters. Catch
	// that below, when S3 tells us our md5 sum is invalid.
	md5Bytes, err := hex.DecodeString(md5Digest)
	if err != nil {
		detailedError := fmt.Errorf("Md5 sum '%s' contains invalid characters.",
			md5Digest)
		return "", detailedError
	}
	// Base64-encoded md5 sum suitable for sending to S3
	base64md5 := base64.StdEncoding.EncodeToString(md5Bytes)
	return base64md5, nil
}

// Returns true if url looks like a URL.
func LooksLikeURL(url string) (bool) {
	reUrl := regexp.MustCompile(`^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$`)
	return reUrl.Match([]byte(url))
}

func LooksLikeUUID(uuid string) (bool) {
	reUUID := regexp.MustCompile(`(?i)^([a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}?)$`)
	return reUUID.Match([]byte(uuid))
}

// QueueToNSQ sends data to NSQ. The URL param must be a valid NSQ
// URL. The data will be converted to JSON, with each object/record
// in a single line, then posted to url. This requires an NSQ server,
// so it's covered in the integration tests in the scripts directory.
func QueueToNSQ(url string, data []interface{}) (error) {
	jsonData := make([]string, len(data))
	for i, record := range data {
		json, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("Error marshalling record %d to JSON: %v", i + 1, err)
		} else {
			jsonData[i] = string(json)
		}
	}
	batch := strings.Join(jsonData, "\n")
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(batch)))
	if err != nil {
		return fmt.Errorf("nsqd returned an error: %v", err)
	}
	if resp == nil {
		return fmt.Errorf("No response from nsqd. Is it running? bucket_reader is quitting.")
	} else if resp.StatusCode != 200 {
		return fmt.Errorf("nsqd returned status code %d on last mput", resp.StatusCode)
	}
	return nil
}

// Expands the tilde in a directory path to the current
// user's home directory. For example, on Linux, ~/data
// would expand to something like /home/josie/data
func ExpandTilde(filePath string) (string, error) {
	if strings.Index(filePath, "~") < 0 {
		return filePath, nil
	}
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	homeDir := usr.HomeDir + "/"
	expandedDir := strings.Replace(filePath, "~/", homeDir, 1)
	return expandedDir, nil
}

// Cleans a string we might find a config file, trimming leading
// and trailing spaces, single quotes and double quoted. Note that
// leading and trailing spaces inside the quotes are not trimmed.
func CleanString(str string) (string) {
	cleanStr := strings.TrimSpace(str)
	// Strip leading and traling quotes, but only if string has matching
	// quotes at both ends.
	if strings.HasPrefix(cleanStr, "'") && strings.HasSuffix(cleanStr, "'") ||
		strings.HasPrefix(cleanStr, "\"") && strings.HasSuffix(cleanStr, "\"") {
		return cleanStr[1:len(cleanStr) - 1]
	}
	return cleanStr
}

// Given an S3 URI, returns the bucket name and key.
func BucketNameAndKey(uri string) (string, string) {
	relativeUri := strings.Replace(uri, S3UriPrefix, "", 1)
	parts := strings.SplitN(relativeUri, "/", 2)
	return parts[0], parts[1]
}

// Adds a file to a tar archive.
func AddToArchive(tarWriter *tar.Writer, filePath, pathWithinArchive string) (error) {
	finfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("Cannot add '%s' to archive: %v", filePath, err)
	}
	header := &tar.Header{
		Name: pathWithinArchive,
		Size: finfo.Size(),
		Mode: int64(finfo.Mode().Perm()),
		ModTime: finfo.ModTime(),
	}
	systat := finfo.Sys().(*syscall.Stat_t)
	if systat != nil {
		header.Uid = int(systat.Uid)
		header.Gid = int(systat.Gid)
	}

	// Write the header entry
	if err := tarWriter.WriteHeader(header); err != nil {
		return err
	}

	// Open the file whose data we're going to add.
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		return err
	}

	// Copy the contents of the file into the tarWriter.
	bytesWritten, err := io.Copy(tarWriter, file)
	if bytesWritten != header.Size {
		return fmt.Errorf("addToArchive() copied only %d of %d bytes for file %s",
			bytesWritten, header.Size, filePath)
	}
	if err != nil {
		return fmt.Errorf("Error copying %s into tar archive: %v",
			filePath, err)
	}

	return nil
}

// RecursiveFileList returns a list of all files in path dir
// and its subfolders. It does not return directories.
func RecursiveFileList(dir string) ([]string, error) {
    files := make([]string, 0)
    err := filepath.Walk(dir, func(filePath string, f os.FileInfo, err error) error {
		if f.IsDir() == false {
			files = append(files, filePath)
		}
        return nil
    })
	return files, err
}

type FileDigest struct {
	PathToFile     string
	Md5Digest      string
	Sha256Digest   string
	Size           int64
}

// Returns a FileDigest structure with the md5 and sha256 digests
// of the specified file as hex-enconded strings, along with the
// file's size.
func CalculateDigests(pathToFile string) (*FileDigest, error) {
	md5Hash := md5.New()
	shaHash := sha256.New()
	multiWriter := io.MultiWriter(md5Hash, shaHash)
	reader, err := os.Open(pathToFile)
	defer reader.Close()

	if err != nil {
		detailedError := fmt.Errorf("Error opening file '%s': %v", pathToFile, err)
		return nil, detailedError
	}
	fileInfo, err := reader.Stat()
	if err != nil {
		detailedError := fmt.Errorf("Cannot stat file '%s': %v", pathToFile, err)
		return nil, detailedError
	}
	// Calculate md5 and sha256 checksums in one read
	bytesWritten, err := io.Copy(multiWriter, reader)
	if err != nil {
		detailedError := fmt.Errorf("Error running md5 checksum on file '%s': %v",
			pathToFile, err)
		return nil, detailedError
	}
	if bytesWritten != fileInfo.Size() {
		detailedError := fmt.Errorf("Error running md5 checksum on file '%s': " +
			"read only %d of %d bytes.",
			pathToFile, bytesWritten, fileInfo.Size())
		return nil, detailedError
	}
	fileDigest := &FileDigest{
		PathToFile: pathToFile,
		Md5Digest: fmt.Sprintf("%x", md5Hash.Sum(nil)),
		Sha256Digest: fmt.Sprintf("%x", shaHash.Sum(nil)),
		Size: fileInfo.Size(),
	}
	return fileDigest, nil
}
