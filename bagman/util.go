package bagman

import (
	"encoding/json"
	"fmt"
	"github.com/op/go-logging"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
	bagmanHome, err := BagmanHome()
	if err != nil {
		return nil, err
	}
	absPath := filepath.Join(bagmanHome, relativePath)
	return ioutil.ReadFile(absPath)
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
