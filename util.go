package bagman

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/fluctus/models"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

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

func LoadIntelObjFixture(filename string) (*models.IntellectualObject, error) {
	data, err := LoadRelativeFile(filename)
	if err != nil {
		return nil, err
	}
	intelObj := &models.IntellectualObject{}
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
func LoadEnv(path string) (error) {
	if FileExists(path) == false {
		return fmt.Errorf("File '%s' does not exist", path)
	}
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return err
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
		}
	}
	return nil
}

type SynchronizedMap struct {
	data  map[string]string
	mutex *sync.RWMutex
}

func NewSynchronizedMap() *SynchronizedMap {
	return &SynchronizedMap{
		data:  make(map[string]string),
		mutex: &sync.RWMutex{},
	}
}

func (syncMap *SynchronizedMap) HasKey(key string) bool {
	syncMap.mutex.RLock()
	_, hasKey := syncMap.data[key]
	syncMap.mutex.RUnlock()
	return hasKey
}

func (syncMap *SynchronizedMap) Add(key, value string) {
	syncMap.mutex.Lock()
	syncMap.data[key] = value
	syncMap.mutex.Unlock()
}

func (syncMap *SynchronizedMap) Get(key string) string {
	syncMap.mutex.RLock()
	value, _ := syncMap.data[key]
	syncMap.mutex.RUnlock()
	return value
}

func (syncMap *SynchronizedMap) Delete(key string) {
	syncMap.mutex.Lock()
	delete(syncMap.data, key)
	syncMap.mutex.Unlock()
}

func (syncMap *SynchronizedMap) Keys() []string {
	keys := make([]string, len(syncMap.data))
	syncMap.mutex.RLock()
	i := 0
	for key, _ := range syncMap.data {
		keys[i] = key
		i += 1
	}
	syncMap.mutex.RUnlock()
	return keys
}

func (syncMap *SynchronizedMap) Values() []string {
	vals := make([]string, len(syncMap.data))
	syncMap.mutex.RLock()
	i := 0
	for _, val := range syncMap.data {
		vals[i] = val
		i += 1
	}
	syncMap.mutex.RUnlock()
	return vals
}
