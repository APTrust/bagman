package bagman

import (
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
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
