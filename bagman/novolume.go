// +build partners

// Dummy Volume struct and methods for Windows. We don't use
// the volume package on the partner apps, which is all that
// runs on Windows. But we need to have the structs and methods
// defined.
package bagman

import (
	"github.com/op/go-logging"
	"sync"
)

// Volume struct is not implemented for partner apps.
type Volume struct {
	path        string
	mutex       *sync.Mutex
	initialFree uint64
	claimed     uint64
	messageLog  *logging.Logger
}

// Dummy constructor. Does nothing useful.
func NewVolume(path string, messageLog *logging.Logger) (*Volume, error) {
	volume := new(Volume)
	return volume, nil
}

// Dummy method. Always returns zero.
func (volume *Volume) InitialFreeSpace() (numBytes uint64) {
	return uint64(0)
}

// Dummy method. Always returns zero.
func (volume *Volume) ClaimedSpace() (numBytes uint64) {
	return uint64(0)
}

// Dummy method. Always returns zero.
func (volume *Volume) currentFreeSpace() (numBytes uint64, err error) {
	return uint64(0), nil
}

// Dummy method. Always returns zero.
func (volume *Volume) AvailableSpace() (numBytes uint64) {
	return uint64(0)
}

// Dummy method. Always returns nil.
func (volume *Volume) Reserve(numBytes uint64) (err error) {
	return nil
}

// Dummy method. Does nothing at all.
func (volume *Volume) Release(numBytes uint64) {

}
