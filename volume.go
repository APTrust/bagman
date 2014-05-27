package bagman

import (
	"syscall"
	"sync"
	"errors"
)

// Volume tracks the amount of available space on a volume (disk),
// as well as the amount of space claimed for pending operations.
// The purpose is to allow the bag processor to try to determine
// ahead of time whether the underlying disk has enough space to
// accomodate the file it just pulled off the queue. We want to
// avoid downloading 100GB files when we know ahead of time that
// we don't have enough space to process them.
type Volume struct {
	path             string
	mutex            *sync.Mutex
	initialFree      uint64
	claimed          uint64
}

// NewVolume creates a new Volume structure to track the amount
// of available space and claimed space on a volume (disk).
func NewVolume(path string) (*Volume, error) {
	volume := new(Volume)
	volume.mutex = &sync.Mutex{}
	volume.path = path
	volume.claimed = 0
	initialFree, err := volume.currentFreeSpace()
	if err != nil {
		return nil, err
	}
	volume.initialFree = initialFree
	return volume, nil
}

// InitialFreeSpace returns the number of bytes available to an
// unprivileged user on the volume at the time the Volume struct
// was initialized.
func (volume *Volume) InitialFreeSpace() (numBytes uint64) {
	return volume.initialFree
}

// Claimed space returns the number of bytes reserved for pending
// operations, including downloading and untarring bag archives.
func (volume *Volume) ClaimedSpace() (numBytes uint64) {
	return volume.claimed
}

// currentFreeSpace returns the number of bytes currently available
// to unprivileged users on the underlying volume. This number comes
// directly from the operating system's statfs call, and does not
// take into account the number of bytes reserved for pending operations.
func (volume *Volume)currentFreeSpace() (numBytes uint64, err error) {
	stat := &syscall.Statfs_t{}
	err = syscall.Statfs(volume.path, stat)
	if err != nil {
		return 0, err
	}
	freeBytes := uint64(stat.Bsize) * uint64(stat.Bavail)
	return freeBytes, nil
}

// AvailableSpace returns an approximate number of free bytes currently
// available to unprivileged users on the underlying volume, minus the
// number of bytes reserved for pending processes. The value returned
// will never be 100% accurate, because other processes may be writing
// to the volume.
func (volume *Volume) AvailableSpace() (numBytes uint64) {
	volume.mutex.Lock()
	numBytes = volume.initialFree - volume.claimed
	volume.mutex.Unlock()
	return numBytes
}

// Reserve requests that a number of bytes on disk be reserved for an
// upcoming operation, such as downloading and untarring a file.
// Reserving space does not have any effect on the file system. It
// simply allows the Volume struct to maintain some internal bookkeeping.
// Reserve will return an error if there is not enough free disk space to
// accomodate the requested number of bytes.
func (volume *Volume) Reserve(numBytes uint64) (err error) {
	available := volume.AvailableSpace()
	if numBytes >= available {
		err = errors.New("Volume does not have enough space to " +
			"accomodate the requested number of bytes")
	} else  {
		volume.mutex.Lock()
		volume.claimed += numBytes
		volume.mutex.Unlock()
	}
	return err
}

// Release tells the Volume struct that numBytes have been deleted from
// the underlying volume and are free to be reused later.
func (volume *Volume) Release(numBytes uint64) {
	if numBytes > volume.claimed {
		panic("Volume.claimed should not be less than zero!")
	}
	volume.mutex.Lock()
	volume.claimed = volume.claimed - numBytes
	volume.mutex.Unlock()
}