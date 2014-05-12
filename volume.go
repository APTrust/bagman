package bagman

import (
	"syscall"
	"sync"
	"errors"
)

type Volume struct {
	mutex            *sync.Mutex
	initialFree      uint64
	claimed          uint64
}

func NewVolume(path string) (*Volume, error) {
	stat := &syscall.Statfs_t{}
	err := syscall.Statfs(path, stat)
	if err != nil {
		return nil, err
	}
	volume := new(Volume)
	volume.mutex = &sync.Mutex{}
	volume.initialFree = uint64(stat.Bsize) * uint64(stat.Bavail)
	return volume, nil
}


func (volume *Volume) InitialFreeSpace() (numBytes uint64) {
	return volume.initialFree
}

func (volume *Volume) AvailableSpace() (uint64) {
	volume.mutex.Lock()
	available := volume.initialFree - volume.claimed
	volume.mutex.Unlock()
	return available
}

func (volume *Volume) Reserve(numBytes uint64) (err error) {
	volume.mutex.Lock()
	available := volume.initialFree - volume.claimed
	if numBytes >= available {
		err = errors.New("Disk does not have enough space to accomodate the requested number of bytes")
	} else  {
		volume.claimed += numBytes
	}
	volume.mutex.Unlock()
	return err
}

func (volume *Volume) Release(numBytes uint64) {
	volume.mutex.Lock()
	if numBytes > volume.claimed {
		volume.mutex.Unlock()
		panic("Volume.claimed should not be less than zero!")
	}
	volume.claimed = volume.claimed - numBytes
	volume.mutex.Unlock()
}
