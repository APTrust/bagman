package bagman

import (
	"syscall"
)

// AvailableSpace returns the number of bytes available to unprivileged
// users on the volume that contains path.
func AvailableSpace(path string) (bytes uint64, err error) {
	stat := &syscall.Statfs_t{}
	err = syscall.Statfs(path, stat)
	if err != nil {
		return 0, err
	}
	available := uint64(stat.Bsize) * uint64(stat.Bavail)
	return available, nil
}
