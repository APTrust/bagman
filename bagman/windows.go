// +build partners

package bagman

import (
	"archive/tar"
	"os"
)

// We implement this call for Unix/Linux/Mac in nix.go.
// Windows does not implement the syscall.Stat_t type we
// need, and our partners may be using Windows. The partners
// won't be doing any tarring with the partner tools, so we
// make this a no-op.
func GetOwnerAndGroup(finfo os.FileInfo, header *tar.Header) {
	return
}
