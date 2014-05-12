package bagman_test

import (
	"testing"
	"runtime"
	"github.com/APTrust/bagman"
)


// Make sure we can find the number of available bytes
// on the current device. Var filename contains the full
// path to this source file.
func TestAvailableSpace(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	_, err := bagman.AvailableSpace(filename)
	if err != nil {
		t.Error("Cannot get file system's available space: %v\n", err)
	}
}
