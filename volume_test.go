package bagman_test

import (
	"testing"
	"runtime"
	"github.com/APTrust/bagman"
)


// Make sure we can find the number of available bytes
// on the current device. Var filename contains the full
// path to this source file.
func TestVolume(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	volume, err := bagman.NewVolume(filename)
	if err != nil {
		t.Errorf("Cannot get file system's available space: %v\n", err)
	}

	// Make sure we can reserve space that's actually there.
	initialSpace := volume.InitialFreeSpace()
	numBytes := initialSpace / 3
	err = volume.Reserve(numBytes)
	if err != nil {
		t.Errorf("Reserve rejected first reservation request: %v", err)
	}
	err = volume.Reserve(numBytes)
	if err != nil {
		t.Errorf("Reserve rejected second reservation request: %v", err)
	}

	// Make sure we're tracking the available space correctly.
	bytesAvailable := volume.AvailableSpace()
	expectedBytesAvailable := (initialSpace - (2 * numBytes))
	if bytesAvailable != expectedBytesAvailable {
		t.Errorf("Available space was calculated incorrectly after Reserve: was %d, expected %d",
			bytesAvailable, expectedBytesAvailable)
	}

	// Make sure a request for too much space is rejected
	err = volume.Reserve(numBytes * 2)
	if err == nil {
		t.Error("Reserve should have rejected third reservation request")
	}

	// Free the two chunks of space we just requested.
	volume.Release(numBytes)
	volume.Release(numBytes)

	// Make sure it was freed.
	if(volume.AvailableSpace() != volume.InitialFreeSpace()) {
		t.Errorf("Available space was calculated incorrectly after Release: was %d, expected %d",
			volume.AvailableSpace(), volume.InitialFreeSpace())
	}

	// Now we should have enough space for this.
	err = volume.Reserve(numBytes * 2)
	if err != nil {
		t.Error("Reserve rejected final reservation request: %v", err)
	}

}
