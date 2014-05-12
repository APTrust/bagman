package bagman_test

import (
	"testing"
	"github.com/APTrust/bagman"
)


func TestAvailableSpace(t *testing.T) {
	_, err := bagman.AvailableSpace("/")
	if err != nil {
		t.Error("Cannot get file system's available space: %v\n", err)
	}
}
