package dpn

import (
//	"github.com/APTrust/bagman/bagman"
//	"path/filepath"
//	"strings"
)

// StorageResult maintains information about the state of
// an attempt to store a DPN bag in AWS Glacier.
type StorageResult struct {

	// BagIdentifier is the APTrust bag identifier. If this is
	// a non-empty value, it means this bag came from APTrust,
	// and we will need to record a PREMIS event noting that
	// it was ingested into DPN. If the bag identifier is empty,
	// this bag came from somewhere else. We're just replicating
	// and we don't need to store a PREMIS event in Fluctus.
	BagIdentifier   string

	// UUID is the DPN identifier for this bag.
	UUID            string

	// The path to the bag, which is stored on disk as a tar file.
	TarFilePath     string

	// The URL of this file in Glacier. This will be empty until
	// we actually manage to store the file.
	StorageURL      string

	// A message describing what went wrong in the storage process.
	// If we have a StorageURL and ErrorMessage is empty,
	// storage succeeded.
	ErrorMessage    string

	// Should we try again to store this object? Usually, this is
	// true if we encounter network errors, false if there's some
	// fatal error, like TarFilePath cannot be found.
	Retry           bool
}
