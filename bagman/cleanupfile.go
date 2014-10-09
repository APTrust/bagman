package bagman

import (
	"time"
)

// CleanupFile represents a file (key) to be deleted
// from an S3 bucket.
type CleanupFile struct {
	// The name of the bucket that contains the key
	// we want to delete.
	BucketName string
	// The key to delete from S3.
	Key string
	// The error message, if the attempt
	// to delete the key resulted in an error.
	ErrorMessage string
	// The date and time at which the key/file
	// was successfully deleted from S3. If this
	// is zero time, file was not deleted. If it's
	// any other time, delete succeeded.
	DeletedAt time.Time
}

// Returns true if delete was attempted.
func (file *CleanupFile) DeleteAttempted() bool {
	return file.ErrorMessage != "" || file.DeletedAt.IsZero() == false
}
