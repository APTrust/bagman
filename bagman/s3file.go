package bagman

import (
	"fmt"
	"github.com/crowdmob/goamz/s3"
)


// S3File contains information about the S3 file we're
// trying to process from an intake bucket. BucketName
// and Key are the S3 bucket name and key. AttemptNumber
// describes whether this is the 1st, 2nd, 3rd,
// etc. attempt to process this file.
type S3File struct {
	BucketName string
	Key        s3.Key
}

// Returns the object identifier that will identify this bag
// in fedora. That's the institution identifier, followed by
// a slash and the tar file name, minus the .tar extension
// and the ".bag1of12" multipart extension. So for BucketName
// "aptrust.receiving.unc.edu" and Key.Key "nc_bag.001.of030.tar",
// this would return "unc.edu/nc_bag"
func (s3File *S3File) ObjectName() (string, error) {
	institution := OwnerOf(s3File.BucketName)
	cleanBagName, err := CleanBagName(s3File.Key.Key)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s", institution, cleanBagName), nil
}

// The name of the owning institution, followed by a slash, followed
// by the name of the tar file. This differs from the ObjectName,
// because it will have the .tar or bag.001.of030.tar suffix.
func (s3File *S3File) BagName() (string) {
	return fmt.Sprintf("%s/%s", OwnerOf(s3File.BucketName), s3File.Key.Key)
}
