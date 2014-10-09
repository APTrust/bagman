package bagman

import (
	"github.com/crowdmob/goamz/s3"
)

// BucketSummary contains information about an S3 bucket and its contents.
type BucketSummary struct {
	BucketName string
	Keys       []s3.Key // TODO: Change to slice of pointers!
}
