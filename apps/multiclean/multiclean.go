/*
multiclean.go cleans up fragments from failed S3 multipart-uploads.

For normal cleanup of ingested tar files from the partner's receiving
buckets, see cleanup.go.

Usage:

  go run multiclean.go -config=dev
  go run multiclean.go -config=dev -delete

The first example just lists all of the unrecoverable parts of failed
multipart uploads and prints the total number of unreachable bytes
we're paying for.

The second version actually deletes the unrecoverable bytes.

Be careful using -delete when the bag processor is running, because
you may wind up deleting parts of a multipart upload that the bag
processor is actively uploading. To be safe, use the -delete flag
only when you know the bag processor is not running.
*/
package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"os"
)

func main() {
	requestedConfig := flag.String("config", "", "configuration to run")
	deleteParts := flag.Bool("delete", false, "delete stray parts of multipart uploads")
	flag.Parse()
	procUtil := bagman.NewProcessUtil(requestedConfig)

	s3Client := procUtil.S3Client.S3
	bucket := s3Client.Bucket(procUtil.Config.PreservationBucket)
	multis, _, err := bucket.ListMulti("", "")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	totalBytes := int64(0)
	bytesDeleted := int64(0)

	for _, m := range multis {
		bytesInThisMulti := int64(0)
		parts, err := m.ListParts()
		if err != nil {
			fmt.Printf("Can't get part details for %s: %v\n", m.Key, err)
		}
		if parts != nil {
			for _, part := range parts {
				fmt.Println(m.Key, "part", part.N, "is", part.Size, "bytes")
				totalBytes += int64(part.Size)
				bytesInThisMulti += int64(part.Size)
			}
		}

		if *deleteParts == true {
			err = m.Abort()
			if err != nil {
				fmt.Printf("Error deleting %s: %v\n", m.Key, err)
			} else {
				fmt.Printf("Deleted %s\n", m.Key)
				bytesDeleted += bytesInThisMulti
			}
		}
	}

	fmt.Println("_____________________________________________________")
	fmt.Printf("Total unrecoverable bytes: %d\n", totalBytes)
	if *deleteParts == true {
		fmt.Printf("Bytes deleted: %d\n", bytesDeleted)
	}
}
