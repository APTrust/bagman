package main

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/crowdmob/goamz/aws"
	"io"
	"os"
)

func main() {
	client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		message := fmt.Sprintf("Exiting. Cannot init S3 client: %v", err)
		fmt.Fprintln(os.Stderr, message)
		os.Exit(1)
	}
	bucketName := os.Args[1]
	key := os.Args[2]
	localFile := os.Args[3]
	if bucketName == "" || key == "" || localFile == "" {
		fmt.Println("downloader.go: downloads a (private) S3 file to local storage")
		fmt.Println("Usage: go run downloader.go <bucket_name> <key> <local_file>")
	}
	fmt.Printf("Fetching %s/%s to %s...\n", bucketName, key, localFile)

	bucket := client.S3.Bucket(bucketName)
	readCloser, err := bucket.GetReader(key)
	if readCloser != nil {
		defer readCloser.Close()
	}
	if err != nil {
		fmt.Printf("Error retrieving file from receiving bucket: %v\n", err)
		os.Exit(1)
	}

	outputFile, err := os.Create(localFile)
	if outputFile != nil {
		defer outputFile.Close()
	}
	if err != nil {
		fmt.Printf("Could not create local file %s: %v\n", localFile, err)
		os.Exit(1)
	}

	bytesWritten, err := io.Copy(outputFile, readCloser)
	if err != nil {
		fmt.Printf("Error copying file from receiving bucket: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Copied %d bytes to %s\n", bytesWritten, localFile)
}
