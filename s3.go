package bagman

import (
	"fmt"
	"io"
	"os"
	"strings"
	"crypto/md5"
	"errors"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)


// Returns an S3 client for the specified region, using
// AWS credentials from the environment. Please keep your AWS
// keys out of the source code repos! Store them somewhere
// else and load them into environment variables AWS_ACCESS_KEY_ID
// and AWS_SECRET_ACCESS_KEY.
func GetClient(region aws.Region) (*s3.S3, error) {
	auth, err := aws.EnvAuth()
	if err != nil {
		return nil, err
	}
	return s3.New(auth, region), nil
}


// Returns a list of keys in the specified bucket.
// If limit is zero, this will return all the keys in the bucket;
// otherwise, it will return only the number of keys specifed.
// Note that listing all keys may result in the underlying client
// issuing multiple requests.
func ListBucket(bucket *s3.Bucket, limit int) (keys []s3.Key, err error) {
	actualLimit := limit
	if limit == 0 {
		actualLimit = 1000
	}
	bucketList, err := bucket.List("", "/", "", actualLimit)
	if err != nil {
		return nil, err
	}
	contents := bucketList.Contents
	if len(contents) == 0 {
		return contents, nil
	}
	for limit == 0 {
		lastKey := contents[len(contents) - 1].Key
		bucketList, err := bucket.List("", "/", lastKey, actualLimit)
		if err != nil {
			return nil, err
		}
		contents = append(contents, bucketList.Contents ...)
		if !bucketList.IsTruncated {
			break
		}
	}
	return contents, nil
}

// Fetches key from bucket and saves it to path.
// This validates the md5 sum of the byte stream before
// saving to disk. If the md5 sum of the downloaded bytes
// does not match the md5 sum in the key, this will not
// save the file. It will just return an error.
func FetchToFile(bucket *s3.Bucket, key s3.Key, path string) (fetchResult *FetchResult) {
	result := new(FetchResult)
	result.BucketName = bucket.Name
	result.Key = key.Key
	result.LocalTarFile = path

	// In general, we want to retry if the fetch operation
	// fails. We will override this in certain cases below.
	result.Retry = true

	// S3 etag is md5 hex string enclosed in quotes,
	// unless file was a multipart upload. See below for that.
	result.RemoteMd5 = strings.Replace(key.ETag, "\"", "", -1)

	// Fetch the file into a reader instead of using the usual bucket.Get().
	// Files may be up to 250GB, so we want to process them as streams.
	// If we get an error here, it's typically a network error, and we
	// will want to retry later.
	readCloser, err := bucket.GetReader(key.Key)
	if err != nil {
		result.Error = err
		if strings.Contains(err.Error(), "key does not exist") {
			result.Retry = false
		}
		return result
	}
	defer readCloser.Close()

	// Write the contents of the stream into both our md5 hasher
	// and the file.
	md5Hash := md5.New()
	outputFile, err := os.Create(path)
	if err != nil {
		result.Error = err
		return result
	}
	defer outputFile.Close()

	multiWriter := io.MultiWriter(outputFile, md5Hash)
	bytesWritten, err := io.Copy(multiWriter, readCloser)
	if err != nil {
		result.Error = err
		return result
	}
	if bytesWritten != key.Size {
		result.Error = fmt.Errorf("Wrote only %d of %d bytes for %s", bytesWritten, key.Size, key.Key)
		return result
	}

	result.LocalMd5 = fmt.Sprintf("%x", md5Hash.Sum(nil))

	// ETag for S3 multi-part upload is not an accurate md5 sum.
	// If the ETag ends with a dash and some number, it's a
	// multi-part upload.
	if strings.Contains(result.RemoteMd5, "-") {
		result.Warning = fmt.Sprintf("Skipping md5 check on %s: this was a multi-part upload", key.Key)
		result.Md5Verified = false
		result.Md5Verifiable = false
	} else {
		result.Md5Verifiable = true
		result.Md5Verified = true
		if result.LocalMd5 != result.RemoteMd5 {
			os.Remove(path)
			result.Error = fmt.Errorf("Our md5 sum '%x' does not match the S3 md5 sum '%s'",
				result.LocalMd5, result.RemoteMd5)
			result.Md5Verified = false
			// Don't bother reprocessing this item.
			result.Retry = false
		}
	}
	return result
}

// Collects info about all of the buckets listed in buckets.
// TODO: Write unit test
func CheckAllBuckets(buckets []string) (bucketSummaries []*BucketSummary, err error) {
	bucketSummaries = make([]*BucketSummary, 0)
	for _, bucketName := range(buckets) {
		bucketSummary, err := CheckBucket(bucketName)
		if err != nil {
			return bucketSummaries, err
		}
		bucketSummaries = append(bucketSummaries, bucketSummary)
	}
	return bucketSummaries, nil
}

// Returns info about the contents of the bucket named bucketName.
// BucketSummary contains the bucket name, a list of keys, and the
// size of the largest file in the bucket.
// TODO: Write unit test
func CheckBucket(bucketName string) (bucketSummary *BucketSummary, err error) {
	client, err := GetClient(aws.USEast)
	if err != nil {
		return nil, err
	}
	bucket := client.Bucket(bucketName)
	if bucket == nil {
		err = errors.New(fmt.Sprintf("Cannot retrieve bucket: %s", bucketName))
		return nil, err
	}
	bucketSummary = new(BucketSummary)
	bucketSummary.BucketName = bucketName
	bucketSummary.Keys, err = ListBucket(bucket, 0)
	if err != nil {
		return nil, err
	}
	return bucketSummary, nil
}
