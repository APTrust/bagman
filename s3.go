package bagman

import (
    "fmt"
    "io"
    "os"
    "strings"
    "crypto/md5"
	"github.com/diamondap/goamz/aws"
	"github.com/diamondap/goamz/s3"
)


type S3Client struct {
    S3    *s3.S3
}

// Returns an S3Client for the specified region, using
// AWS credentials from the environment. Please keep your AWS
// keys out of the source code repos! Store them somewhere
// else and load them into environment variables AWS_ACCESS_KEY_ID
// and AWS_SECRET_ACCESS_KEY.
func NewS3Client(region aws.Region) (*S3Client, error) {
    auth, err := aws.EnvAuth()
    if err != nil {
        return nil, err
    }
    S3 := s3.New(auth, region)
    return &S3Client{S3}, nil
}


// Returns a list of keys in the specified bucket.
// If limit is zero, this will return all the keys in the bucket;
// otherwise, it will return only the number of keys specifed.
// Note that listing all keys may result in the underlying client
// issuing multiple requests.
func (client *S3Client)ListBucket(bucketName string, limit int) (keys []s3.Key, err error) {
    bucket := client.S3.Bucket(bucketName)
    if bucket == nil {
        err = fmt.Errorf("Cannot retrieve bucket: %s", bucketName)
        return nil, err
    }
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
func (client *S3Client)FetchToFile(bucketName string, key s3.Key, path string) (fetchResult *FetchResult) {
    bucket := client.S3.Bucket(bucketName)
    result := new(FetchResult)
    result.BucketName = bucketName
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
    if readCloser != nil {
        defer readCloser.Close()
    }
    if err != nil {
        result.ErrorMessage = fmt.Sprintf("Error retrieving file from receiving bucket: %v", err)
        if strings.Contains(err.Error(), "key does not exist") {
            result.Retry = false
        }
        return result
    }

    // Write the contents of the stream into both our md5 hasher
    // and the file.
    md5Hash := md5.New()
    outputFile, err := os.Create(path)
    if outputFile != nil {
        defer outputFile.Close()
    }
    if err != nil {
        result.ErrorMessage = fmt.Sprintf("Could not create local file %s: %v", path, err)
        return result
    }

    multiWriter := io.MultiWriter(outputFile, md5Hash)
    bytesWritten, err := io.Copy(multiWriter, readCloser)
    if err != nil {
        result.ErrorMessage = fmt.Sprintf("Error copying file from receiving bucket: %v", err)
        return result
    }
    if bytesWritten != key.Size {
        result.ErrorMessage = fmt.Sprintf("While downloading from receiving bucket, " +
            "copied only %d of %d bytes for %s", bytesWritten, key.Size, key.Key)
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
            result.ErrorMessage = fmt.Sprintf("Our md5 sum '%x' does not match the S3 md5 sum '%s'",
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
func (client *S3Client) CheckAllBuckets(buckets []string) (bucketSummaries []*BucketSummary, err error) {
    bucketSummaries = make([]*BucketSummary, 0)
    for _, bucketName := range(buckets) {
        bucketSummary, err := client.CheckBucket(bucketName)
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
func (client *S3Client) CheckBucket(bucketName string) (bucketSummary *BucketSummary, err error) {
    bucket := client.S3.Bucket(bucketName)
    if bucket == nil {
        err = fmt.Errorf("Cannot retrieve bucket: %s", bucketName)
        return nil, err
    }
    bucketSummary = new(BucketSummary)
    bucketSummary.BucketName = bucketName
    bucketSummary.Keys, err = client.ListBucket(bucketName, 0)
    if err != nil {
        return nil, err
    }
    return bucketSummary, nil
}

// Creates an options struct that adds metadata headers to the S3 put.
func (client *S3Client) MakeOptions(md5sum string, metadata map[string][]string) (s3.Options) {
	if md5sum != "" {
		return s3.Options{
			ContentMD5: md5sum,
			Meta: metadata,
		}
	} else {
		return s3.Options{
			Meta: metadata,
		}
	}
}

// Saves a file to S3 with default access of Private.
// The underlying S3 client does not return the md5 checksum
// from s3, but we already have this info elsewhere. If the
// PUT produces no error, we assume the copy worked and the
// files md5 sum is the same on S3 as here.
func (client *S3Client) SaveToS3(bucketName, fileName, contentType string, reader io.Reader, byteCount int64, options s3.Options) (url string, err error) {
    bucket := client.S3.Bucket(bucketName)
    putErr := bucket.PutReader(fileName, reader, byteCount,
		contentType, s3.Private, options)
    if putErr != nil {
        err = fmt.Errorf("Error saving file '%s' to bucket '%s': %v",
            fileName, bucketName, putErr)
		return "", err
    }
	url = fmt.Sprintf("https://s3.amazonaws.com/%s/%s", bucketName, fileName)
    return url, nil
}

// Returns an S3 key object for the specified file in the
// specified bucket. The key object has the ETag, last mod
// date, size and other useful info.
func (client *S3Client) GetKey(bucketName, fileName string) (*s3.Key, error) {
    bucket := client.S3.Bucket(bucketName)
    listResp, err := bucket.List(fileName, "", "", 1)
    if err != nil {
        err = fmt.Errorf("Error checking key '%s' in bucket '%s': '%v'",
            fileName, bucketName, err)
        return nil, err
    }
    if listResp == nil || len(listResp.Contents) < 1 {
        err = fmt.Errorf("Key '%s' not found in bucket '%s'",
            fileName, bucketName)
        return nil, err
    }
    return &listResp.Contents[0], nil
}


// Deletes an item from S3
func (client *S3Client) Delete(bucketName, fileName string) (error) {
    bucket := client.S3.Bucket(bucketName)
    return bucket.Del(fileName)
}


// Sends a large file (>= 5GB) to S3 in 200MB chunks. This operation
// may take several minutes to complete. Note that os.File satisfies
// the s3.ReaderAtSeeker interface.
func (client *S3Client) SaveLargeFileToS3(bucketName, fileName, contentType string,
	reader s3.ReaderAtSeeker, byteCount int64, options s3.Options, chunkSize int64) (url string, err error) {

    bucket := client.S3.Bucket(bucketName)
	multipartPut, err := bucket.InitMulti(fileName, contentType, s3.Private, options)
	if err != nil {
		return "", err
	}

	// Send all of the individual parts to S3 in chunks
	parts, err := multipartPut.PutAll(reader, chunkSize)
	if err != nil {
		return "", err
	}

	// This command tells S3 to stitch all the parts into a single file.
	err = multipartPut.Complete(parts)
	if err != nil {
		return "", err
	}

	resp, err := bucket.Head(fileName, nil)
	if err != nil {
		return "", fmt.Errorf("Files were uploaded to S3, but attempt to " +
			"confirm metadata returned this error: %v", err)
	}

	// Make sure all the meta data made it there.
	// Var metadata is the metadata we sent to S3.
	metadata := options.Meta
	notVerified := ""

	if !metadataMatches(metadata, "institution", resp.Header, "X-Amz-Meta-Institution") {
		notVerified += "institution, "
	}
	if !metadataMatches(metadata, "bag", resp.Header, "X-Amz-Meta-Bag") {
		notVerified += "bag, "
	}
	if !metadataMatches(metadata, "bagpath", resp.Header, "X-Amz-Meta-Bagpath") {
		notVerified += "bagpath, "
	}
	if !metadataMatches(metadata, "md5", resp.Header, "X-Amz-Meta-Md5") {
		notVerified += "md5"
	}
	if len(notVerified) > 0 {
		return "", fmt.Errorf("Multi-part upload succeeded, but S3 does not return " +
			"the following metadata: %s", notVerified)
	}

	url = fmt.Sprintf("https://s3.amazonaws.com/%s/%s", bucketName, fileName)
    return url, nil
}

func metadataMatches(metadata map[string][]string, key string, s3headers map[string][]string, headerName string)(bool) {
	metaValue, keyExists := metadata[key]
	headerValue, headerExists := s3headers[headerName]

	// If we didn't send this metadata in the first place, we
	// don't care if S3 has it.
	if !keyExists {
		return true
	}

	// If we sent the metadata, test whether S3 returned
	// what we sent.
	if keyExists && len(metaValue) > 0 && headerExists && len(headerValue) > 0 {
		return metaValue[0] == headerValue[0]
	}

	// If we get here, the key exists in the metadata we
	// sent, but not in the S3 headers.
	return false
}
