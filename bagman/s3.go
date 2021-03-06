package bagman

import (
	"crypto/md5"
	"crypto/sha256"
	"hash"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// Constants
const (
	// A Gigabyte!
	GIGABYTE int64 = int64(1024 * 1024 * 1024)

	// Files over 5GB in size must be uploaded via multi-part put.
	S3_LARGE_FILE int64 = int64(5 * GIGABYTE)

	// Chunk size for multipart puts to S3: ~500 MB
	S3_CHUNK_SIZE = int64(500000000)
)

type S3Client struct {
	S3 *s3.S3
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
	s3Client := s3.New(auth, region)
	return &S3Client{S3: s3Client}, nil
}

// Returns an S3 client from specific auth credentials,
// instead of reading credentials from the environment.
func NewS3ClientExplicitAuth(region aws.Region, accessKey, secretKey string) (*S3Client, error) {
	auth := aws.Auth {
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
	s3Client := s3.New(auth, region)
	return &S3Client{S3: s3Client}, nil
}

// Returns a list of keys in the specified bucket.
// If limit is zero, this will return all the keys in the bucket;
// otherwise, it will return only the number of keys specifed.
// Note that listing all keys may result in the underlying client
// issuing multiple requests.
func (client *S3Client) ListBucket(bucketName string, limit int) (keys []s3.Key, err error) {
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
		lastKey := contents[len(contents)-1].Key
		bucketList, err := bucket.List("", "/", lastKey, actualLimit)
		if err != nil {
			return nil, err
		}
		contents = append(contents, bucketList.Contents...)
		if !bucketList.IsTruncated {
			break
		}
	}
	return contents, nil
}

// This fetches the file from S3, but does **not** save it
// unless you specify a localPath, in which case the file
// will be saved into the file specified by localPath.
//
// Without localPath, this simply calculates the sha256 digest of
// the stream thatS3 returns.  Keep in mind that the remote file
// may be up to 250GB, so this call can run for several hours and
// use a lot of CPU.
//
// Returns a FixityResult object that includes not only the
// checksum, but also some information about what went wrong
// and whether the operation should be retried.
func (client *S3Client) FetchAndCalculateSha256(fixityResult *FixityResult, localPath string) (error) {
	if fixityResult == nil {
		return fmt.Errorf("Param fixityResult cannot be nil")
	}
	if fixityResult.GenericFile == nil {
		return fmt.Errorf("FixityResult.GenericFile cannot be nil")
	}
	bucketName, key, err := fixityResult.BucketAndKey()
	if err != nil {
		// GenericFile URI is invalid. FixityResult sets its
		// own error message in this case.
		fixityResult.Retry = false
		return fmt.Errorf(fixityResult.ErrorMessage)
	}
	bucket := client.S3.Bucket(bucketName)

	// Get a read for this here file. We occasionally get
	// "connection reset by peer" on some larger files, so
	// we build in a few retries. This is the source of a
	// lot of headaches, since network errors often occur
	// 249GB into the download. That sets us back a few hours.
	var readCloser io.ReadCloser = nil
	for attemptNumber := 0; attemptNumber < 5; attemptNumber++ {
		readCloser, err = bucket.GetReader(key)
		if err == nil {
			break  // we got a reader, so move on
		}
	}
	if readCloser != nil {
		defer readCloser.Close()
	}
	// Oh no! Can't fetch the file!
	if err != nil {
		fixityResult.ErrorMessage = fmt.Sprintf("Error retrieving file from receiving bucket: %v", err)
		if strings.Contains(err.Error(), "key does not exist") {
			fixityResult.S3FileExists = false
			fixityResult.Retry = false
		}
		return fmt.Errorf(fixityResult.ErrorMessage)
	}

	fixityResult.S3FileExists = true
	shaHash := sha256.New()

	var multiWriter io.Writer

	// For fixity check, we don't save the file.
	// For partners using apt_download, we do save it.
	if localPath != "" {
		// Make sure download dir exists
		err = os.MkdirAll(filepath.Dir(localPath), 0755)
		if err != nil {
			return fmt.Errorf(
				"Could not create directory %s to download file into: %v",
				filepath.Dir(localPath), err)
		}

		// Open the output file for writing
		outputFile, err := os.Create(localPath)
		if outputFile != nil {
			defer outputFile.Close()
		}
		if err != nil {
			return fmt.Errorf("Could not create local file %s: %v", localPath, err)
		}
		multiWriter = io.MultiWriter(shaHash, outputFile)
	} else {
		// Just calculate Sha256 & discard the bytes
		multiWriter = io.MultiWriter(shaHash)
	}


	_, err = io.Copy(multiWriter, readCloser)
	if err != nil {
		fixityResult.ErrorMessage = fmt.Sprintf(
			"Error calculating SHA256 checksum from S3 data stream: %v", err)
		// Probably a network error, so retry later.
		fixityResult.Retry = true
		return fmt.Errorf(fixityResult.ErrorMessage)
	}
	fixityResult.Sha256 = fmt.Sprintf("%x", shaHash.Sum(nil))
	return nil
}

// Fetches key from bucket and saves it to path.
// This validates the md5 sum of the byte stream before
// saving to disk. If the md5 sum of the downloaded bytes
// does not match the md5 sum in the key, this will not
// save the file. It will just return an error.
//
// This method is primarily intended for fetching tar
// files from the receiving buckets. It calculates the
// file's Md5 checksum as it writes it to disk.
func (client *S3Client) FetchToFile(bucketName string, key s3.Key, path string) (fetchResult *FetchResult) {
	bucket := client.S3.Bucket(bucketName)
	result := new(FetchResult)
	result.BucketName = bucketName
	result.Key = key.Key
	result.LocalFile = path

	// In general, we want to retry if the fetch operation
	// fails. We will override this in certain cases below.
	result.Retry = true

	// S3 etag is md5 hex string enclosed in quotes,
	// unless file was a multipart upload. See below for that.
	result.RemoteMd5 = strings.Replace(key.ETag, "\"", "", -1)

	// Fetch the file into a reader instead of using the usual bucket.Get().
	// Files may be up to 250GB, so we want to process them as streams.
	// If we get an error here, it's typically a network error, and we
	// will want to retry later. Try up to 5 times to download the file.
	var readCloser io.ReadCloser = nil
	var err error = nil
	for attemptNumber := 0; attemptNumber < 5; attemptNumber++ {
		readCloser, err = bucket.GetReader(key.Key)
		if err == nil {
			break
		}
	}
	if readCloser != nil {
		defer readCloser.Close()
	}
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Error retrieving file %s/%s: %v", bucketName, key.Key, err)
		if strings.Contains(err.Error(), "key does not exist") {
			result.Retry = false
		}
		return result
	}

	// Make sure download dir exists
	err = os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf(
			"Could not create directory %s to download file into: %v",
			filepath.Dir(path), err)
		return result
	}

	// Open the output file for writing
	outputFile, err := os.Create(path)
	if outputFile != nil {
		defer outputFile.Close()
	}
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Could not create local file %s: %v", path, err)
		return result
	}

	// If this is a huge file, the e-tag will include a dash,
	// indicating it was a multi-part upload, and we can't do
	// our standard md5 check on it. We don't want to anyway
	// for files >5GB, since it eats up too much CPU and we're
	// going to validate the md5 checksums of its individual
	// generic files later.
	var md5Hash hash.Hash = nil
	var multiWriter io.Writer = nil
	if strings.Contains(result.RemoteMd5, "-") {
		multiWriter = io.MultiWriter(outputFile)
	} else {
		md5Hash = md5.New()
		multiWriter = io.MultiWriter(outputFile, md5Hash)
	}

	bytesWritten := int64(0)
	for attemptNumber := 0; attemptNumber < 5; attemptNumber++ {
		bytesWritten, err = io.Copy(multiWriter, readCloser)
		if err == nil {
			break
		}
	}

	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Error copying file from receiving bucket: %v", err)
		return result
	}
	if bytesWritten != key.Size {
		result.ErrorMessage = fmt.Sprintf("While downloading from receiving bucket, "+
			"copied only %d of %d bytes for %s", bytesWritten, key.Size, key.Key)
		return result
	}

	// ETag for S3 multi-part upload is not an accurate md5 sum.
	// If the ETag ends with a dash and some number, it's a
	// multi-part upload.
	if md5Hash == nil {
		result.Warning = fmt.Sprintf("Skipping md5 check on %s: this was a multi-part upload", key.Key)
		result.Md5Verified = false
		result.Md5Verifiable = false
	} else {
		result.LocalMd5 = fmt.Sprintf("%x", md5Hash.Sum(nil))
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


// Fetches the specified S3 URL and saves it in the specified localPath.
// Ensures that the directory containing localPath exists, and calculates
// an md5 checksum on download. The FetchResult will tell you whether the
// md5 matched what AWS said it should be. You'll get an error if url is
// not an S3 url, or if it doesn't exist. Check FetchResult.ErrorMessage.
func (client *S3Client) FetchURLToFile(url, localPath string) (*FetchResult) {
	bucketName, key := BucketNameAndKey(url)
	s3Key, err := client.GetKey(bucketName, key)
	if err != nil {
		errMsg := fmt.Sprintf("Could not get key info for %s: %v", url, err)
		return &FetchResult {
			ErrorMessage: errMsg,
		}
	}
	return client.FetchToFile(bucketName, *s3Key, localPath)
}

// Fetches file key from bucketName and saves it to localPath.
// The file will be at localPath when this is done, unless it
// returns an error. This is a simple fetch method that does
// none of the accounting required for ingest.
func (client *S3Client) FetchToFileWithoutChecksum(bucketName, key, localPath string) (error) {
	// Get a reader for the S3 file
	bucket := client.S3.Bucket(bucketName)
	var readCloser io.ReadCloser = nil
	var err error = nil
	for attemptNumber := 0; attemptNumber < 5; attemptNumber++ {
		readCloser, err = bucket.GetReader(key)
		if err == nil {
			break
		}
	}
	if readCloser != nil {
		defer readCloser.Close()
	}
	if err != nil {
		return fmt.Errorf("Error retrieving file from %s/%s: %v", bucketName, key, err)
	}

	// Make sure download dir exists
	err = os.MkdirAll(filepath.Dir(localPath), 0755)
	if err != nil {
		return fmt.Errorf(
			"Could not create directory %s to download file into: %v",
			filepath.Dir(localPath), err)
	}

	// Open the local file for writing
	outputFile, err := os.Create(localPath)
	if outputFile != nil {
		defer outputFile.Close()
	}
	if err != nil {
		return fmt.Errorf("Could not create local file %s: %v", localPath, err)
	}

	// Copy the data
	_, err = io.Copy(outputFile, readCloser)
	if err != nil {
		return fmt.Errorf("Error while copying %s from S3 to %s: %v", key, localPath, err)
	}

	return nil
}

// Collects info about all of the buckets listed in buckets.
// TODO: Write unit test
func (client *S3Client) CheckAllBuckets(buckets []string) (bucketSummaries []*BucketSummary, errors []error) {
	bucketSummaries = make([]*BucketSummary, 0)
	errors = make([]error, 0)
	for _, bucketName := range buckets {
		bucketSummary, err := client.CheckBucket(bucketName)
		if err != nil {
			errors = append(errors, fmt.Errorf("%s: %v", bucketName, err))
		} else {
			bucketSummaries = append(bucketSummaries, bucketSummary)
		}
	}
	return bucketSummaries, errors
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
func (client *S3Client) MakeOptions(md5sum string, metadata map[string][]string) s3.Options {
	if md5sum != "" {
		return s3.Options{
			ContentMD5: md5sum,
			Meta:       metadata,
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
func (client *S3Client) Delete(bucketName, fileName string) error {
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
		abortErr := multipartPut.Abort()
		if abortErr != nil {
			return "", fmt.Errorf("Multipart put failed with error %v "+
				"while uploading a part and abort failed with error %v. "+
				"YOU WILL BE CHARGED FOR THESE FILE PARTS UNTIL YOU DELETE THEM! "+
				"Use multi.ListMulti in the S3 package to list orphaned parts.",
				err, abortErr)
		}
		return "", err
	}

	// This command tells S3 to stitch all the parts into a single file.
	err = multipartPut.Complete(parts)
	if err != nil {
		abortErr := multipartPut.Abort()
		if abortErr != nil {
			return "", fmt.Errorf("Multipart put failed in 'complete' stage "+
				"with error %v and abort failed with error %v",
				err, abortErr)
		}
		return "", err
	}

	resp, err := bucket.Head(fileName, nil)
	if err != nil {
		return "", fmt.Errorf("Files were uploaded to S3, but attempt to "+
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
		return "", fmt.Errorf("Multi-part upload succeeded, but S3 does not return "+
			"the following metadata: %s", notVerified)
	}

	url = fmt.Sprintf("https://s3.amazonaws.com/%s/%s", bucketName, fileName)
	return url, nil
}

// Returns true/false indicating whether a bucket exists.
func (client *S3Client) Exists(bucketName, key string) (bool, error) {
	bucket := client.S3.Bucket(bucketName)
	return bucket.Exists(key)
}

// Returns a reader that lets you read data from bucket/key.
func (client *S3Client) GetReader(bucketName, key string) (io.ReadCloser, error) {
	bucket := client.S3.Bucket(bucketName)
	return bucket.GetReader(key)
}

// Performs a HEAD request on an S3 object and returns the response.
// Check the response status code. You may get a 401 or 403 for files
// that don't exist, and the body will be an XML error message.
func (client *S3Client) Head(bucketName, key string) (*http.Response, error) {
	bucket := client.S3.Bucket(bucketName)
	return bucket.Head(key, nil)
}

func metadataMatches(metadata map[string][]string, key string, s3headers map[string][]string, headerName string) bool {
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
