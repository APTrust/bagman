// PartnerS3Client is used by apt_upload, which APTrust partners
// use to upload bags to the S3 receiving buckets.
package bagman

import (
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"os"
	"path/filepath"
	"strings"
)

type PartnerS3Client struct {
	PartnerConfig  *PartnerConfig
	S3Client       *S3Client
	LogVerbose     bool
	Test           bool  // used only in testing to suppress output
}

// Returns a new PartnerS3Client object. Will return an error
// if the config file is missing, unreadable or incomplete.
func NewPartnerS3ClientFromConfigFile(configFile string, logVerbose bool) (*PartnerS3Client, error) {
	client := &PartnerS3Client{
		LogVerbose: logVerbose,
	}
	err := client.LoadConfig(configFile)
	if err != nil {
		return nil, err
	}
	err = client.initS3Client()
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Returns a new PartnerS3Client object with the specified
// configuration.
func NewPartnerS3ClientWithConfig(partnerConfig *PartnerConfig, logVerbose bool) (*PartnerS3Client, error) {
	client := &PartnerS3Client{
		PartnerConfig: partnerConfig,
		LogVerbose: logVerbose,
	}
	err := client.PartnerConfig.Validate()
	if err != nil {
		return nil, err
	}
	err = client.initS3Client()
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (client *PartnerS3Client) initS3Client() (error) {
	s3Client, err := NewS3ClientExplicitAuth(aws.USEast,
		client.PartnerConfig.AwsAccessKeyId,
		client.PartnerConfig.AwsSecretAccessKey)
	if err != nil {
		return fmt.Errorf("Cannot init S3 client: %v\n", err)
	}
	client.S3Client = s3Client
	return nil
}

// Loads configuration from the specified file path.
func (client *PartnerS3Client) LoadConfig(configFile string) (error) {
	partnerConfig, err := LoadPartnerConfig(configFile)
	if err != nil {
		return err
	}
	client.PartnerConfig = partnerConfig
	err = client.PartnerConfig.Validate()
	return err
}

// Uploads all files in filePaths to the S3 receiving bucket.
// Returns the number of uploads that succeeded and the number
// that failed.
func (client *PartnerS3Client) UploadFiles(filePaths []string) (succeeded, failed int) {
	if filePaths == nil || len(filePaths) == 0 {
		if client.LogVerbose && !client.Test {
			fmt.Println("[INFO]  There are no files in the upload list.")
		}
		return
	}
	succeeded = 0
	failed = 0
	for _, filePath := range(filePaths) {
		file, err := os.Open(filePath)
		if err != nil {
			if !client.Test {
				fmt.Printf("[ERROR] Cannot read local file '%s': %v\n", filePath, err)
			}
			failed++
			continue
		}
		fileStat, err := file.Stat()
		if err == nil && fileStat.IsDir() {
			if client.LogVerbose && !client.Test {
				fmt.Printf("[INFO]  Skipping %s because it's a directory.\n", file.Name())
			}
			continue
		}
		etag, err := client.UploadFile(file)
		if err != nil {
			if !client.Test {
				fmt.Printf("[ERROR] Uploading '%s' to S3: %v\n", filePath, err)
			}
			failed++
			continue
		}
		if !client.Test {
			fmt.Printf("[OK]    S3 returned md5 checksum %s for %s\n", etag, filePath)
		}
		succeeded++
	}
	if !client.Test {
		fmt.Printf("Finished uploading. %d succeeded, %d failed.\n", succeeded, failed)
	}
	return succeeded, failed
}

// Uploads a single file to the S3 receiving bucket.
// Returns S3's checksum for the file, or an error.
// Note that S3 checksums for large multi-part uploads
// will not be normal md5 checksums.
func (client *PartnerS3Client) UploadFile(file *os.File) (string, error) {
	fileStat, err := file.Stat()
	if err != nil {
		return "", err
	}
	bucketName := client.PartnerConfig.ReceivingBucket
	fileName := filepath.Base(fileStat.Name())
	mimeType := "application/x-tar"
	url := ""
	if fileStat.Size() < S3_LARGE_FILE {
		if client.LogVerbose && !client.Test {
			fmt.Printf("[INFO]  Sending %s to %s in single-part put\n", fileName, bucketName)
		}
		url, err = client.S3Client.SaveToS3(
			bucketName,
			fileName,
			mimeType,
			file,
			fileStat.Size(),
			s3.Options{})
	} else {
		if client.LogVerbose && !client.Test {
			fmt.Printf("[INFO]  Sending %s to %s in multi-part put because size is %d\n",
				fileName, bucketName, fileStat.Size())
		}
		url, err = client.S3Client.SaveLargeFileToS3(
			bucketName,
			fileName,
			mimeType,
			file,
			fileStat.Size(),
			s3.Options{},
			S3_CHUNK_SIZE)
	}
	if err != nil {
		return "", err
	}
	if client.LogVerbose && !client.Test {
		fmt.Printf("[INFO]  File %s saved to %s\n", fileName, url)
	}

	httpResponse, err := client.S3Client.Head(bucketName, fileName)
	if err != nil {
		return "", fmt.Errorf("File %s was uploaded to S3, but cannot get md5 receipt. Try re-uploading.",
			fileStat.Name())
	}
	etag := ""
	etags := httpResponse.Header["Etag"]
	if etags != nil && len(etags) > 0 {
		etag = strings.Replace(etags[0], "\"", "", -1)
	}
	return etag, nil
}

// Downloads a file from the S3 restoration bucket and saves it
// in the directory specified by PartnerConfig.DownloadDir.
// Param checksum may be "md5", "sha256" or "none". This returns
// the md5 or sha256 checksum of the downloaded file, or an empty
// string if the checksum param was "none". Returns an error if
// any occurred.
func (client *PartnerS3Client) DownloadFile(bucketName, key, checksum string) (string, error) {
	localPath := filepath.Join(client.PartnerConfig.DownloadDir, key)
	if checksum == "md5" {
		s3Key, err := client.S3Client.GetKey(bucketName, key)
		if err != nil {
			return "", err
		}
		fetchResult := client.S3Client.FetchToFile(bucketName, *s3Key, localPath)
		if fetchResult.ErrorMessage != "" {
			return "", fmt.Errorf(fetchResult.ErrorMessage)
		}
		return fetchResult.LocalMd5, nil
	} else if checksum == "sha256" {
		// This is unfortunate, but this particular function was
		// written for running fixity checks, not for partner use.
		genericFile := &GenericFile{
			URI: fmt.Sprintf("https://s3.amazonaws.com/%s/%s", bucketName, key),
		}
		fixityResult := &FixityResult{ GenericFile: genericFile }
		err := client.S3Client.FetchAndCalculateSha256(fixityResult, localPath)
		if err != nil {
			return "", err
		}
		return fixityResult.Sha256, nil
	} else if checksum == "none" {
		err := client.S3Client.FetchToFileWithoutChecksum(bucketName, key, localPath)
		if err != nil {
			return "", err
		}
		return "", nil
	}
	return "", fmt.Errorf("checksum param '%s' is invalid. Use 'md5', 'sha256' or 'none'", checksum)
}

// Lists up the contents of a bucket, return up to limit number
// of entries.
func (client *PartnerS3Client) List(bucketName string, limit int) (keys []s3.Key, err error) {
	return client.S3Client.ListBucket(bucketName, limit)
}

// Deletes the specified file from the specified bucket.
func (client *PartnerS3Client) Delete(bucketName, fileName string) (error) {
	return client.S3Client.Delete(bucketName, fileName)
}
