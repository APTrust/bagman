// PartnerUpload is used by apt_upload, which APTrust partners
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

type PartnerUpload struct {
	PartnerConfig  *PartnerConfig
	S3Client       *S3Client
	LogVerbose     bool
	Test           bool  // used only in testing to suppress output
}

// Returns a new PartnerUpload object. Will return an error
// if the config file is missing, unreadable or incomplete.
func NewPartnerUploadFromConfigFile(configFile string, logVerbose bool) (*PartnerUpload, error) {
	partnerUpload := &PartnerUpload{
		LogVerbose: logVerbose,
	}
	err := partnerUpload.LoadConfig(configFile)
	if err != nil {
		return nil, err
	}
	err = partnerUpload.initS3Client()
	if err != nil {
		return nil, err
	}
	return partnerUpload, nil
}

// Returns a new PartnerUpload object with the specified
// configuration.
func NewPartnerUploadWithConfig(partnerConfig *PartnerConfig, logVerbose bool) (*PartnerUpload, error) {
	partnerUpload := &PartnerUpload{
		PartnerConfig: partnerConfig,
		LogVerbose: logVerbose,
	}
	err := partnerUpload.ValidateConfig()
	if err != nil {
		return nil, err
	}
	err = partnerUpload.initS3Client()
	if err != nil {
		return nil, err
	}
	return partnerUpload, nil
}

func (partnerUpload *PartnerUpload) initS3Client() (error) {
	s3Client, err := NewS3ClientExplicitAuth(aws.USEast,
		partnerUpload.PartnerConfig.AwsAccessKeyId,
		partnerUpload.PartnerConfig.AwsSecretAccessKey)
	if err != nil {
		return fmt.Errorf("Cannot init S3 client: %v\n", err)
	}
	partnerUpload.S3Client = s3Client
	return nil
}

// Loads configuration from the specified file path.
func (partnerUpload *PartnerUpload) LoadConfig(configFile string) (error) {
	partnerConfig, err := LoadPartnerConfig(configFile)
	if err != nil {
		return err
	}
	partnerUpload.PartnerConfig = partnerConfig
	err = partnerUpload.ValidateConfig()
	return err
}

func (partnerUpload *PartnerUpload) ValidateConfig() (error) {
	if partnerUpload.PartnerConfig == nil {
		return fmt.Errorf("PartnerConfig cannot be nil.")
	}
	if partnerUpload.PartnerConfig.AwsAccessKeyId == "" || partnerUpload.PartnerConfig.AwsSecretAccessKey == "" {
		if partnerUpload.LogVerbose && !partnerUpload.Test {
			fmt.Println("[INFO]  AWS keys are not in config file. Loading from environment.")
		}
		partnerUpload.PartnerConfig.LoadAwsFromEnv()
	}
	if partnerUpload.PartnerConfig.AwsAccessKeyId == "" {
		return fmt.Errorf("AWS_ACCESS_KEY_ID is missing. This should be set in " +
			"the config file as AwsAccessKeyId or in the environment as AWS_ACCESS_KEY_ID.")
	}
	if partnerUpload.PartnerConfig.AwsSecretAccessKey == "" {
		return fmt.Errorf("AWS_SECRET_ACCESS_KEY is missing. This should be set in " +
			"the config file as AwsSecretAccessKey or in the environment as AWS_SECRET_ACCESS_KEY.")
	}
	if partnerUpload.PartnerConfig.ReceivingBucket == "" {
		return fmt.Errorf("Config file setting ReceivingBucket is missing.")
	}
	return nil
}

// Uploads all files in filePaths to the S3 receiving bucket.
// Returns the number of uploads that succeeded and the number
// that failed.
func (partnerUpload *PartnerUpload) UploadFiles(filePaths []string) (succeeded, failed int) {
	if filePaths == nil || len(filePaths) == 0 {
		if partnerUpload.LogVerbose && !partnerUpload.Test {
			fmt.Println("[INFO]  There are no files in the upload list.")
		}
		return
	}
	succeeded = 0
	failed = 0
	for _, filePath := range(filePaths) {
		file, err := os.Open(filePath)
		if err != nil {
			if !partnerUpload.Test {
				fmt.Printf("[ERROR] Cannot read local file '%s': %v\n", filePath, err)
			}
			failed++
			continue
		}
		fileStat, err := file.Stat()
		if err == nil && fileStat.IsDir() {
			if partnerUpload.LogVerbose && !partnerUpload.Test {
				fmt.Printf("[INFO]  Skipping %s because it's a directory.\n", file.Name())
			}
			continue
		}
		etag, err := partnerUpload.UploadFile(file)
		if err != nil {
			if !partnerUpload.Test {
				fmt.Printf("[ERROR] Uploading '%s' to S3: %v\n", filePath, err)
			}
			failed++
			continue
		}
		if !partnerUpload.Test {
			fmt.Printf("[OK]    S3 returned md5 checksum %s for %s\n", etag, filePath)
		}
		succeeded++
	}
	if !partnerUpload.Test {
		fmt.Printf("Finished uploading. %d succeeded, %d failed.\n", succeeded, failed)
	}
	return succeeded, failed
}

// Uploads a single file to the S3 receiving bucket.
// Returns S3's checksum for the file, or an error.
// Note that S3 checksums for large multi-part uploads
// will not be normal md5 checksums.
func (partnerUpload *PartnerUpload) UploadFile(file *os.File) (string, error) {
	fileStat, err := file.Stat()
	if err != nil {
		return "", err
	}
	bucketName := partnerUpload.PartnerConfig.ReceivingBucket
	fileName := filepath.Base(fileStat.Name())
	mimeType := "application/x-tar"
	url := ""
	if fileStat.Size() < S3_LARGE_FILE {
		if partnerUpload.LogVerbose && !partnerUpload.Test {
			fmt.Printf("[INFO]  Sending %s to %s in single-part put\n", fileName, bucketName)
		}
		url, err = partnerUpload.S3Client.SaveToS3(
			bucketName,
			fileName,
			mimeType,
			file,
			fileStat.Size(),
			s3.Options{})
	} else {
		if partnerUpload.LogVerbose && !partnerUpload.Test {
			fmt.Printf("[INFO]  Sending %s to %s in multi-part put because size is %d\n",
				fileName, bucketName, fileStat.Size())
		}
		url, err = partnerUpload.S3Client.SaveLargeFileToS3(
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
	if partnerUpload.LogVerbose && !partnerUpload.Test {
		fmt.Printf("[INFO]  File %s saved to %s\n", fileName, url)
	}

	httpResponse, err := partnerUpload.S3Client.Head(bucketName, fileName)
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
