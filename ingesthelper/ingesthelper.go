package ingesthelper

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"github.com/bitly/go-nsq"
	"github.com/crowdmob/goamz/s3"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

type IngestHelper struct {
	ProcUtil        *processutil.ProcessUtil
	Result          *bagman.ProcessResult
	bytesInS3       int64
	bytesProcessed  int64
}

// Returns a new IngestHelper
func NewIngestHelper(procUtil *processutil.ProcessUtil, message *nsq.Message, s3File *bagman.S3File) (*IngestHelper){
	return &IngestHelper{
		ProcUtil: procUtil,
		Result: newResult(message, s3File),
		bytesInS3: int64(0),
		bytesProcessed: int64(0),
	}
}

// Returns a new ProcessResult for the specified NSQ message
// and S3 bag (tar file)
func newResult(message *nsq.Message, s3File *bagman.S3File) (*bagman.ProcessResult) {
	return &bagman.ProcessResult{
		NsqMessage:    message,
		S3File:        s3File,
		ErrorMessage:  "",
		FetchResult:   nil,
		TarResult:     nil,
		BagReadResult: nil,
		FedoraResult:  nil,
		Stage:         "",
		Retry:         true,
	}
}


// Returns true if the file needs processing. We check this
// because the bucket reader may add duplicate items to the
// queue when the queue is long and the reader refills it hourly.
// If we get rid of NSQ and read directly from the
// database, we can get rid of this.
func BagNeedsProcessing(s3File *bagman.S3File, procUtil *processutil.ProcessUtil) bool {
	bagDate, err := time.Parse(bagman.S3DateFormat, s3File.Key.LastModified)
	if err != nil {
		procUtil.MessageLog.Error("Cannot parse S3File mod date '%s'. "+
			"File %s will be re-processed.",
			s3File.Key.LastModified, s3File.Key.Key)
		return true
	}
	etag := strings.Replace(s3File.Key.ETag, "\"", "", 2)
	status, err := procUtil.FluctusClient.GetBagStatus(etag, s3File.Key.Key, bagDate)
	if err != nil {
		procUtil.MessageLog.Error("Error getting status for file %s. Will reprocess.",
			s3File.Key.Key)
	}
	if status != nil && (status.Stage == bagman.StageRecord && status.Status == bagman.StatusSuccess) {
		return false
	}
	return true
}

func (helper *IngestHelper) IncompleteCopyToS3() (bool) {
	return (helper.Result.TarResult.AnyFilesCopiedToPreservation() == true &&
		helper.Result.TarResult.AllFilesCopiedToPreservation() == false)
}

func (helper *IngestHelper) FailedAndNoMoreRetries() (bool) {
	return (helper.Result.ErrorMessage != "" &&
		helper.Result.NsqMessage.Attempts >= uint16(helper.ProcUtil.Config.MaxBagAttempts))
}

// Returns an OPEN reader for the specified GenericFile (reading it from
// the local disk). Caller is responsible for closing the reader.
func (helper *IngestHelper) GetFileReader(gf *bagman.GenericFile) (*os.File, string, error) {
	re := regexp.MustCompile("\\.tar$")
	bagDir := re.ReplaceAllString(helper.Result.S3File.Key.Key, "")
	file := filepath.Join(helper.ProcUtil.Config.TarDirectory, bagDir, gf.Path)
	absPath, err := filepath.Abs(file)
	if err != nil {
		// Consider this error transient. Leave retry = true.
		detailedError := fmt.Errorf("Cannot get absolute "+
			"path to file '%s'. "+
			"File cannot be copied to long-term storage: %v",
			file, err)
		return nil, "", detailedError
	}
	reader, err := os.Open(absPath)
	if err != nil {
		// Consider this error transient. Leave retry = true.
		detailedError := fmt.Errorf("Error opening file '%s'"+
			". File cannot be copied to long-term storage: %v",
			absPath, err)
		return nil, absPath, detailedError
	}
	return reader, absPath, nil
}

func (helper *IngestHelper) GetS3Options(gf *bagman.GenericFile) (*s3.Options, error) {
	// Prepare metadata for save to S3
	bagName, err := bagman.CleanBagName(helper.Result.S3File.Key.Key)
	if err != nil {
		return nil, err
	}
	instDomain := bagman.OwnerOf(helper.Result.S3File.BucketName)
	s3Metadata := make(map[string][]string)
	s3Metadata["md5"] = []string{gf.Md5}
	s3Metadata["institution"] = []string{instDomain}
	s3Metadata["bag"] = []string{bagName}
	s3Metadata["bagpath"] = []string{gf.Path}

	// We'll get error if md5 contains non-hex characters. Catch
	// that below, when S3 tells us our md5 sum is invalid.
	md5Bytes, err := hex.DecodeString(gf.Md5)
	if err != nil {
		detailedError := fmt.Errorf("Md5 sum '%s' contains invalid characters. "+
			"S3 will reject this!", gf.Md5)
		return nil, detailedError
	}

	// Save to S3 with the base64-encoded md5 sum
	base64md5 := base64.StdEncoding.EncodeToString(md5Bytes)

	options := helper.ProcUtil.S3Client.MakeOptions(base64md5, s3Metadata)
	return &options, nil
}

// Unpacks the bag file at path, extracts tag info and returns information
// about whether it was successfully unpacked, valid and complete.
func (helper *IngestHelper) ProcessBagFile() {
	helper.Result.Stage = "Unpack"
	instDomain := bagman.OwnerOf(helper.Result.S3File.BucketName)
	helper.Result.TarResult = bagman.Untar(helper.Result.FetchResult.LocalTarFile,
		instDomain, helper.Result.S3File.BagName())
	if helper.Result.TarResult.ErrorMessage != "" {
		helper.Result.ErrorMessage = helper.Result.TarResult.ErrorMessage
		// If we can't untar this, there's no reason to retry...
		// but we'll have to revisit this. There may be cases
		// where we do want to retry, such as if disk was full.
		helper.Result.Retry = false
	} else {
		helper.Result.Stage = "Validate"
		helper.Result.BagReadResult = bagman.ReadBag(helper.Result.TarResult.OutputDir)
		if helper.Result.BagReadResult.ErrorMessage != "" {
			helper.Result.ErrorMessage = helper.Result.BagReadResult.ErrorMessage
			// Something was wrong with this bag. Bad checksum,
			// missing file, etc. Don't reprocess it.
			helper.Result.Retry = false
		} else {
			for i := range helper.Result.TarResult.GenericFiles {
				gf := helper.Result.TarResult.GenericFiles[i]
				gf.Md5Verified = time.Now()
			}
		}
	}
}

func (helper *IngestHelper) LogResult() {
		// Log full results to the JSON log
		json, err := json.Marshal(helper.Result)
		if err != nil {
			helper.ProcUtil.MessageLog.Error(err.Error())
		}
		helper.ProcUtil.JsonLog.Println(string(json))

		// Add a message to the message log
		atomic.AddInt64(&helper.bytesInS3, int64(helper.Result.S3File.Key.Size))
		if helper.Result.ErrorMessage != "" {
			helper.ProcUtil.IncrementFailed()
			helper.ProcUtil.MessageLog.Error("%s -> %s", helper.Result.S3File.BagName(), helper.Result.ErrorMessage)
		} else {
			helper.ProcUtil.IncrementSucceeded()
			atomic.AddInt64(&helper.bytesProcessed, int64(helper.Result.S3File.Key.Size))
			helper.ProcUtil.MessageLog.Info("%s -> finished OK", helper.Result.S3File.BagName())
		}

		// Add some stats to the message log
		helper.ProcUtil.LogStats()
		helper.ProcUtil.MessageLog.Info("Total Bytes Processed: %d", helper.bytesProcessed)

		// Tell Fluctus what happened
		err = helper.ProcUtil.FluctusClient.SendProcessedItem(helper.Result.IngestStatus())
		if err != nil {
			helper.Result.ErrorMessage += fmt.Sprintf("Attempt to record processed "+
				"item status returned error %v. ", err)
			helper.ProcUtil.MessageLog.Error("Error sending ProcessedItem to Fluctus: %v",
				err)
		}
}

// Our result object contains information about the bag we just unpacked.
// Fedora may have information about a previous version of this bag, or
// about the same version of the same bag from an earlier round of processing.
// This function merges data from Fedora into our result, so we can know
// whether any of the generic files have been updated.
func (helper *IngestHelper) MergeFedoraRecord() (error) {
	intelObj, err := helper.Result.IntellectualObject()
	if err != nil {
		return err
	}
	fedoraObj, err := helper.ProcUtil.FluctusClient.IntellectualObjectGet(intelObj.Identifier, true)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Error checking Fluctus for existing IntellectualObject '%s': %v",
			intelObj.Identifier, err)
		return detailedError
	}
	if fedoraObj != nil {
		helper.Result.TarResult.MergeExistingFiles(fedoraObj.GenericFiles)
	}
	return nil
}

// This deletes the tar file and all of the files that were
// unpacked from it. Param file is the path the tar file.
func (helper *IngestHelper) DeleteLocalFiles() (errors []error) {
	errors = make([]error, 0)
	err := os.Remove(helper.Result.FetchResult.LocalTarFile)
	if err != nil {
		errors = append(errors, err)
	}
	// The untarred dir name is the same as the tar file, minus
	// the .tar extension. This is guaranteed by bag.Untar.
	re := regexp.MustCompile("\\.tar$")
	untarredDir := re.ReplaceAllString(helper.Result.FetchResult.LocalTarFile, "")
	err = os.RemoveAll(untarredDir)
	if err != nil {
		helper.ProcUtil.MessageLog.Error("Error deleting dir %s: %s\n", untarredDir, err.Error())
		errors = append(errors, err)
	}
	return errors
}

// This fetches a file from S3 and stores it locally.
func (helper *IngestHelper) FetchTarFile() {
	helper.Result.Stage = "Fetch"
	tarFilePath := filepath.Join(helper.ProcUtil.Config.TarDirectory, helper.Result.S3File.Key.Key)
	helper.Result.FetchResult = helper.ProcUtil.S3Client.FetchToFile(helper.Result.S3File.BucketName,
		helper.Result.S3File.Key, tarFilePath)
	helper.Result.Retry = helper.Result.FetchResult.Retry
	if helper.Result.FetchResult.ErrorMessage != "" {
		// Copy all errors up to the top level
		helper.Result.ErrorMessage = helper.Result.FetchResult.ErrorMessage
	}
}

func (helper *IngestHelper) SaveGenericFiles() (error) {
	result := helper.Result
	result.Stage = "Store"
	// See what Fedora knows about this object's files.
	// If none are new/changed, there's no need to save.
	err := helper.MergeFedoraRecord()
	if err != nil {
		helper.Result.ErrorMessage += fmt.Sprintf("%v ", err)
		return err
	}
	if result.TarResult.AnyFilesNeedSaving() == false {
		helper.ProcUtil.MessageLog.Info("Nothing to save to S3 for %s: " +
			"files have not changed since they were last ingested",
			result.S3File.Key.Key)
		return nil
	}

	helper.ProcUtil.MessageLog.Info("Storing %s", result.S3File.Key.Key)

	// Copy each generic file to S3
	for i := range result.TarResult.GenericFiles {
		gf := result.TarResult.GenericFiles[i]
		if gf.NeedsSave == false {
			helper.ProcUtil.MessageLog.Info("Not saving %s to S3, because it has not " +
				"changed since it was last saved.", gf.Identifier)
			continue
		}
		_, err := helper.SaveFile(gf)
		if err != nil {
			continue
		}
	}
	return nil
}

// Saves a file to the preservation bucket.
// Returns the url of the file that was saved. Returns an error if there
// was a problem.
func (helper *IngestHelper) SaveFile(gf *bagman.GenericFile) (string, error) {
	// Create the S3 metadata to save with the file
	options, err := helper.GetS3Options(gf)
	if err != nil {
		helper.ProcUtil.MessageLog.Error("Cannot send %s to S3: %v", gf.Path, err)
		helper.Result.ErrorMessage += fmt.Sprintf("%v ", err)
		return "", err
	}

	// Open the local file for reading
	reader, absPath, err := helper.GetFileReader(gf)
	if err != nil {
		// Consider this error transient. Leave retry = true.
		helper.ProcUtil.MessageLog.Error("Cannot send %s to S3: %v", gf.Path, err)
		helper.Result.ErrorMessage += fmt.Sprintf("%v ", err)
		return "", err
	}

	// Tweet to all our fans
	helper.ProcUtil.MessageLog.Debug("Sending %d bytes to S3 for file %s (UUID %s)",
		gf.Size, gf.Path, gf.Uuid)

	// Copy the file to preservation.
	// This fails often with 'connection reset by peer', so try several times
	var url string = ""
	for attemptNumber := 0; attemptNumber < 5; attemptNumber++ {
		url, err = helper.CopyToPreservationBucket(gf, reader, options)
		if err == nil {
			break
		}
	}
	reader.Close()
	if err != nil {
		// Consider this error transient. Leave retry = true.
		helper.Result.ErrorMessage += fmt.Sprintf("Error copying file '%s'"+
			"to long-term storage: %v ", absPath, err)
		helper.ProcUtil.MessageLog.Warning("Failed to send %s to long-term storage: %s",
			helper.Result.S3File.Key.Key,
			err.Error())
		return "", err
	} else {
		gf.StorageURL = url
		gf.StoredAt = time.Now()
		// We send the md5 checksum with the file to S3.
		// If S3 calculates a different checksum, it returns an error.
		// Since there was no error, we know S3 calculated the same checksum
		// that we calculated.
		gf.StorageMd5 = gf.Md5

		helper.ProcUtil.MessageLog.Debug("Successfully sent %s (UUID %s)"+
			"to long-term storage bucket.", gf.Path, gf.Uuid)
	}
	return url, nil
}


// Returns the S# URL of the file that was copied to
// the preservation bucket, or an error.
func (helper *IngestHelper) CopyToPreservationBucket(gf *bagman.GenericFile, reader *os.File, options *s3.Options) (string, error) {
	if gf.Size < bagman.S3_LARGE_FILE {
		return helper.ProcUtil.S3Client.SaveToS3(
			helper.ProcUtil.Config.PreservationBucket,
			gf.Uuid,
			gf.MimeType,
			reader,
			gf.Size,
			*options)
	} else {
		// Multi-part put for files >= 5GB
		helper.ProcUtil.MessageLog.Debug("File %s is %d bytes. Using multi-part put.\n",
			gf.Path, gf.Size)
		return helper.ProcUtil.S3Client.SaveLargeFileToS3(
			helper.ProcUtil.Config.PreservationBucket,
			gf.Uuid,
			gf.MimeType,
			reader,
			gf.Size,
			*options,
			bagman.S3_CHUNK_SIZE)
	}
}

func (helper *IngestHelper) UpdateFluctusStatus(stage bagman.StageType, status bagman.StatusType) {
	helper.ProcUtil.MessageLog.Debug("Setting status for %s to %s/%s in Fluctus",
		helper.Result.S3File.Key.Key, stage, status)
	ingestStatus := helper.Result.IngestStatus()
	ingestStatus.Stage = stage
	ingestStatus.Status = status
	err := helper.ProcUtil.FluctusClient.SendProcessedItem(ingestStatus)
	if err != nil {
		helper.Result.ErrorMessage += fmt.Sprintf("Attempt to record processed "+
			"item status returned error %v. ", err)
		helper.ProcUtil.MessageLog.Error("Error sending ProcessedItem to Fluctus: %v",
			err)
	}
}
