package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"github.com/bitly/go-nsq"
	"github.com/diamondap/goamz/s3"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

type Channels struct {
	FetchChannel   chan *bagman.ProcessResult
	UnpackChannel  chan *bagman.ProcessResult
	StorageChannel chan *bagman.ProcessResult
	CleanUpChannel chan *bagman.ProcessResult
	ResultsChannel chan *bagman.ProcessResult
}

// Global vars.
var procUtil *processutil.ProcessUtil
var channels *Channels
var bytesInS3 = int64(0)
var bytesProcessed = int64(0)

// bag_processor receives messages from nsqd describing
// items in the S3 receiving buckets. Each item/message
// follows this flow:
//
// 1. Fetch channel: fetches the file from S3.
// 2. Unpack channel: untars the bag files, parses and validates
//    the bag, reads tags, generates checksums and generic file
//    UUIDs.
// 3. Storage channel: copies files to S3 permanent storage.
// 4. Results channel: tells the queue whether processing
//    succeeded, and if not, whether the item should be requeued.
//    Also logs results to json and message logs.
// 5. Cleanup channel: cleans up the files after processing
//    completes.
//
// If a failure occurs anywhere in the first three steps,
// processing goes directly to the Results Channel, which
// records the error and the disposition (retry/give up).
//
// As long as the message from nsq contains valid JSON,
// steps 5 and 6 ALWAYS run.
//
// The bag processor has so many responsibilities because
// downloading, untarring and running checksums on
// multi-gigabyte files takes a lot of time. We want to
// avoid having separate processes repeatedly download and
// untar the files, so bag_processor performs all operations
// that require local access to the raw contents of the bags.
func main() {
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	procUtil = processutil.NewProcessUtil(requestedConfig)

	procUtil.MessageLog.Info("Bag Processor started")
	err := procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}

	initChannels()
	initGoRoutines()

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(procUtil.Config.MaxBagAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "60m")
	consumer, err := nsq.NewConsumer(procUtil.Config.BagProcessorTopic, procUtil.Config.BagProcessorChannel, nsqConfig)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}

	handler := &BagProcessor{}
	consumer.SetHandler(handler)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}

// Set up the channels. It's essential that that the fetchChannel
// be limited to a relatively low number. If we are downloading
// 1GB tar files, we need space to store the tar file AND the
// untarred version. That's about 2 x 1GB. We do not want to pull
// down 1000 files at once, or we'll run out of disk space!
// If config sets fetchers to 10, we can pull down 10 files at a
// time. The fetch queue could hold 10 * 4 = 40 items, so we'd
// have max 40 tar files + untarred directories on disk at once.
// The number of workers should be close to the number of CPU
// cores.
func initChannels() {
	fetcherBufferSize := procUtil.Config.Fetchers * 4
	workerBufferSize := procUtil.Config.Workers * 10

	channels = &Channels{}
	channels.FetchChannel = make(chan *bagman.ProcessResult, fetcherBufferSize)
	channels.UnpackChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	channels.StorageChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	channels.CleanUpChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	channels.ResultsChannel = make(chan *bagman.ProcessResult, workerBufferSize)
}

// Set up our go routines. We do NOT want one go routine per
// S3 file. If we do that, the system will run out of file handles,
// as we'll have tens of thousands of open connections to S3
// trying to write data into tens of thousands of local files.
func initGoRoutines() {
	for i := 0; i < procUtil.Config.Fetchers; i++ {
		go doFetch()
	}

	for i := 0; i < procUtil.Config.Workers; i++ {
		go doUnpack()
		go saveToStorage()
		go logResult()
		go doCleanUp()
	}
}

type BagProcessor struct {
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*BagProcessor) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var s3File bagman.S3File
	err := json.Unmarshal(message.Body, &s3File)
	if err != nil {
		procUtil.MessageLog.Error("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		message.Finish()
		return nil
	}

	// If we're not reprocessing on purpose, and this item has already
	// been successfully processed, skip it. There are certain timing
	// conditions that can cause the bucket reader to add items to the
	// queue twice. If we get rid of NSQ, we can get rid of this check.
	if procUtil.Config.SkipAlreadyProcessed == true && needsProcessing(&s3File) == false {
		procUtil.MessageLog.Info("Marking %s as complete, without processing because "+
			"this bag was successfully processed previously and Config.SkipAlreadyProcessed "+
			"= true", s3File.Key.Key)
		message.Finish()
		return nil
	}

	// Don't start working on a message that we're already working on.
	// Note that the key we include in the syncMap includes multipart
	// bag endings, so we can be working on ncsu.edu/obj.b1of2.tar and
	// ncsu.edu/obj.b2of2.tar at the same time. This is what we want.
	key := fmt.Sprintf("%s/%s", bagman.OwnerOf(s3File.BucketName), s3File.Key.Key)
	mapErr := procUtil.RegisterItem(key, message.ID)
	if mapErr != nil {
		procUtil.MessageLog.Info("Marking %s as complete because the file is already "+
			"being processed under another message id.\n", s3File.Key.Key)
		message.Finish()
		return nil
	}

	// Create the result struct and pass it down the pipeline
	result := &bagman.ProcessResult{
		NsqMessage:    message,
		S3File:        &s3File,
		ErrorMessage:  "",
		FetchResult:   nil,
		TarResult:     nil,
		BagReadResult: nil,
		FedoraResult:  nil,
		Stage:         "",
		Retry:         true,
	}
	channels.FetchChannel <- result
	procUtil.MessageLog.Debug("Put %s into fetch queue", s3File.Key.Key)
	return nil
}

// Returns true if the file needs processing. We check this
// because the bucket reader may add duplicate items to the
// queue when the queue is long and the reader refills it hourly.
// If we get rid of NSQ and read directly from the
// database, we can get rid of this.
func needsProcessing(s3File *bagman.S3File) bool {
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

// -- Step 1 of 5 --
// This runs as a go routine to fetch files from S3.
func doFetch() {
	for result := range channels.FetchChannel {
		result.Stage = "Fetch"
		s3Key := result.S3File.Key
		result.FetchResult = &bagman.FetchResult{}
		// Disk needs filesize * 2 disk space to accomodate tar file & untarred files
		err := procUtil.Volume.Reserve(uint64(s3Key.Size * 2))
		if err != nil {
			// Not enough room on disk
			procUtil.MessageLog.Warning("Requeueing %s - not enough disk space", s3Key.Key)
			result.ErrorMessage = err.Error()
			result.Retry = true
			channels.ResultsChannel <- result
		} else {
			procUtil.MessageLog.Info("Fetching %s", s3Key.Key)
			fetchResult := Fetch(result.S3File.BucketName, s3Key)
			result.FetchResult = fetchResult
			result.Retry = fetchResult.Retry
			if fetchResult.ErrorMessage != "" {
				// Fetch from S3 failed. Requeue.
				result.ErrorMessage = fetchResult.ErrorMessage
				channels.ResultsChannel <- result
			} else {
				// Got S3 file. Untar it.
				// And touch the message, so nsqd knows we're making progress.
				result.NsqMessage.Touch()
				channels.UnpackChannel <- result
			}
		}
	}
}

// -- Step 2 of 5 --
// This runs as a go routine to untar files downloaded from S3.
// We calculate checksums and create generic files during the unpack
// stage to avoid having to reprocess large streams of data several times.
func doUnpack() {
	for result := range channels.UnpackChannel {
		if result.ErrorMessage != "" {
			// Unpack failed. Go to end.
			procUtil.MessageLog.Warning("Nothing to unpack for %s",
				result.S3File.Key.Key)
			channels.ResultsChannel <- result
		} else {
			// Unpacked! Now process the bag and touch message
			// so nsqd knows we're making progress.
			procUtil.MessageLog.Info("Unpacking %s", result.S3File.Key.Key)
			result.NsqMessage.Touch()
			ProcessBagFile(result)
			if result.ErrorMessage == "" {
				// Move to permanent storage if bag processing succeeded
				channels.StorageChannel <- result
			} else {
				channels.ResultsChannel <- result
			}
		}
	}
}

// -- Step 3 of 5 --
// This runs as a go routine to save generic files to the permanent
// S3 storage bucket. Unfortunately, there is no concept of transaction
// here. Ideally, either all GenericFiles make to S3 or none do. In
// cases where we're updating an existing bag (i.e. user uploaded a new
// version of it), we may wind up in a state where half of the new files
// make it successfully to S3 and half do not. That would leave us with
// an inconsistent bag, containing half new files and half old files.
// In addition, the failure to copy all files to S3 would result in no
// metadata going to Fedora. So Fedora would not show that any of the
// generic files were overwritten, even though some were. We should alert
// an admin in these cases. The JSON log will have full information about
// the state of all of the files.
func saveToStorage() {
	for result := range channels.StorageChannel {
		result.Stage = "Store"
		// See what Fedora knows about this object's files.
		// If none are new/changed, there's no need to save.
		err := mergeFedoraRecord(result)
		if err != nil {
			result.ErrorMessage += fmt.Sprintf("%v ", err)
			channels.ResultsChannel <- result
			continue
		}
		if result.TarResult.AnyFilesNeedSaving() == false {
			procUtil.MessageLog.Info("Nothing to save to S3 for %s: " +
				"files have not changed since they were last ingested",
				result.S3File.Key.Key)
			queueForMetadata(result)
			channels.ResultsChannel <- result
			continue
		}

		// TODO: Way too much code here for a single function!
		// Break it up!
		procUtil.MessageLog.Info("Storing %s", result.S3File.Key.Key)
		result.NsqMessage.Touch()
		re := regexp.MustCompile("\\.tar$")
		// Copy each generic file to S3
		for i := range result.TarResult.GenericFiles {
			gf := result.TarResult.GenericFiles[i]
			if gf.NeedsSave == false {
				procUtil.MessageLog.Info("Not saving %s to S3, because it has not " +
					"changed since it was last saved.", gf.Identifier)
				continue
			}
			bagDir := re.ReplaceAllString(result.S3File.Key.Key, "")
			file := filepath.Join(
				procUtil.Config.TarDirectory,
				bagDir,
				gf.Path)
			absPath, err := filepath.Abs(file)
			if err != nil {
				// Consider this error transient. Leave retry = true.
				result.ErrorMessage += fmt.Sprintf("Cannot get absolute "+
					"path to file '%s'. "+
					"File cannot be copied to long-term storage: %v",
					file, err)
				continue
			}
			reader, err := os.Open(absPath)
			if err != nil {
				// Consider this error transient. Leave retry = true.
				result.ErrorMessage += fmt.Sprintf("Error opening file '%s'"+
					". File cannot be copied to long-term storage: %v",
					absPath, err)
				continue
			}
			procUtil.MessageLog.Debug("Sending %d bytes to S3 for file %s (UUID %s)",
				gf.Size, gf.Path, gf.Uuid)

			// Prepare metadata for save to S3
			bagName := result.S3File.Key.Key[0 : len(result.S3File.Key.Key)-4]
			instDomain := bagman.OwnerOf(result.S3File.BucketName)
			s3Metadata := make(map[string][]string)
			s3Metadata["md5"] = []string{gf.Md5}
			s3Metadata["institution"] = []string{instDomain}
			s3Metadata["bag"] = []string{bagName}
			s3Metadata["bagpath"] = []string{gf.Path}

			// We'll get error if md5 contains non-hex characters. Catch
			// that below, when S3 tells us our md5 sum is invalid.
			md5Bytes, err := hex.DecodeString(gf.Md5)
			if err != nil {
				msg := fmt.Sprintf("Md5 sum '%s' contains invalid characters. "+
					"S3 will reject this!", gf.Md5)
				result.ErrorMessage += msg
				procUtil.MessageLog.Error(msg)
			}

			// Save to S3 with the base64-encoded md5 sum
			base64md5 := base64.StdEncoding.EncodeToString(md5Bytes)
			options := procUtil.S3Client.MakeOptions(base64md5, s3Metadata)
			var url string = ""
			// Standard put to S3 for files < 5GB
			if gf.Size < bagman.S3_LARGE_FILE {
				url, err = procUtil.S3Client.SaveToS3(
					procUtil.Config.PreservationBucket,
					gf.Uuid,
					gf.MimeType,
					reader,
					gf.Size,
					options)
			} else {
				// Multi-part put for files >= 5GB
				procUtil.MessageLog.Debug("File %s is %d bytes. Using multi-part put.\n",
					gf.Path, gf.Size)
				url, err = procUtil.S3Client.SaveLargeFileToS3(
					procUtil.Config.PreservationBucket,
					gf.Uuid,
					gf.MimeType,
					reader,
					gf.Size,
					options,
					bagman.S3_CHUNK_SIZE)
			}
			reader.Close()
			if err != nil {
				// Consider this error transient. Leave retry = true.
				result.ErrorMessage += fmt.Sprintf("Error copying file '%s'"+
					"to long-term storage: %v ", absPath, err)
				procUtil.MessageLog.Warning("Failed to send %s to long-term storage: %s",
					result.S3File.Key.Key,
					err.Error())
			} else {
				gf.StorageURL = url
				gf.StoredAt = time.Now()
				procUtil.MessageLog.Debug("Successfully sent %s (UUID %s)"+
					"to long-term storage bucket.", gf.Path, gf.Uuid)
			}
		}

		// If there were no errors, put this into the metadata
		// queue, so we can record the events in Fluctus.
		if result.ErrorMessage == "" {
			queueForMetadata(result)
		}

		// Pass problem cases off to the trouble queue
		copyToS3Incomplete := (result.TarResult.AnyFilesCopiedToPreservation() == true &&
			result.TarResult.AllFilesCopiedToPreservation() == false)
		failedAndNoMoreRetries := (result.ErrorMessage != "" &&
			result.NsqMessage.Attempts >= uint16(procUtil.Config.MaxBagAttempts))
		if copyToS3Incomplete || failedAndNoMoreRetries {
			err := bagman.Enqueue(procUtil.Config.NsqdHttpAddress, procUtil.Config.TroubleTopic, result)
			if err != nil {
				procUtil.MessageLog.Error("Could not send '%s' to trouble queue: %v\n",
					result.S3File.Key.Key, err)
			} else {
				reason := "Processing failed and we reached the maximum number of retries."
				if copyToS3Incomplete {
					reason = "Some files could not be copied to S3."
				}
				result.ErrorMessage += fmt.Sprintf("%s This item has been queued for administrative review.",
					reason)
				procUtil.MessageLog.Warning("Sent '%s' to trouble queue: %s", result.S3File.Key.Key, reason)
			}
		}

		// Record the results.
		channels.ResultsChannel <- result
	}
}

// -- Step 4 of 5 --
// TODO: This code is duplicated in metarecord.go
// This prints to the log the result of the program's attempt to fetch,
// untar, unbag and verify an individual S3 tar file.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func logResult() {
	for result := range channels.ResultsChannel {
		// Log full results to the JSON log
		json, err := json.Marshal(result)
		if err != nil {
			procUtil.MessageLog.Error(err.Error())
		}
		procUtil.JsonLog.Println(string(json))

		// Add a message to the message log
		atomic.AddInt64(&bytesInS3, int64(result.S3File.Key.Size))
		if result.ErrorMessage != "" {
			procUtil.IncrementFailed()
			procUtil.MessageLog.Error("%s %s -> %s",
				result.S3File.BucketName,
				result.S3File.Key.Key,
				result.ErrorMessage)
		} else {
			procUtil.IncrementSucceeded()
			atomic.AddInt64(&bytesProcessed, int64(result.S3File.Key.Size))
			procUtil.MessageLog.Info("%s -> finished OK", result.S3File.Key.Key)
		}

		// Add some stats to the message log
		procUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d, Bytes Processed: %d",
			procUtil.Succeeded(), procUtil.Failed(), bytesProcessed)

		// Tell Fluctus what happened
		go func() {
			err := procUtil.FluctusClient.SendProcessedItem(result.IngestStatus())
			if err != nil {
				result.ErrorMessage += fmt.Sprintf("Attempt to record processed "+
					"item status returned error %v. ", err)
				procUtil.MessageLog.Error("Error sending ProcessedItem to Fluctus: %v",
					err)
			}
		}()

		// Clean up the bag/tar files
		channels.CleanUpChannel <- result
	}
}

// -- Step 5 of 5 --
// This runs as a go routine to remove the files we downloaded
// and untarred.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func doCleanUp() {
	for result := range channels.CleanUpChannel {
		procUtil.MessageLog.Debug("Cleaning up %s", result.S3File.Key.Key)
		if result.S3File.Key.Key != "" && result.FetchResult.LocalTarFile != "" {
			// Clean up any files we downloaded and unpacked
			errors := CleanUp(result.FetchResult.LocalTarFile)
			if errors != nil && len(errors) > 0 {
				procUtil.MessageLog.Warning("Errors cleaning up %s",
					result.FetchResult.LocalTarFile)
				for _, e := range errors {
					procUtil.MessageLog.Error(e.Error())
				}
			}
			// Let our volume tracker know we just freed up some disk space.
			// Free the same amount we reserved.
			procUtil.Volume.Release(uint64(result.S3File.Key.Size * 2))
		}

		// Build and send message back to NSQ, indicating whether
		// processing succeeded.
		if result.ErrorMessage != "" && result.Retry == true {
			procUtil.MessageLog.Info("Requeueing %s", result.S3File.Key.Key)
			result.NsqMessage.Requeue(5 * time.Minute)
		} else {
			result.NsqMessage.Finish()
		}

		// We're done processing this, so remove it from the map.
		// If it comes in again, we'll reprocess it again.
		key := fmt.Sprintf("%s/%s", bagman.OwnerOf(result.S3File.BucketName), result.S3File.Key.Key)
		procUtil.UnregisterItem(key)
	}
}

// This fetches a file from S3 and stores it locally.
func Fetch(bucketName string, key s3.Key) (result *bagman.FetchResult) {
	tarFilePath := filepath.Join(procUtil.Config.TarDirectory, key.Key)
	return procUtil.S3Client.FetchToFile(bucketName, key, tarFilePath)
}

// This deletes the tar file and all of the files that were
// unpacked from it. Param file is the path the tar file.
func CleanUp(file string) (errors []error) {
	errors = make([]error, 0)
	err := os.Remove(file)
	if err != nil {
		errors = append(errors, err)
	}
	// The untarred dir name is the same as the tar file, minus
	// the .tar extension. This is guaranteed by bag.Untar.
	re := regexp.MustCompile("\\.tar$")
	untarredDir := re.ReplaceAllString(file, "")
	err = os.RemoveAll(untarredDir)
	if err != nil {
		procUtil.MessageLog.Error("Error deleting dir %s: %s\n", untarredDir, err.Error())
		errors = append(errors, err)
	}
	return errors
}

// Runs tests on the bag file at path and returns information about
// whether it was successfully unpacked, valid and complete.
func ProcessBagFile(result *bagman.ProcessResult) {
	result.Stage = "Unpack"
	instDomain := bagman.OwnerOf(result.S3File.BucketName)
	bagName := result.S3File.Key.Key[0 : len(result.S3File.Key.Key)-4]
	result.TarResult = bagman.Untar(result.FetchResult.LocalTarFile,
		instDomain, bagName)
	if result.TarResult.ErrorMessage != "" {
		result.ErrorMessage = result.TarResult.ErrorMessage
		// If we can't untar this, there's no reason to retry...
		// but we'll have to revisit this. There may be cases
		// where we do want to retry, such as if disk was full.
		result.Retry = false
	} else {
		result.Stage = "Validate"
		result.BagReadResult = bagman.ReadBag(result.TarResult.OutputDir)
		if result.BagReadResult.ErrorMessage != "" {
			result.ErrorMessage = result.BagReadResult.ErrorMessage
			// Something was wrong with this bag. Bad checksum,
			// missing file, etc. Don't reprocess it.
			result.Retry = false
		} else {
			for i := range result.TarResult.GenericFiles {
				gf := result.TarResult.GenericFiles[i]
				gf.Md5Verified = time.Now()
			}
		}
	}
}

// Puts an item into the queue for Fluctus/Fedora metadata processing.
func queueForMetadata(result *bagman.ProcessResult) {
	err := bagman.Enqueue(procUtil.Config.NsqdHttpAddress, procUtil.Config.MetadataTopic, result)
	if err != nil {
		errMsg := fmt.Sprintf("Error adding '%s' to metadata queue: %v ",
			result.S3File.Key.Key, err)
		procUtil.MessageLog.Error(errMsg)
		result.ErrorMessage += errMsg
	} else {
		procUtil.MessageLog.Debug("Sent '%s' to metadata queue",
			result.S3File.Key.Key)
	}
}

// Our result object contains information about the bag we just unpacked.
// Fedora may have information about a previous version of this bag, or
// about the same version of the same bag from an earlier round of processing.
// This function merges data from Fedora into our result, so we can know
// whether any of the generic files have been updated.
func mergeFedoraRecord(result *bagman.ProcessResult) (error) {
	intelObj, err := result.IntellectualObject()
	if err != nil {
		return err
	}
	fedoraObj, err := procUtil.FluctusClient.IntellectualObjectGet(intelObj.Identifier, true)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Error checking Fluctus for existing IntellectualObject '%s': %v",
			intelObj.Identifier, err)
		return detailedError
	}
	if fedoraObj != nil {
		result.TarResult.MergeExistingFiles(fedoraObj.GenericFiles)
	}
	return nil
}
