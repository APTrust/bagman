package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"github.com/APTrust/bagman/ingesthelper"
	"github.com/bitly/go-nsq"
	"time"
)

type Channels struct {
	FetchChannel   chan *ingesthelper.IngestHelper
	UnpackChannel  chan *ingesthelper.IngestHelper
	StorageChannel chan *ingesthelper.IngestHelper
	CleanUpChannel chan *ingesthelper.IngestHelper
	ResultsChannel chan *ingesthelper.IngestHelper
}

// Global vars.
var procUtil *processutil.ProcessUtil
var channels *Channels

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
	nsqConfig.Set("msg_timeout", "180m")
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
	channels.FetchChannel = make(chan *ingesthelper.IngestHelper, fetcherBufferSize)
	channels.UnpackChannel = make(chan *ingesthelper.IngestHelper, workerBufferSize)
	channels.StorageChannel = make(chan *ingesthelper.IngestHelper, workerBufferSize)
	channels.CleanUpChannel = make(chan *ingesthelper.IngestHelper, workerBufferSize)
	channels.ResultsChannel = make(chan *ingesthelper.IngestHelper, workerBufferSize)
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
	if procUtil.Config.SkipAlreadyProcessed == true && ingesthelper.BagNeedsProcessing(&s3File, procUtil) == false {
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
	mapErr := procUtil.RegisterItem(s3File.BagName(), message.ID)
	if mapErr != nil {
		procUtil.MessageLog.Info("Marking %s as complete because the file is already "+
			"being processed under another message id.\n", s3File.Key.Key)
		message.Finish()
		return nil
	}

	// Create the result struct and pass it down the pipeline
	helper := ingesthelper.NewIngestHelper(procUtil, message, &s3File)
	channels.FetchChannel <- helper
	procUtil.MessageLog.Debug("Put %s into fetch queue", s3File.Key.Key)
	return nil
}

// -- Step 1 of 5 --
// This runs as a go routine to fetch files from S3.
func doFetch() {
	for helper := range channels.FetchChannel {
		result := helper.Result
		result.NsqMessage.Touch()
		s3Key := result.S3File.Key
		// Disk needs filesize * 2 disk space to accomodate tar file & untarred files
		err := procUtil.Volume.Reserve(uint64(s3Key.Size * 2))
		if err != nil {
			// Not enough room on disk
			procUtil.MessageLog.Warning("Requeueing %s - not enough disk space", s3Key.Key)
			result.ErrorMessage = err.Error()
			result.Retry = true
			channels.ResultsChannel <- helper
		} else {
			procUtil.MessageLog.Info("Fetching %s", s3Key.Key)
			helper.FetchTarFile()
			if result.ErrorMessage != "" {
				// Fetch from S3 failed. Requeue.
				channels.ResultsChannel <- helper
			} else {
				// Got S3 file. Untar it.
				// And touch the message, so nsqd knows we're making progress.
				result.NsqMessage.Touch()
				channels.UnpackChannel <- helper
			}
		}
	}
}

// -- Step 2 of 5 --
// This runs as a go routine to untar files downloaded from S3.
// We calculate checksums and create generic files during the unpack
// stage to avoid having to reprocess large streams of data several times.
func doUnpack() {
	for helper := range channels.UnpackChannel {
		result := helper.Result
		if result.ErrorMessage != "" {
			// Unpack failed. Go to end.
			procUtil.MessageLog.Warning("Nothing to unpack for %s",
				result.S3File.Key.Key)
			channels.ResultsChannel <- helper
		} else {
			// Unpacked! Now process the bag and touch message
			// so nsqd knows we're making progress.
			procUtil.MessageLog.Info("Unpacking %s", result.S3File.Key.Key)
			// Touch when we start
			result.NsqMessage.Touch()
			// Processing can take 90+ minutes for very large files!
			helper.ProcessBagFile()
			// And touch again when we're done
			result.NsqMessage.Touch()
			if result.ErrorMessage == "" {
				// Move to permanent storage if bag processing succeeded
				channels.StorageChannel <- helper
			} else {
				channels.ResultsChannel <- helper
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
	for helper := range channels.StorageChannel {
		// Touch before and after sending generic files,
		// since that process can take a long time for large bags.
		helper.Result.NsqMessage.Touch()
		err := helper.SaveGenericFiles()
		helper.Result.NsqMessage.Touch()
		if err != nil {
			channels.ResultsChannel <- helper
			continue
		}
		// If there were no errors, put this into the metadata
		// queue, so we can record the events in Fluctus.
		if helper.Result.ErrorMessage == "" {
			SendToMetadataQueue(helper)
		}

		// Pass problem cases off to the trouble queue
		if helper.IncompleteCopyToS3() || helper.FailedAndNoMoreRetries() {
			SendToTroubleQueue(helper)
		}

		// Record the results.
		channels.ResultsChannel <- helper
	}
}


// -- Step 4 of 5 --
// TODO: This code is duplicated in metarecord.go
// This prints to the log the result of the program's attempt to fetch,
// untar, unbag and verify an individual S3 tar file.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func logResult() {
	for helper := range channels.ResultsChannel {
		helper.LogResult()
		channels.CleanUpChannel <- helper
	}
}

// -- Step 5 of 5 --
// This runs as a go routine to remove the files we downloaded
// and untarred.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func doCleanUp() {
	for helper := range channels.CleanUpChannel {
		result := helper.Result
		result.NsqMessage.Touch()
		procUtil.MessageLog.Debug("Cleaning up %s", result.S3File.Key.Key)
		if (result.S3File.Key.Key != "" && result.FetchResult != nil &&
			result.FetchResult.LocalTarFile != "") {
			// Clean up any files we downloaded and unpacked
			errors := helper.DeleteLocalFiles()
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
		procUtil.UnregisterItem(result.S3File.BagName())
	}
}


// Puts an item into the queue for Fluctus/Fedora metadata processing.
func SendToMetadataQueue(helper *ingesthelper.IngestHelper) {
	err := bagman.Enqueue(helper.ProcUtil.Config.NsqdHttpAddress,
		helper.ProcUtil.Config.MetadataTopic, helper.Result)
	if err != nil {
		errMsg := fmt.Sprintf("Error adding '%s' to metadata queue: %v ",
			helper.Result.S3File.Key.Key, err)
		helper.ProcUtil.MessageLog.Error(errMsg)
		helper.Result.ErrorMessage += errMsg
	} else {
		helper.ProcUtil.MessageLog.Debug("Sent '%s' to metadata queue",
			helper.Result.S3File.Key.Key)
	}
}

// Puts an item into the trouble queue.
func SendToTroubleQueue(helper *ingesthelper.IngestHelper) {
	err := bagman.Enqueue(helper.ProcUtil.Config.NsqdHttpAddress,
		helper.ProcUtil.Config.TroubleTopic, helper.Result)
	if err != nil {
		helper.ProcUtil.MessageLog.Error("Could not send '%s' to trouble queue: %v\n",
			helper.Result.S3File.Key.Key, err)
	} else {
		reason := "Processing failed and we reached the maximum number of retries."
		if helper.IncompleteCopyToS3() {
			reason = "Some files could not be copied to S3."
		}
		helper.Result.ErrorMessage += fmt.Sprintf("%s This item has been queued for administrative review.",
			reason)
		helper.ProcUtil.MessageLog.Warning("Sent '%s' to trouble queue: %s",
			helper.Result.S3File.Key.Key, reason)
	}
}
