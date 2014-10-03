package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"github.com/APTrust/bagman/ingesthelper"
	"github.com/bitly/go-nsq"
	"os"
	"time"
)

type Channels struct {
	FetchChannel   chan *ingesthelper.IngestHelper
	UnpackChannel  chan *ingesthelper.IngestHelper
	CleanUpChannel chan *ingesthelper.IngestHelper
	ResultsChannel chan *ingesthelper.IngestHelper
}

// Large file is ~50GB
const LARGE_FILE_SIZE = int64(50000000000)

// Global vars.
var procUtil *processutil.ProcessUtil
var channels *Channels
var largeFile1 string = ""
var largeFile2 string = ""

// apt_prepare receives messages from nsqd describing
// items in the S3 receiving buckets. It fetches, untars,
// and validates tar files, then queues them for storage,
// if they untar and validate successfully. Each item/message
// follows this flow:
//
// 1. Fetch channel: fetches the file from S3.
// 2. Unpack channel: untars the bag files, parses and validates
//    the bag, reads tags, generates checksums and generic file
//    UUIDs.
// 3. Results channel: tells the queue whether processing
//    succeeded, and if not, whether the item should be requeued.
//    Also logs results to json and message logs.
// 4. Cleanup channel: cleans up the files after processing
//    completes.
//
// If a failure occurs anywhere in the first three steps,
// processing goes directly to the Results Channel, which
// records the error and the disposition (retry/give up).
//
// As long as the message from nsq contains valid JSON,
// steps 4 and 5 ALWAYS run.
func main() {
	requestedConfig := flag.String("config", "", "Configuration to run. Options are in config.json file. REQUIRED")
	customEnvFile := flag.String("env", "", "Absolute path to file containing custom environment vars. OPTIONAL")
	flag.Parse()
	procUtil = processutil.NewProcessUtil(requestedConfig)
	bagman.LoadCustomEnvOrDie(customEnvFile, procUtil.MessageLog)

	procUtil.MessageLog.Info("apt_prepare started")
	err := procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}

	initChannels()
	initGoRoutines()

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(procUtil.Config.MaxPrepareAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "180m")
	consumer, err := nsq.NewConsumer(procUtil.Config.PrepareTopic,
		procUtil.Config.PrepareChannel, nsqConfig)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}

	handler := &APTPrepare{}
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
	workerBufferSize := procUtil.Config.PrepareWorkers * 10

	channels = &Channels{}
	channels.FetchChannel = make(chan *ingesthelper.IngestHelper, fetcherBufferSize)
	channels.UnpackChannel = make(chan *ingesthelper.IngestHelper, workerBufferSize)
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

	for i := 0; i < procUtil.Config.PrepareWorkers; i++ {
		go doUnpack()
		go logResult()
		go doCleanUp()
	}
}

type APTPrepare struct {
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*APTPrepare) HandleMessage(message *nsq.Message) error {
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
			"Config.SkipAlreadyProcessed = true and this bag was ingested or is currently "+
			"being processed.", s3File.Key.Key)
		message.Finish()
		return nil
	}

	// Special case for very large bags: the bag is in process under
	// the same ID. NSQ thinks it timed out and has re-sent it. In this
	// case, return nil so NSQ knows we're OK, but don't finish the message.
	// The original process will call Finish() on the message when it's
	// done. If we call Finish() here, NSQ will throw a "not-in-flight"
	// error when the processor calls Finish() on the original message later.
	currentMessageId := procUtil.MessageIdString(message.ID)
	if procUtil.BagAlreadyInProgress(&s3File, currentMessageId) {
		procUtil.MessageLog.Info("Bag %s is already in progress under message id '%s'",
			s3File.Key.Key, procUtil.MessageIdFor(s3File.BagName()))
		return nil
	}

	// For very large files, do max two at a time so we don't get cut off
	// from S3 for going 20+ seconds without a read. If we do multiple
	// large files at once, we get cut off from S3 often. We can do lots
	// of small files while one or two large ones are processing.
	if s3File.Key.Size > LARGE_FILE_SIZE {
		if largeFile1 == "" {
			largeFile1 = s3File.BagName()
		} else if largeFile2 == "" {
			largeFile2 = s3File.BagName()
		} else {
			procUtil.MessageLog.Info("Requeueing %s because is >50GB and there are " +
				"already two large files in progress.", s3File.Key.Key)
			message.Requeue(60 * time.Minute)
			return nil
		}
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
			helper.UpdateFluctusStatus(bagman.StageFetch, bagman.StatusStarted)
			helper.FetchTarFile()
			if result.ErrorMessage != "" {
				// Fetch from S3 failed. Requeue.
				channels.ResultsChannel <- helper
			} else {
				// Got S3 file. Untar it.
				// And touch the message, so nsqd knows we're making progress.
				result.NsqMessage.Touch()
				helper.UpdateFluctusStatus(bagman.StageFetch, bagman.StatusPending)
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
			// Processing can take 3+ hours for very large files!
			helper.UpdateFluctusStatus(bagman.StageUnpack, bagman.StatusStarted)
			helper.ProcessBagFile()
			helper.UpdateFluctusStatus(bagman.StageValidate, bagman.StatusPending)
			// And touch again when we're done
			result.NsqMessage.Touch()
			channels.ResultsChannel <- helper
		}
	}
}


// -- Step 4 of 5 --
// This prints to the log the result of the program's attempt to fetch,
// untar, unbag and verify an individual S3 tar file. It logs state info
// about this bag to a json file on the local file system. Also logs
// a text message to the local bag_processor.log file and sends info
// to Fluctus saying whether the bag succeeded or failed.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func logResult() {
	for helper := range channels.ResultsChannel {
		result := helper.Result
		result.NsqMessage.Touch()
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
			cleanupBag(helper)
		}

		// Build and send message back to NSQ, indicating whether
		// processing succeeded.
		if result.ErrorMessage != "" {
			if result.Retry == true {
				procUtil.MessageLog.Info("Requeueing %s", result.S3File.Key.Key)
				result.NsqMessage.Requeue(5 * time.Minute)
			} else {
				result.NsqMessage.Finish()
			}
		} else {
			// Prepare succeeded. Send this off to storage queue,
			// so the generic files can go into long-term storage.
			SendToStorageQueue(helper)
			result.NsqMessage.Finish()
		}

		// We're done processing this, so remove it from the map.
		// If it comes in again, we'll reprocess it again.
		procUtil.UnregisterItem(result.S3File.BagName())
		if largeFile1 == result.S3File.BagName() {
			procUtil.MessageLog.Info("Done with largeFile1 %s", result.S3File.Key.Key)
			largeFile1 = ""
		} else if largeFile2 == result.S3File.BagName() {
			procUtil.MessageLog.Info("Done with largeFile2 %s", result.S3File.Key.Key)
			largeFile2 = ""
		}
	}
}

func cleanupBag(helper *ingesthelper.IngestHelper) {
	result := helper.Result
	if result.ErrorMessage == "" {
		// Clean up the tar file, but leave the unpacked files
		// for apt_store to send off to long-term storage.
		err := os.Remove(result.FetchResult.LocalTarFile)
		if err != nil {
			procUtil.MessageLog.Error("Error deleting tar file %s: %v",
				result.FetchResult.LocalTarFile, err)
		}
	} else {
		// Clean up ALL files we downloaded and unpacked
		errors := helper.DeleteLocalFiles()
		if errors != nil && len(errors) > 0 {
			procUtil.MessageLog.Warning("Errors cleaning up %s",
				result.FetchResult.LocalTarFile)
			for _, e := range errors {
				procUtil.MessageLog.Error(e.Error())
			}
		}
	}
	procUtil.Volume.Release(uint64(result.S3File.Key.Size * 2))
}


// Puts an item into the queue for Fluctus/Fedora metadata processing.
func SendToStorageQueue(helper *ingesthelper.IngestHelper) {
	err := bagman.Enqueue(helper.ProcUtil.Config.NsqdHttpAddress,
		helper.ProcUtil.Config.StoreTopic, helper.Result)
	if err != nil {
		errMsg := fmt.Sprintf("Error adding '%s' to storage queue: %v ",
			helper.Result.S3File.Key.Key, err)
		helper.ProcUtil.MessageLog.Error(errMsg)
		helper.Result.ErrorMessage += errMsg
	} else {
		helper.ProcUtil.MessageLog.Debug("Sent '%s' to storage queue",
			helper.Result.S3File.Key.Key)
	}
}
