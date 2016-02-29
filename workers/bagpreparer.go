package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"os"
	"strings"
	"time"
)

// Large file is ~50GB
const LARGE_FILE_SIZE = int64(50000000000)


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
//
// Some notes... It's essential that that the fetchChannel
// be limited to a relatively low number. If we are downloading
// 1GB tar files, we need space to store the tar file AND the
// untarred version. That's about 2 x 1GB. We do not want to pull
// down 1000 files at once, or we'll run out of disk space!
// If config sets fetchers to 10, we can pull down 10 files at a
// time. The fetch queue could hold 10 * 4 = 40 items, so we'd
// have max 40 tar files + untarred directories on disk at once.
// The number of workers should be close to the number of CPU
// cores.
//
// We do NOT want one go routine per S3 file. If we do that,
// the system will run out of file handles, as we'll have tens
// of thousands of open connections to S3 trying to write data
// into tens of thousands of local files.
type BagPreparer struct {
	FetchChannel   chan *bagman.IngestHelper
	UnpackChannel  chan *bagman.IngestHelper
	CleanUpChannel chan *bagman.IngestHelper
	ResultsChannel chan *bagman.IngestHelper
	ProcUtil       *bagman.ProcessUtil
	largeFile1     string
	largeFile2     string
}

func NewBagPreparer(procUtil *bagman.ProcessUtil) (*BagPreparer) {
	bagPreparer := &BagPreparer{
		ProcUtil: procUtil,
	}
	// Set up buffered channels
	fetcherBufferSize := procUtil.Config.PrepareWorker.NetworkConnections * 4
	workerBufferSize := procUtil.Config.PrepareWorker.Workers * 10
	bagPreparer.FetchChannel = make(chan *bagman.IngestHelper, fetcherBufferSize)
	bagPreparer.UnpackChannel = make(chan *bagman.IngestHelper, workerBufferSize)
	bagPreparer.CleanUpChannel = make(chan *bagman.IngestHelper, workerBufferSize)
	bagPreparer.ResultsChannel = make(chan *bagman.IngestHelper, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < procUtil.Config.PrepareWorker.NetworkConnections; i++ {
		go bagPreparer.doFetch()
	}
	for i := 0; i < procUtil.Config.PrepareWorker.Workers; i++ {
		go bagPreparer.doUnpack()
		go bagPreparer.logResult()
		go bagPreparer.doCleanUp()
	}
	return bagPreparer
}



// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (bagPreparer *BagPreparer) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var s3File bagman.S3File
	err := json.Unmarshal(message.Body, &s3File)
	if err != nil {
		bagPreparer.ProcUtil.MessageLog.Error("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		message.Finish()
		return nil
	}

	// If we're not reprocessing on purpose, and this item has already
	// been successfully processed, skip it. There are certain timing
	// conditions that can cause the bucket reader to add items to the
	// queue twice. If we get rid of NSQ, we can get rid of this check.
	if bagPreparer.ProcUtil.Config.SkipAlreadyProcessed == true &&
		bagman.BagNeedsProcessing(&s3File, bagPreparer.ProcUtil) == false {
		bagPreparer.ProcUtil.MessageLog.Info("Marking %s as complete, without processing because "+
			"Config.SkipAlreadyProcessed = true and this bag was ingested or is currently "+
			"being processed.", s3File.Key.Key)
		message.Finish()
		return nil
	}

	// Don't start ingest if there's a pending delete or restore request.
	// Ingest would just overwrite the files and metadata that delete/restore
	// would be operating on. If there is a pending delete/restore request,
	// send this back into the queue with an hour or so backoff time.
	//
	// If we can't parse the bag date, it's OK to send an empty date into
	// the search. We may pull back a few extra records and get a false positive
	// on the pending delete/restore. A false positive will delay ingest, but a
	// false negative could cause some cascading errors.
	bagDate, _ := time.Parse(bagman.S3DateFormat, s3File.Key.LastModified)
	processStatus := &bagman.ProcessStatus {
		ETag: strings.Replace(s3File.Key.ETag, "\"", "", -1),
		Name: s3File.Key.Key,
		BagDate: bagDate,
	}
	statusRecords, err := bagPreparer.ProcUtil.FluctusClient.ProcessStatusSearch(processStatus, true, true)
	if err != nil {
		bagPreparer.ProcUtil.MessageLog.Error("Error fetching status info on bag %s " +
			"from Fluctus. Will retry in 5 minutes. Error: %v", s3File.Key.Key, err)
		message.Requeue(5 * time.Minute)
		return nil
	}
	if bagman.HasPendingDeleteRequest(statusRecords) ||
		bagman.HasPendingRestoreRequest(statusRecords) {
		bagPreparer.ProcUtil.MessageLog.Info("Requeuing %s due to pending delete or " +
			"restore request. Will retry in at least 60 minutes.", s3File.Key.Key)
		message.Requeue(60 * time.Minute)
		return nil
	}

	// Special case for very large bags: the bag is in process under
	// the same ID. NSQ thinks it timed out and has re-sent it. In this
	// case, return nil so NSQ knows we're OK, but don't finish the message.
	// The original process will call Finish() on the message when it's
	// done. If we call Finish() here, NSQ will throw a "not-in-flight"
	// error when the processor calls Finish() on the original message later.
	currentMessageId := bagPreparer.ProcUtil.MessageIdString(message.ID)
	if bagPreparer.ProcUtil.BagAlreadyInProgress(&s3File, currentMessageId) {
		bagPreparer.ProcUtil.MessageLog.Info("Bag %s is already in progress under message id '%s'",
			s3File.Key.Key, bagPreparer.ProcUtil.MessageIdFor(s3File.BagName()))
		return nil
	}

	// For very large files, do max two at a time so we don't get cut off
	// from S3 for going 20+ seconds without a read. If we do multiple
	// large files at once, we get cut off from S3 often. We can do lots
	// of small files while one or two large ones are processing.
	if s3File.Key.Size > LARGE_FILE_SIZE {
		if bagPreparer.largeFile1 == "" {
			bagPreparer.largeFile1 = s3File.BagName()
		} else if bagPreparer.largeFile2 == "" {
			bagPreparer.largeFile2 = s3File.BagName()
		} else {
			bagPreparer.ProcUtil.MessageLog.Info("Requeueing %s because is >50GB and there are " +
				"already two large files in progress.", s3File.Key.Key)
			message.Requeue(60 * time.Minute)
			return nil
		}
	}

	// Don't start working on a message that we're already working on.
	// Note that the key we include in the syncMap includes multipart
	// bag endings, so we can be working on ncsu.edu/obj.b1of2.tar and
	// ncsu.edu/obj.b2of2.tar at the same time. This is what we want.
	mapErr := bagPreparer.ProcUtil.RegisterItem(s3File.BagName(), message.ID)
	if mapErr != nil {
		bagPreparer.ProcUtil.MessageLog.Info("Marking %s as complete because the file is already "+
			"being processed under another message id.\n", s3File.Key.Key)
		message.Finish()
		return nil
	}

	// Create the result struct and pass it down the pipeline
	helper := bagman.NewIngestHelper(bagPreparer.ProcUtil, message, &s3File)
	bagPreparer.FetchChannel <- helper
	bagPreparer.ProcUtil.MessageLog.Debug("Put %s into fetch queue", s3File.Key.Key)
	return nil
}

// -- Step 1 of 5 --
// This runs as a go routine to fetch files from S3.
func (bagPreparer *BagPreparer) doFetch() {
	for helper := range bagPreparer.FetchChannel {
		result := helper.Result
		result.NsqMessage.Touch()
		s3Key := result.S3File.Key
		// Disk needs filesize * 2 disk space to accomodate tar file & untarred files
		err := bagPreparer.ProcUtil.Volume.Reserve(uint64(s3Key.Size * 2))
		if err != nil {
			// Not enough room on disk
			bagPreparer.ProcUtil.MessageLog.Warning("Requeueing %s - not enough disk space", s3Key.Key)
			result.ErrorMessage = err.Error()
			result.Retry = true
			bagPreparer.ResultsChannel <- helper
		} else {
			bagPreparer.ProcUtil.MessageLog.Info("Fetching %s", s3Key.Key)
			helper.UpdateFluctusStatus(bagman.StageFetch, bagman.StatusStarted)
			helper.FetchTarFile()
			if result.ErrorMessage != "" {
				// Fetch from S3 failed. Requeue.
				bagPreparer.ResultsChannel <- helper
			} else {
				// Got S3 file. Untar it.
				// And touch the message, so nsqd knows we're making progress.
				result.NsqMessage.Touch()
				helper.UpdateFluctusStatus(bagman.StageFetch, bagman.StatusPending)
				bagPreparer.UnpackChannel <- helper
			}
		}
	}
}

// -- Step 2 of 5 --
// This runs as a go routine to untar files downloaded from S3.
// We calculate checksums and create generic files during the unpack
// stage to avoid having to reprocess large streams of data several times.
func (bagPreparer *BagPreparer) doUnpack() {
	for helper := range bagPreparer.UnpackChannel {
		result := helper.Result
		if result.ErrorMessage != "" {
			// Unpack failed. Go to end.
			bagPreparer.ProcUtil.MessageLog.Warning("Nothing to unpack for %s",
				result.S3File.Key.Key)
			bagPreparer.ResultsChannel <- helper
		} else {
			// Unpacked! Now process the bag and touch message
			// so nsqd knows we're making progress.
			bagPreparer.ProcUtil.MessageLog.Info("Unpacking %s", result.S3File.Key.Key)
			// Touch when we start
			result.NsqMessage.Touch()
			// Processing can take 3+ hours for very large files!
			helper.UpdateFluctusStatus(bagman.StageUnpack, bagman.StatusStarted)
			helper.ProcessBagFile()
			helper.UpdateFluctusStatus(bagman.StageValidate, bagman.StatusPending)
			// And touch again when we're done
			result.NsqMessage.Touch()
			bagPreparer.ResultsChannel <- helper
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
func (bagPreparer *BagPreparer) logResult() {
	for helper := range bagPreparer.ResultsChannel {
		result := helper.Result
		result.NsqMessage.Touch()
		helper.LogResult()
		bagPreparer.CleanUpChannel <- helper
	}
}

// -- Step 5 of 5 --
// This runs as a go routine to remove the files we downloaded
// and untarred.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func (bagPreparer *BagPreparer) doCleanUp() {
	for helper := range bagPreparer.CleanUpChannel {
		result := helper.Result
		result.NsqMessage.Touch()
		bagPreparer.ProcUtil.MessageLog.Debug("Cleaning up %s", result.S3File.Key.Key)
		if (result.S3File.Key.Key != "" && result.FetchResult != nil &&
			result.FetchResult.LocalFile != "") {
			bagPreparer.cleanupBag(helper)
		}

		// Build and send message back to NSQ, indicating whether
		// processing succeeded.
		if result.ErrorMessage != "" {
			if result.Retry == true {
				bagPreparer.ProcUtil.MessageLog.Info("Requeueing %s", result.S3File.Key.Key)
				result.NsqMessage.Requeue(5 * time.Minute)
			} else {
				// Too many failures. No more retries. Get the admin to see what's up.
				result.NsqMessage.Finish()
			}
		} else {
			// Prepare succeeded. Send this off to storage queue,
			// so the generic files can go into long-term storage.
			bagPreparer.SendToStorageQueue(helper)
			result.NsqMessage.Finish()
		}

		// We're done processing this, so remove it from the map.
		// If it comes in again, we'll reprocess it again.
		bagPreparer.ProcUtil.UnregisterItem(result.S3File.BagName())
		if bagPreparer.largeFile1 == result.S3File.BagName() {
			bagPreparer.ProcUtil.MessageLog.Info("Done with largeFile1 %s", result.S3File.Key.Key)
			bagPreparer.largeFile1 = ""
		} else if bagPreparer.largeFile2 == result.S3File.BagName() {
			bagPreparer.ProcUtil.MessageLog.Info("Done with largeFile2 %s", result.S3File.Key.Key)
			bagPreparer.largeFile2 = ""
		}
	}
}

func (bagPreparer *BagPreparer) cleanupBag(helper *bagman.IngestHelper) {
	result := helper.Result
	if result.ErrorMessage == "" {
		// Clean up the tar file, but leave the unpacked files
		// for apt_store to send off to long-term storage.
		err := os.Remove(result.FetchResult.LocalFile)
		if err != nil {
			bagPreparer.ProcUtil.MessageLog.Error("Error deleting tar file %s: %v",
				result.FetchResult.LocalFile, err)
		}
	} else {
		// Clean up ALL files we downloaded and unpacked
		errors := helper.DeleteLocalFiles()
		if errors != nil && len(errors) > 0 {
			bagPreparer.ProcUtil.MessageLog.Warning("Errors cleaning up %s",
				result.FetchResult.LocalFile)
			for _, e := range errors {
				bagPreparer.ProcUtil.MessageLog.Error(e.Error())
			}
		}
	}
	bagPreparer.ProcUtil.Volume.Release(uint64(result.S3File.Key.Size * 2))
}


// Puts an item into the queue for Fluctus/Fedora metadata processing.
func (bagPreparer *BagPreparer) SendToStorageQueue(helper *bagman.IngestHelper) {
	err := bagman.Enqueue(helper.ProcUtil.Config.NsqdHttpAddress,
		helper.ProcUtil.Config.StoreWorker.NsqTopic, helper.Result)
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
