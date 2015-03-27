// bagstorer stores bags that have been unpacked and validated
// by apt_prepare. Each item/message follows this flow:
//
// 1. Storage channel: copies files to S3 permanent storage.
// 2. Results channel: tells the queue whether processing
//    succeeded, and if not, whether the item should be requeued.
//    Also logs results to json and message logs.
// 3. Cleanup channel: cleans up the files after processing
//    completes.
//
// If a failure occurs anywhere in the first step,
// processing goes directly to the Results Channel, which
// records the error and the disposition (retry/give up).
//
// As long as the message from nsq contains valid JSON,
// steps 2 and 3 ALWAYS run.
package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"path/filepath"
	"time"
)

type BagStorer struct {
	StorageChannel chan *bagman.IngestHelper
	CleanUpChannel chan *bagman.IngestHelper
	ResultsChannel chan *bagman.IngestHelper
	ProcUtil            *bagman.ProcessUtil
}

func NewBagStorer(procUtil *bagman.ProcessUtil) (*BagStorer) {
	bagStorer := &BagStorer{
		ProcUtil: procUtil,
	}
	workerBufferSize := procUtil.Config.StoreWorker.Workers * 10
	bagStorer.StorageChannel = make(chan *bagman.IngestHelper, workerBufferSize)
	bagStorer.CleanUpChannel = make(chan *bagman.IngestHelper, workerBufferSize)
	bagStorer.ResultsChannel = make(chan *bagman.IngestHelper, workerBufferSize)
	for i := 0; i < procUtil.Config.StoreWorker.Workers; i++ {
		go bagStorer.saveToStorage()
		go bagStorer.logResult()
		go bagStorer.doCleanUp()
	}
	return bagStorer
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (bagStorer *BagStorer) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var result bagman.ProcessResult
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		bagStorer.ProcUtil.MessageLog.Error("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		message.Finish()
		return fmt.Errorf("Could not unmarshal JSON data from nsq")
	}

	if result.BagReadResult == nil {
		bagStorer.ProcUtil.MessageLog.Error("Result.BagReadResult is nil")
		message.Finish()
		return fmt.Errorf("Result.BagReadResult is nil")
	}

	// Special case for very large bags: the bag is in process under
	// the same ID. NSQ thinks it timed out and has re-sent it. In this
	// case, return nil so NSQ knows we're OK, but don't finish the message.
	// The original process will call Finish() on the message when it's
	// done. If we call Finish() here, NSQ will throw a "not-in-flight"
	// error when the processor calls Finish() on the original message later.
	if bagStorer.ProcUtil.MessageIdFor(result.S3File.BagName()) != "" {
		bagStorer.ProcUtil.MessageLog.Info("Skipping bag %s: already in progress",
			result.S3File.Key.Key)
		message.Finish()
		return nil
	}

	// NOTE: This is commented out for now, so we can see if it is necessary.
	// It eats resources when bags are large (10,000+ files), and the validate
	// step in apt_prepare should ensure that all files are present.
	//
	// if bagStorer.allFilesExist(result.TarResult.OutputDir, result.TarResult.Files) == false {
	// 	bagStorer.ProcUtil.MessageLog.Error("Cannot process %s because of missing file(s)",
	// 		result.S3File.BagName())
	// 	message.Finish()
	// 	return fmt.Errorf("At least one data file does not exist")
	// }

	// Don't start working on a message that we're already working on.
	// Note that the key we include in the syncMap includes multipart
	// bag endings, so we can be working on ncsu.edu/obj.b1of2.tar and
	// ncsu.edu/obj.b2of2.tar at the same time. This is what we want.
	mapErr := bagStorer.ProcUtil.RegisterItem(result.S3File.BagName(), message.ID)
	if mapErr != nil {
		bagStorer.ProcUtil.MessageLog.Info("Marking %s as complete because the file is already "+
			"being processed under another message id.\n", result.S3File.Key.Key)
		message.Finish()
		return nil
	}

	// Create the result struct and pass it down the pipeline
	helper := bagman.NewIngestHelper(bagStorer.ProcUtil, message, result.S3File)
	helper.Result = &result
	helper.Result.NsqMessage = message
	bagStorer.StorageChannel <- helper
	bagStorer.ProcUtil.MessageLog.Debug("Put %s into storage queue", result.S3File.Key.Key)
	return nil
}


// -- Step 1 of 3 --
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
func (bagStorer *BagStorer) saveToStorage() {
	for helper := range bagStorer.StorageChannel {
		// Touch before and after sending generic files,
		// since that process can take a long time for large bags.
		helper.Result.NsqMessage.Touch()
		helper.UpdateFluctusStatus(bagman.StageStore, bagman.StatusStarted)
		err := helper.SaveGenericFiles()
		helper.Result.NsqMessage.Touch()
		if err != nil {
			bagStorer.ResultsChannel <- helper
			continue
		}
		// If there were no errors, put this into the metadata
		// queue, so we can record the events in Fluctus.
		if helper.Result.ErrorMessage == "" {
			helper.UpdateFluctusStatus(bagman.StageStore, bagman.StatusPending)
			bagStorer.SendToMetadataQueue(helper)
		}

		// Pass problem cases off to the trouble queue
		if helper.IncompleteCopyToS3() || helper.FailedAndNoMoreRetries() {
			bagStorer.SendToTroubleQueue(helper)
		}

		// Record the results.
		bagStorer.ResultsChannel <- helper
	}
}


// -- Step 2 of 3 --
// This prints to the log the result of the program's attempt to fetch,
// untar, unbag and verify an individual S3 tar file. It logs state info
// about this bag to a json file on the local file system. Also logs
// a text message to the local bag_processor.log file and sends info
// to Fluctus saying whether the bag succeeded or failed.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func (bagStorer *BagStorer) logResult() {
	for helper := range bagStorer.ResultsChannel {
		helper.LogResult()
		bagStorer.CleanUpChannel <- helper
	}
}

// -- Step 3 of 3 --
// This runs as a go routine to remove the files we downloaded
// and untarred.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func (bagStorer *BagStorer) doCleanUp() {
	for helper := range bagStorer.CleanUpChannel {
		result := helper.Result
		result.NsqMessage.Touch()
		bagStorer.ProcUtil.MessageLog.Debug("Cleaning up %s", result.S3File.Key.Key)
		if (result.S3File.Key.Key != "" && result.FetchResult != nil &&
			result.FetchResult.LocalFile != "") {
			// Clean up any files we downloaded and unpacked
			errors := helper.DeleteLocalFiles()
			if errors != nil && len(errors) > 0 {
				bagStorer.ProcUtil.MessageLog.Warning("Errors cleaning up %s",
					result.FetchResult.LocalFile)
				for _, e := range errors {
					bagStorer.ProcUtil.MessageLog.Error(e.Error())
				}
			}
		}

		// Build and send message back to NSQ, indicating whether
		// processing succeeded.
		if result.ErrorMessage != "" && result.Retry == true {
			bagStorer.ProcUtil.MessageLog.Info("Requeueing %s", result.S3File.Key.Key)
			result.NsqMessage.Requeue(5 * time.Minute)
		} else {
			result.NsqMessage.Finish()
		}

		// We're done processing this, so remove it from the map.
		// If it comes in again, we'll reprocess it again.
		bagStorer.ProcUtil.UnregisterItem(result.S3File.BagName())
	}
}


// Puts an item into the queue for Fluctus/Fedora metadata processing.
func (bagStorer *BagStorer) SendToMetadataQueue(helper *bagman.IngestHelper) {
	err := bagman.Enqueue(helper.ProcUtil.Config.NsqdHttpAddress,
		helper.ProcUtil.Config.RecordWorker.NsqTopic, helper.Result)
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
func (bagStorer *BagStorer) SendToTroubleQueue(helper *bagman.IngestHelper) {
	err := bagman.Enqueue(helper.ProcUtil.Config.NsqdHttpAddress,
		helper.ProcUtil.Config.TroubleWorker.NsqTopic, helper.Result)
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

func (bagStorer *BagStorer) allFilesExist(rootDir string, files []*bagman.File) (bool) {
	for _, file := range files {
		absPath := filepath.Join(rootDir, file.Path)
		if bagman.FileExists(absPath) == false {
			bagStorer.ProcUtil.MessageLog.Error("File %s does not exist", absPath)
			return false
		}
	}
	return true
}
