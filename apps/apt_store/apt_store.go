package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"github.com/APTrust/bagman/ingesthelper"
	"github.com/bitly/go-nsq"
	"path/filepath"
	"time"
)

type Channels struct {
	StorageChannel chan *ingesthelper.IngestHelper
	CleanUpChannel chan *ingesthelper.IngestHelper
	ResultsChannel chan *ingesthelper.IngestHelper
}

// Global vars.
var procUtil *processutil.ProcessUtil
var channels *Channels

// apt_store stores bags that have been unpacked and validated
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
func main() {
	requestedConfig := flag.String("config", "", "Configuration to run. Options are in config.json file. REQUIRED")
	customEnvFile := flag.String("env", "", "Absolute path to file containing custom environment vars. OPTIONAL")
	flag.Parse()
	procUtil = processutil.NewProcessUtil(requestedConfig)
	procUtil.LoadCustomEnv(customEnvFile)

	procUtil.MessageLog.Info("apt_store started")
	err := procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}

	initChannels()
	initGoRoutines()

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(procUtil.Config.MaxStoreAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "180m")
	consumer, err := nsq.NewConsumer(procUtil.Config.StoreTopic,
		procUtil.Config.StoreChannel, nsqConfig)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}

	handler := &APTStore{}
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
	workerBufferSize := procUtil.Config.StoreWorkers * 10

	channels = &Channels{}
	channels.StorageChannel = make(chan *ingesthelper.IngestHelper, workerBufferSize)
	channels.CleanUpChannel = make(chan *ingesthelper.IngestHelper, workerBufferSize)
	channels.ResultsChannel = make(chan *ingesthelper.IngestHelper, workerBufferSize)
}

// Set up our go routines. We do NOT want one go routine per
// S3 file. If we do that, the system will run out of file handles,
// as we'll have tens of thousands of open connections to S3
// trying to write data into tens of thousands of local files.
func initGoRoutines() {
	for i := 0; i < procUtil.Config.StoreWorkers; i++ {
		go saveToStorage()
		go logResult()
		go doCleanUp()
	}
}

type APTStore struct {
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*APTStore) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var result bagman.ProcessResult
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		procUtil.MessageLog.Error("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		message.Finish()
		return fmt.Errorf("Could not unmarshal JSON data from nsq")
	}

	if result.BagReadResult == nil {
		procUtil.MessageLog.Error("Result.BagReadResult is nil")
		message.Finish()
		return fmt.Errorf("Result.BagReadResult is nil")
	}

	// Special case for very large bags: the bag is in process under
	// the same ID. NSQ thinks it timed out and has re-sent it. In this
	// case, return nil so NSQ knows we're OK, but don't finish the message.
	// The original process will call Finish() on the message when it's
	// done. If we call Finish() here, NSQ will throw a "not-in-flight"
	// error when the processor calls Finish() on the original message later.
	if procUtil.MessageIdFor(result.S3File.BagName()) != "" {
		procUtil.MessageLog.Info("Skipping bag %s: already in progress",
			result.S3File.Key.Key)
		message.Finish()
		return nil
	}

	// NOTE: This is commented out for now, so we can see if it is necessary.
	// It eats resources when bags are large (10,000+ files), and the validate
	// step in apt_prepare should ensure that all files are present.
	//
	// if allFilesExist(result.TarResult.OutputDir, result.TarResult.GenericFiles) == false {
	// 	procUtil.MessageLog.Error("Cannot process %s because of missing file(s)",
	// 		result.S3File.BagName())
	// 	message.Finish()
	// 	return fmt.Errorf("At least one data file does not exist")
	// }

	// Don't start working on a message that we're already working on.
	// Note that the key we include in the syncMap includes multipart
	// bag endings, so we can be working on ncsu.edu/obj.b1of2.tar and
	// ncsu.edu/obj.b2of2.tar at the same time. This is what we want.
	mapErr := procUtil.RegisterItem(result.S3File.BagName(), message.ID)
	if mapErr != nil {
		procUtil.MessageLog.Info("Marking %s as complete because the file is already "+
			"being processed under another message id.\n", result.S3File.Key.Key)
		message.Finish()
		return nil
	}

	// Create the result struct and pass it down the pipeline
	helper := ingesthelper.NewIngestHelper(procUtil, message, result.S3File)
	helper.Result = &result
	helper.Result.NsqMessage = message
	channels.StorageChannel <- helper
	procUtil.MessageLog.Debug("Put %s into storage queue", result.S3File.Key.Key)
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
func saveToStorage() {
	for helper := range channels.StorageChannel {
		// Touch before and after sending generic files,
		// since that process can take a long time for large bags.
		helper.Result.NsqMessage.Touch()
		helper.UpdateFluctusStatus(bagman.StageStore, bagman.StatusStarted)
		err := helper.SaveGenericFiles()
		helper.Result.NsqMessage.Touch()
		if err != nil {
			channels.ResultsChannel <- helper
			continue
		}
		// If there were no errors, put this into the metadata
		// queue, so we can record the events in Fluctus.
		if helper.Result.ErrorMessage == "" {
			helper.UpdateFluctusStatus(bagman.StageStore, bagman.StatusPending)
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


// -- Step 2 of 3 --
// This prints to the log the result of the program's attempt to fetch,
// untar, unbag and verify an individual S3 tar file. It logs state info
// about this bag to a json file on the local file system. Also logs
// a text message to the local bag_processor.log file and sends info
// to Fluctus saying whether the bag succeeded or failed.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func logResult() {
	for helper := range channels.ResultsChannel {
		helper.LogResult()
		channels.CleanUpChannel <- helper
	}
}

// -- Step 3 of 3 --
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

func allFilesExist(rootDir string, genericFiles []*bagman.GenericFile) (bool) {
	for _, gf := range genericFiles {
		absPath := filepath.Join(rootDir, gf.Path)
		if bagman.FileExists(absPath) == false {
			procUtil.MessageLog.Error("File %s does not exist", absPath)
			return false
		}
	}
	return true
}
