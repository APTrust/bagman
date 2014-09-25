package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"github.com/APTrust/bagman/fluctus/models"
	"github.com/bitly/go-nsq"
	"time"
)

type DeleteObject struct {
	GenericFile     *models.GenericFile
	ProcessStatus   *bagman.ProcessStatus
	NsqMessage      *nsq.Message
	ErrorMessage    string
	Retry           bool
}

type Channels struct {
	DeleteChannel chan *DeleteObject
	ResultsChannel chan *DeleteObject
}

// Global vars.
var channels *Channels
var procUtil *processutil.ProcessUtil

func main() {
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	procUtil = processutil.NewProcessUtil(requestedConfig)

	procUtil.MessageLog.Info("Delete processor started")
	err := procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}

	initChannels()
	initGoRoutines()

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(procUtil.Config.MaxDeleteAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "30m")
	consumer, err := nsq.NewConsumer(procUtil.Config.DeleteTopic,
		procUtil.Config.DeleteChannel, nsqConfig)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}

	handler := &DeleteProcessor{}
	consumer.SetHandler(handler)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}

// Set up the channels.
func initChannels() {
	workerBufferSize := procUtil.Config.DeleteWorkers * 10
	channels = &Channels{}
	channels.DeleteChannel = make(chan *DeleteObject, workerBufferSize)
	channels.ResultsChannel = make(chan *DeleteObject, workerBufferSize)
}

// Set up our go routines. We want to limit the number of
// go routines so we do not have 1000+ simultaneous connections
// to Fluctus. That would just cause Fluctus to crash.
func initGoRoutines() {
	for i := 0; i < procUtil.Config.DeleteWorkers; i++ {
		go logResult()
		go doDelete()
	}
}


type DeleteProcessor struct {
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*DeleteProcessor) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	// Deserialize the NSQ JSON message into object.ProcessStatus
	processStatus := &bagman.ProcessStatus{}
	err := json.Unmarshal(message.Body, processStatus)
	if err != nil {
		detailedError := fmt.Errorf(
			"Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		procUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	if processStatus.GenericFileIdentifier == "" {
		detailedError := fmt.Errorf("ProcessedItem has no GenericFileIdentifier")
		procUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// If this item has already been deleted, or is in process
	// of being deleted, just finish the message and return.
	items, err := procUtil.FluctusClient.DeletionItemsGet(processStatus.GenericFileIdentifier)
	if err != nil {
		detailedError := fmt.Errorf(
			"Could not get current status of delete request %s " +
				"from Fluctus: %v.", processStatus.GenericFileIdentifier, err)
		procUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	if len(items) == 0 {
		procUtil.MessageLog.Info("Marking %s as complete because Fluctus says it "+
			"has been deleted, or deletion should not be retried",
			processStatus.GenericFileIdentifier)
		message.Finish()
		return nil
	}

	// Make a note that we're working on this item
	err = procUtil.RegisterItem(processStatus.GenericFileIdentifier, message.ID)
	if err != nil {
		procUtil.MessageLog.Info("Marking %s as complete because the file is already "+
			"being deleted under another message id.", processStatus.GenericFileIdentifier)
		message.Finish()
		return nil
	}

	// Get the GenericFile
	genericFile, err := procUtil.FluctusClient.GenericFileGet(processStatus.GenericFileIdentifier, false)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not get GenericFile %s from Fluctus: %v",
			processStatus.GenericFileIdentifier, err)
		procUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// Make sure this file is actually deletable
	if genericFile.URI == "" {
		detailedError := fmt.Errorf("GenericFile with id %d has no preservation storage URI",
			genericFile.Id)
		procUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	if err != nil {
		detailedError := fmt.Errorf("Cannot delete GenericFile: %v", err)
		procUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	deleteObject := DeleteObject{
		NsqMessage: message,
		GenericFile: genericFile,
		ProcessStatus: processStatus,
		Retry: true,
	}

	// Let Fluctus know we're working on it.
	processStatus.Status = bagman.StatusStarted
	processStatus.Note = fmt.Sprintf("Attempting to delete generic file '%s' from '%s'",
		genericFile.Identifier, genericFile.URI)
	err = procUtil.FluctusClient.UpdateProcessedItem(processStatus)
	if err != nil {
		detailedError := fmt.Errorf("Cannot register deletion start with Fluctus for %s: %v",
			processStatus.GenericFileIdentifier, err)
		deleteObject.ErrorMessage = detailedError.Error()
		channels.ResultsChannel <- &deleteObject
		return detailedError
	}

	// Now put the object into the channel for processing
	channels.DeleteChannel <- &deleteObject
	procUtil.MessageLog.Info("Put %s into delete channel", processStatus.GenericFileIdentifier)
	return nil
}

func logResult() {
	for deleteObject := range channels.ResultsChannel {
		// Mark item as resolved in Fluctus & tell the queue what happened.
		var status bagman.StatusType = bagman.StatusSuccess
		var stage bagman.StageType = bagman.StageResolve
		if deleteObject.ErrorMessage != "" {
			status = bagman.StatusFailed
			stage = bagman.StageRequested
		}
		deleteObject.ProcessStatus.Status = status
		deleteObject.ProcessStatus.Stage = stage
		deleteObject.ProcessStatus.Note = fmt.Sprintf("Deleted generic file '%s' " +
			"from '%s' at %s at the request of %s",
			deleteObject.GenericFile.Identifier, deleteObject.GenericFile.URI,
			time.Now().Format(time.RFC3339), deleteObject.ProcessStatus.User)
		err := procUtil.FluctusClient.UpdateProcessedItem(deleteObject.ProcessStatus)
		if err != nil {
			procUtil.MessageLog.Error(
				"Requeueing %s because attempt to update status in Fluctus failed: %v",
				deleteObject.ProcessStatus.GenericFileIdentifier, err)
			deleteObject.NsqMessage.Requeue(1 * time.Minute)
			procUtil.IncrementFailed()
		} else if deleteObject.ErrorMessage != "" {
			procUtil.MessageLog.Error("Requeueing %s: %s",
				deleteObject.ProcessStatus.GenericFileIdentifier, deleteObject.ErrorMessage)
			deleteObject.NsqMessage.Requeue(1 * time.Minute)
			procUtil.IncrementFailed()
		} else {
			procUtil.MessageLog.Info("Deletion of %s succeeded",
				deleteObject.ProcessStatus.GenericFileIdentifier)
			deleteObject.NsqMessage.Finish()
			procUtil.IncrementSucceeded()
		}
		// No longer working on this
		procUtil.UnregisterItem(deleteObject.ProcessStatus.GenericFileIdentifier)
		procUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d", procUtil.Succeeded(), procUtil.Failed())
	}
}


func doDelete() {
	for deleteObject := range channels.DeleteChannel {
		// Make sure it's deletable
		fileName, err := deleteObject.GenericFile.PreservationStorageFileName()
		if err != nil {
			deleteObject.ErrorMessage = err.Error()
			channels.ResultsChannel <- deleteObject
			continue
		}
		// Delete it
		procUtil.MessageLog.Info("Deleting %s from %s/%s",
			deleteObject.ProcessStatus.GenericFileIdentifier,
			procUtil.Config.PreservationBucket,
			fileName)
		err = procUtil.S3Client.Delete(procUtil.Config.PreservationBucket, fileName)
		if err != nil {
			deleteObject.ErrorMessage = fmt.Sprintf(
				"An error occurred during the deletion process: %v", err)
		}
		channels.ResultsChannel <- deleteObject
	}
}
