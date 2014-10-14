// filedelete.go deletes files from the S3 preservation bucket
// at the request of users/admins.

package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"time"
)

// DeleteObject holds information about the state of a
// single delete operation.
type DeleteObject struct {
	GenericFile     *bagman.GenericFile
	ProcessStatus   *bagman.ProcessStatus
	NsqMessage      *nsq.Message
	ErrorMessage    string
	Retry           bool
}

type FileDeleter struct {
	DeleteChannel  chan *DeleteObject
	ResultsChannel chan *DeleteObject
	ProcUtil       *bagman.ProcessUtil
}


func NewFileDeleter(procUtil *bagman.ProcessUtil) (*FileDeleter) {
	fileDeleter := &FileDeleter{
		ProcUtil: procUtil,
	}
	workerBufferSize := procUtil.Config.FileDeleteWorker.Workers * 10
	fileDeleter.DeleteChannel = make(chan *DeleteObject, workerBufferSize)
	fileDeleter.ResultsChannel = make(chan *DeleteObject, workerBufferSize)
	for i := 0; i < procUtil.Config.FileDeleteWorker.Workers; i++ {
		go fileDeleter.logResult()
		go fileDeleter.doDelete()
	}
	return fileDeleter
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (fileDeleter *FileDeleter) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	// Deserialize the NSQ JSON message into object.ProcessStatus
	processStatus := &bagman.ProcessStatus{}
	err := json.Unmarshal(message.Body, processStatus)
	if err != nil {
		detailedError := fmt.Errorf(
			"Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		fileDeleter.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	if processStatus.GenericFileIdentifier == "" {
		detailedError := fmt.Errorf("ProcessedItem has no GenericFileIdentifier")
		fileDeleter.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// If this item has already been deleted, or is in process
	// of being deleted, just finish the message and return.
	items, err := fileDeleter.ProcUtil.FluctusClient.DeletionItemsGet(processStatus.GenericFileIdentifier)
	if err != nil {
		detailedError := fmt.Errorf(
			"Could not get current status of delete request %s " +
				"from Fluctus: %v.", processStatus.GenericFileIdentifier, err)
		fileDeleter.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	if len(items) == 0 {
		fileDeleter.ProcUtil.MessageLog.Info("Marking %s as complete because Fluctus says it "+
			"has been deleted, or deletion should not be retried",
			processStatus.GenericFileIdentifier)
		message.Finish()
		return nil
	}

	// Make a note that we're working on this item
	err = fileDeleter.ProcUtil.RegisterItem(processStatus.GenericFileIdentifier, message.ID)
	if err != nil {
		fileDeleter.ProcUtil.MessageLog.Info("Marking %s as complete because the file is already "+
			"being deleted under another message id.", processStatus.GenericFileIdentifier)
		message.Finish()
		return nil
	}

	// Get the GenericFile
	genericFile, err := fileDeleter.ProcUtil.FluctusClient.GenericFileGet(processStatus.GenericFileIdentifier, false)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not get GenericFile %s from Fluctus: %v",
			processStatus.GenericFileIdentifier, err)
		fileDeleter.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// Make sure this file is actually deletable
	if genericFile.URI == "" {
		detailedError := fmt.Errorf("GenericFile with id %d has no preservation storage URI",
			genericFile.Id)
		fileDeleter.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	if err != nil {
		detailedError := fmt.Errorf("Cannot delete GenericFile: %v", err)
		fileDeleter.ProcUtil.MessageLog.Error(detailedError.Error())
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
	err = fileDeleter.ProcUtil.FluctusClient.UpdateProcessedItem(processStatus)
	if err != nil {
		detailedError := fmt.Errorf("Cannot register deletion start with Fluctus for %s: %v",
			processStatus.GenericFileIdentifier, err)
		deleteObject.ErrorMessage = detailedError.Error()
		fileDeleter.ResultsChannel <- &deleteObject
		return detailedError
	}

	// Now put the object into the channel for processing
	fileDeleter.DeleteChannel <- &deleteObject
	fileDeleter.ProcUtil.MessageLog.Info("Put %s into delete channel", processStatus.GenericFileIdentifier)
	return nil
}

func (fileDeleter *FileDeleter) logResult() {
	for deleteObject := range fileDeleter.ResultsChannel {
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
		err := fileDeleter.ProcUtil.FluctusClient.UpdateProcessedItem(deleteObject.ProcessStatus)
		if err != nil {
			fileDeleter.ProcUtil.MessageLog.Error(
				"Requeueing %s because attempt to update status in Fluctus failed: %v",
				deleteObject.ProcessStatus.GenericFileIdentifier, err)
			deleteObject.NsqMessage.Requeue(1 * time.Minute)
			fileDeleter.ProcUtil.IncrementFailed()
		} else if deleteObject.ErrorMessage != "" {
			fileDeleter.ProcUtil.MessageLog.Error("Requeueing %s: %s",
				deleteObject.ProcessStatus.GenericFileIdentifier, deleteObject.ErrorMessage)
			deleteObject.NsqMessage.Requeue(1 * time.Minute)
			fileDeleter.ProcUtil.IncrementFailed()
		} else {
			fileDeleter.ProcUtil.MessageLog.Info("Deletion of %s succeeded",
				deleteObject.ProcessStatus.GenericFileIdentifier)
			deleteObject.NsqMessage.Finish()
			fileDeleter.ProcUtil.IncrementSucceeded()
		}
		// No longer working on this
		fileDeleter.ProcUtil.UnregisterItem(deleteObject.ProcessStatus.GenericFileIdentifier)
		fileDeleter.ProcUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
			fileDeleter.ProcUtil.Succeeded(), fileDeleter.ProcUtil.Failed())
	}
}


func (fileDeleter *FileDeleter) doDelete() {
	for deleteObject := range fileDeleter.DeleteChannel {
		// Make sure it's deletable
		fileName, err := deleteObject.GenericFile.PreservationStorageFileName()
		if err != nil {
			deleteObject.ErrorMessage = err.Error()
			fileDeleter.ResultsChannel <- deleteObject
			continue
		}
		// Delete it
		fileDeleter.ProcUtil.MessageLog.Info("Deleting %s from %s/%s",
			deleteObject.ProcessStatus.GenericFileIdentifier,
			fileDeleter.ProcUtil.Config.PreservationBucket,
			fileName)
		err = fileDeleter.ProcUtil.S3Client.Delete(fileDeleter.ProcUtil.Config.PreservationBucket, fileName)
		if err != nil {
			deleteObject.ErrorMessage = fmt.Sprintf(
				"An error occurred during the deletion process: %v", err)
		}
		fileDeleter.ResultsChannel <- deleteObject
	}
}
