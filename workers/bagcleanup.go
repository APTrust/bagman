/*
bagcleanup.go deletes tar files from the partners' S3 receiving
buckets after those files have been successfully ingested.

If you want to clean up failed bits of multipart S3 uploads in the
preservation bucket, see multiclean.go.
*/
package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"time"
)

type BagCleanup struct {
	CleanupChannel chan *bagman.CleanupResult
	ResultsChannel chan *bagman.CleanupResult
	ProcUtil       *bagman.ProcessUtil
}

func NewBagCleanup(procUtil *bagman.ProcessUtil) (*BagCleanup) {
	bagCleanup := &BagCleanup{
		ProcUtil: procUtil,
	}
	workerBufferSize := procUtil.Config.CleanupWorkers * 10
	bagCleanup.CleanupChannel = make(chan *bagman.CleanupResult, workerBufferSize)
	bagCleanup.ResultsChannel = make(chan *bagman.CleanupResult, workerBufferSize)
	for i := 0; i < procUtil.Config.CleanupWorkers; i++ {
		go bagCleanup.logResult()
		go bagCleanup.doCleanUp()
	}
	return bagCleanup
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (bagCleanup *BagCleanup) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var result bagman.CleanupResult
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		bagCleanup.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	result.NsqMessage = message
	bagCleanup.CleanupChannel <- &result
	bagCleanup.ProcUtil.MessageLog.Info("Put %s into cleanup channel", result.BagName)
	return nil
}

// TODO: Don't requeue if config.DeleteOnSuccess == false
func (bagCleanup *BagCleanup) logResult() {
	for result := range bagCleanup.ResultsChannel {
		// Log full results to the JSON log
		json, err := json.Marshal(result)
		if err != nil {
			bagCleanup.ProcUtil.MessageLog.Error(err.Error())
			bagCleanup.ProcUtil.MessageLog.Info("Requeueing %s due to error", result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
			bagCleanup.ProcUtil.IncrementFailed()
			bagCleanup.logStats()
			continue
		}
		bagCleanup.ProcUtil.JsonLog.Println(string(json))

		// Log & requeue if something failed.
		if result.Succeeded() == false {
			bagCleanup.ProcUtil.MessageLog.Info("Requeueing %s because at least one S3 delete failed",
				result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
			bagCleanup.ProcUtil.IncrementFailed()
			bagCleanup.logStats()
			continue
		}

		// Mark item as resolved in Fluctus & tell the queue what happened
		err = bagCleanup.MarkItemResolved(result)
		if err != nil {
			// TODO: This will just get retried forever, won't it?
			bagCleanup.ProcUtil.MessageLog.Error("Requeueing %s because we could not update Fluctus",
				result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
			bagCleanup.ProcUtil.IncrementFailed()
		} else {
			bagCleanup.ProcUtil.MessageLog.Info("Cleanup of %s succeeded", result.BagName)
			result.NsqMessage.Finish()
			bagCleanup.ProcUtil.IncrementSucceeded()
		}
		bagCleanup.logStats()
	}
}

func (bagCleanup *BagCleanup) logStats() {
	bagCleanup.ProcUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
		bagCleanup.ProcUtil.Succeeded(), bagCleanup.ProcUtil.Failed())
}

func (bagCleanup *BagCleanup) doCleanUp() {
	for result := range bagCleanup.CleanupChannel {
		bagCleanup.ProcUtil.MessageLog.Info("Cleaning up %s", result.BagName)
		if bagCleanup.ProcUtil.Config.DeleteOnSuccess == true {
			bagCleanup.DeleteS3Files(result)
		} else {
			bagCleanup.ProcUtil.MessageLog.Info("Not deleting %s because " +
				"config.DeleteOnSuccess == false", result.BagName)
		}
		bagCleanup.ResultsChannel <- result
	}
}

// Deletes each item in result.Files from S3.
func (bagCleanup *BagCleanup) DeleteS3Files(result *bagman.CleanupResult) {
	for i := range result.Files {
		file := result.Files[i]
		err := bagCleanup.ProcUtil.S3Client.Delete(file.BucketName, file.Key)
		if err != nil {
			file.ErrorMessage += fmt.Sprintf("Error deleting file '%s' from "+
				"bucket '%s': %v ", file.Key, file.BucketName)
			bagCleanup.ProcUtil.MessageLog.Error(file.ErrorMessage)
		} else {
			file.DeletedAt = time.Now()
			bagCleanup.ProcUtil.MessageLog.Info("Deleted original file '%s' from bucket '%s'",
				file.Key, file.BucketName)
		}
	}
}

// Tell Fluctus this ProcessedItem is resolved
func (bagCleanup *BagCleanup) MarkItemResolved(result *bagman.CleanupResult) error {
	remoteStatus, err := bagCleanup.ProcUtil.FluctusClient.GetBagStatus(
		result.ETag, result.BagName, result.BagDate)
	if err != nil {
		bagCleanup.ProcUtil.MessageLog.Error("Error getting ProcessedItem to Fluctus: %s",
			err.Error())
		return err
	}
	if remoteStatus != nil {
		remoteStatus.Reviewed = false
		remoteStatus.Stage = bagman.StageCleanup
		remoteStatus.Status = bagman.StatusSuccess
	}
	err = bagCleanup.ProcUtil.FluctusClient.UpdateProcessedItem(remoteStatus)
	if err != nil {
		bagCleanup.ProcUtil.MessageLog.Error("Error sending ProcessedItem to Fluctus: %s", err.Error())
	} else {
		bagCleanup.ProcUtil.MessageLog.Info("Updated status in Fluctus for %s: %s/%s\n",
			remoteStatus.Name, remoteStatus.Stage, remoteStatus.Status)
	}
	return err
}
