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
	"github.com/nsqio/go-nsq"
	"time"
)

type BagDeleter struct {
	CleanupChannel chan *bagman.CleanupResult
	ResultsChannel chan *bagman.CleanupResult
	ProcUtil       *bagman.ProcessUtil
}

func NewBagDeleter(procUtil *bagman.ProcessUtil) (*BagDeleter) {
	bagDeleter := &BagDeleter{
		ProcUtil: procUtil,
	}
	workerBufferSize := procUtil.Config.BagDeleteWorker.Workers * 10
	bagDeleter.CleanupChannel = make(chan *bagman.CleanupResult, workerBufferSize)
	bagDeleter.ResultsChannel = make(chan *bagman.CleanupResult, workerBufferSize)
	for i := 0; i < procUtil.Config.BagDeleteWorker.Workers; i++ {
		go bagDeleter.logResult()
		go bagDeleter.doCleanUp()
	}
	return bagDeleter
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (bagDeleter *BagDeleter) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var result bagman.CleanupResult
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		bagDeleter.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	result.NsqMessage = message
	bagDeleter.CleanupChannel <- &result
	bagDeleter.ProcUtil.MessageLog.Info("Put %s into cleanup channel", result.BagName)
	return nil
}

// TODO: Don't requeue if config.DeleteOnSuccess == false
func (bagDeleter *BagDeleter) logResult() {
	for result := range bagDeleter.ResultsChannel {
		// Log full results to the JSON log
		json, err := json.Marshal(result)
		if err != nil {
			bagDeleter.ProcUtil.MessageLog.Error(err.Error())
			bagDeleter.ProcUtil.MessageLog.Info("Requeueing %s due to error", result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
			bagDeleter.ProcUtil.IncrementFailed()
			bagDeleter.logStats()
			continue
		}
		bagDeleter.ProcUtil.JsonLog.Println(string(json))

		// Log & requeue if something failed.
		if result.Succeeded() == false {
			bagDeleter.ProcUtil.MessageLog.Info("Requeueing %s because at least one S3 delete failed",
				result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
			bagDeleter.ProcUtil.IncrementFailed()
			bagDeleter.logStats()
			continue
		}

		// Mark item as resolved in Fluctus & tell the queue what happened
		err = bagDeleter.MarkItemResolved(result)
		if err != nil {
			// TODO: This will just get retried forever, won't it?
			bagDeleter.ProcUtil.MessageLog.Error("Requeueing %s because we could not update Fluctus",
				result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
			bagDeleter.ProcUtil.IncrementFailed()
		} else {
			bagDeleter.ProcUtil.MessageLog.Info("Cleanup of %s succeeded", result.BagName)
			result.NsqMessage.Finish()
			bagDeleter.ProcUtil.IncrementSucceeded()
		}
		bagDeleter.logStats()
	}
}

func (bagDeleter *BagDeleter) logStats() {
	bagDeleter.ProcUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
		bagDeleter.ProcUtil.Succeeded(), bagDeleter.ProcUtil.Failed())
}

func (bagDeleter *BagDeleter) doCleanUp() {
	for result := range bagDeleter.CleanupChannel {
		bagDeleter.ProcUtil.MessageLog.Info("Cleaning up %s", result.BagName)
		if bagDeleter.ProcUtil.Config.DeleteOnSuccess == true {
			bagDeleter.DeleteS3Files(result)
		} else {
			for i := range result.Files {
				file := result.Files[i]
				file.DeleteSkippedPerConfig = true
			}
			bagDeleter.ProcUtil.MessageLog.Info("Not deleting %s because " +
				"config.DeleteOnSuccess == false", result.BagName)
		}
		bagDeleter.ResultsChannel <- result
	}
}

// Deletes each item in result.Files from S3.
func (bagDeleter *BagDeleter) DeleteS3Files(result *bagman.CleanupResult) {
	for i := range result.Files {
		file := result.Files[i]
		err := bagDeleter.ProcUtil.S3Client.Delete(file.BucketName, file.Key)
		if err != nil {
			file.ErrorMessage += fmt.Sprintf("Error deleting file '%s' from "+
				"bucket '%s': %v ", file.Key, file.BucketName)
			bagDeleter.ProcUtil.MessageLog.Error(file.ErrorMessage)
		} else {
			file.DeletedAt = time.Now()
			bagDeleter.ProcUtil.MessageLog.Info("Deleted original file '%s' from bucket '%s'",
				file.Key, file.BucketName)
		}
	}
}

// Tell Fluctus this ProcessedItem is resolved
func (bagDeleter *BagDeleter) MarkItemResolved(result *bagman.CleanupResult) error {
	remoteStatus, err := bagDeleter.ProcUtil.FluctusClient.GetBagStatus(
		result.ETag, result.BagName, result.BagDate)
	if err != nil {
		bagDeleter.ProcUtil.MessageLog.Error("Error getting ProcessedItem to Fluctus: %s",
			err.Error())
		return err
	}
	if remoteStatus != nil {
		remoteStatus.Stage = bagman.StageCleanup
		remoteStatus.Status = bagman.StatusSuccess
	}
	err = bagDeleter.ProcUtil.FluctusClient.UpdateProcessedItem(remoteStatus)
	if err != nil {
		bagDeleter.ProcUtil.MessageLog.Error("Error sending ProcessedItem to Fluctus: %s", err.Error())
	} else {
		bagDeleter.ProcUtil.MessageLog.Info("Updated status in Fluctus for %s: %s/%s\n",
			remoteStatus.Name, remoteStatus.Stage, remoteStatus.Status)
	}
	return err
}
