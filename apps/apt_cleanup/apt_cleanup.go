/*
cleanup.go deletes tar files from the partners' S3 receiving buckets
after those files have been successfully ingested.

If you want to clean up failed bits of multipart S3 uploads in the
preservation bucket, see multiclean.go.
*/
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"github.com/bitly/go-nsq"
	"time"
)

type Channels struct {
	CleanupChannel chan *bagman.CleanupResult
	ResultsChannel chan *bagman.CleanupResult
}

// Global vars.
var channels *Channels
var procUtil *processutil.ProcessUtil

func main() {
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	procUtil = processutil.NewProcessUtil(requestedConfig)

	procUtil.MessageLog.Info("Cleanup started")
	err := procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}

	initChannels()
	initGoRoutines()

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(procUtil.Config.MaxCleanupAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "30m")
	consumer, err := nsq.NewConsumer(procUtil.Config.CleanupTopic,
		procUtil.Config.CleanupChannel, nsqConfig)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}

	handler := &CleanupProcessor{}
	consumer.SetHandler(handler)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}

// Set up the channels.
func initChannels() {
	workerBufferSize := procUtil.Config.CleanupWorkers * 10
	channels = &Channels{}
	channels.CleanupChannel = make(chan *bagman.CleanupResult, workerBufferSize)
	channels.ResultsChannel = make(chan *bagman.CleanupResult, workerBufferSize)
}

// Set up our go routines. We want to limit the number of
// go routines so we do not have 1000+ simultaneous connections
// to Fluctus. That would just cause Fluctus to crash.
func initGoRoutines() {
	for i := 0; i < procUtil.Config.CleanupWorkers; i++ {
		go logResult()
		go doCleanUp()
	}
}

type CleanupProcessor struct {
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*CleanupProcessor) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var result bagman.CleanupResult
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		procUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	result.NsqMessage = message
	channels.CleanupChannel <- &result
	procUtil.MessageLog.Info("Put %s into cleanup channel", result.BagName)
	return nil
}

// TODO: Don't requeue if config.DeleteOnSuccess == false
func logResult() {
	for result := range channels.ResultsChannel {
		// Log full results to the JSON log
		json, err := json.Marshal(result)
		if err != nil {
			procUtil.MessageLog.Error(err.Error())
			procUtil.MessageLog.Info("Requeueing %s due to error", result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
			procUtil.IncrementFailed()
			logStats()
			continue
		}
		procUtil.JsonLog.Println(string(json))

		// Log & requeue if something failed.
		if result.Succeeded() == false {
			procUtil.MessageLog.Info("Requeueing %s because at least one S3 delete failed",
				result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
			procUtil.IncrementFailed()
			logStats()
			continue
		}

		// Mark item as resolved in Fluctus & tell the queue what happened
		err = MarkItemResolved(result)
		if err != nil {
			// TODO: This will just get retried forever, won't it?
			procUtil.MessageLog.Error("Requeueing %s because we could not update Fluctus",
				result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
			procUtil.IncrementFailed()
		} else {
			procUtil.MessageLog.Info("Cleanup of %s succeeded", result.BagName)
			result.NsqMessage.Finish()
			procUtil.IncrementSucceeded()
		}
		logStats()
	}
}

func logStats() {
	procUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d", procUtil.Succeeded(), procUtil.Failed())
}

func doCleanUp() {
	for result := range channels.CleanupChannel {
		procUtil.MessageLog.Info("Cleaning up %s", result.BagName)
		if procUtil.Config.DeleteOnSuccess == true {
			DeleteS3Files(result)
		} else {
			// For testing...
			// result.Files[0].DeletedAt = time.Now()
			procUtil.MessageLog.Info("Not deleting %s because config.DeleteOnSuccess == false", result.BagName)
		}
		channels.ResultsChannel <- result
	}
}

// Deletes each item in result.Files from S3.
func DeleteS3Files(result *bagman.CleanupResult) {
	for i := range result.Files {
		file := result.Files[i]
		err := procUtil.S3Client.Delete(file.BucketName, file.Key)
		if err != nil {
			file.ErrorMessage += fmt.Sprintf("Error deleting file '%s' from "+
				"bucket '%s': %v ", file.Key, file.BucketName)
			procUtil.MessageLog.Error(file.ErrorMessage)
		} else {
			file.DeletedAt = time.Now()
			procUtil.MessageLog.Info("Deleted original file '%s' from bucket '%s'",
				file.Key, file.BucketName)
		}
	}
}

// Tell Fluctus this ProcessedItem is resolved
func MarkItemResolved(result *bagman.CleanupResult) error {
	remoteStatus, err := procUtil.FluctusClient.GetBagStatus(
		result.ETag, result.BagName, result.BagDate)
	if err != nil {
		procUtil.MessageLog.Error("Error getting ProcessedItem to Fluctus: %s", err.Error())
		return err
	}
	if remoteStatus != nil {
		remoteStatus.Reviewed = false
		remoteStatus.Stage = bagman.StageCleanup
		remoteStatus.Status = bagman.StatusSuccess
	}
	err = procUtil.FluctusClient.UpdateProcessedItem(remoteStatus)
	if err != nil {
		procUtil.MessageLog.Error("Error sending ProcessedItem to Fluctus: %s", err.Error())
	} else {
		procUtil.MessageLog.Info("Updated status in Fluctus for %s: %s/%s\n",
			remoteStatus.Name, remoteStatus.Stage, remoteStatus.Status)
	}
	return err
}
