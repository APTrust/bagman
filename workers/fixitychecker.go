/*
fixityworker.go receives GenericFile records from the fixity queue.
It downloads the generic files from S3 preservation storage,
calculates the files' SHA256 checksums and writes the results back
to Fluctus. None of the data downloaded from S3 is saved to disk;
it's simply streamed through the SHA256 hash writer and then discarded.
*/
package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"strings"
	"time"
)

type FixityChecker struct {
	FixityChannel  chan *bagman.FixityResult
	ResultsChannel chan *bagman.FixityResult
	ProcUtil       *bagman.ProcessUtil
}

func NewFixityChecker(procUtil *bagman.ProcessUtil) (*FixityChecker) {
	fixityChecker := &FixityChecker{
		ProcUtil: procUtil,
	}
	workerBufferSize := procUtil.Config.BagDeleteWorker.Workers * 10
	fixityChecker.FixityChannel = make(chan *bagman.FixityResult, workerBufferSize)
	fixityChecker.ResultsChannel = make(chan *bagman.FixityResult, workerBufferSize)
	for i := 0; i < procUtil.Config.BagDeleteWorker.Workers; i++ {
		go fixityChecker.logResult()
		go fixityChecker.checkFile()
	}
	return fixityChecker
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (fixityChecker *FixityChecker) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var genericFile bagman.GenericFile
	err := json.Unmarshal(message.Body, &genericFile)
	if err != nil {
		detailedError := fmt.Errorf(
			"Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		fixityChecker.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	if genericFile.GetChecksum("sha256") == nil {
		detailedError := fmt.Errorf(
			"Cannot check GenericFile %s: it has no SHA256 checksum!",
			genericFile.Identifier)
		fixityChecker.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	result := bagman.NewFixityResult(&genericFile)
	result.NsqMessage = message
	fixityChecker.FixityChannel <- result
	fixityChecker.ProcUtil.MessageLog.Info("Put %s into fixity channel", genericFile.Identifier)
	return nil
}

func (fixityChecker *FixityChecker) logResult() {
	for result := range fixityChecker.ResultsChannel {
		fixityChecker.ProcUtil.MessageLog.Debug("Fedora digest = '%s' ... S3 digest = '%s'",
			result.FedoraSha256(), result.Sha256)
		if result.S3FileExists == false {
			fixityChecker.ProcUtil.MessageLog.Error("GenericFile '%s' with URL '%s' does not exist in S3",
				result.GenericFile.Identifier, result.GenericFile.URI)
			result.NsqMessage.Finish()
			fixityChecker.ProcUtil.IncrementFailed()
			fixityChecker.logStats()
			continue
		}
		// Check failure cases...
		if result.GotDigestFromPreservationFile() == false && result.Retry == true {
			if result.NsqMessage.Attempts >= uint16(fixityChecker.ProcUtil.Config.FixityWorker.MaxAttempts) {
				fixityChecker.ProcUtil.MessageLog.Error(
					"Attempt to calculate checksum for file %s at S3 URL %s has failed too many times. " +
						"This item will not be requeued.",
					result.GenericFile.Identifier,
					result.GenericFile.URI)
				// Too many failures. Send to trouble queue.
				err := bagman.Enqueue(fixityChecker.ProcUtil.Config.NsqdHttpAddress,
					fixityChecker.ProcUtil.Config.FailedFixityWorker.NsqTopic, result)
				if err != nil {
					fixityChecker.ProcUtil.MessageLog.Error("Could not send '%s' to trouble queue: %v",
						result.GenericFile.Identifier, err)
				}
				result.NsqMessage.Finish()
				fixityChecker.ProcUtil.IncrementFailed()
				fixityChecker.logStats()
			} else {
				fixityChecker.ProcUtil.MessageLog.Error(
					"Requeueing %s because fetch from S3 failed, or read from S3 " +
						"datastream failed while calculating checksum. This item is being requeued.",
					result.GenericFile.Identifier)
				result.NsqMessage.Requeue(1 * time.Minute)
				fixityChecker.ProcUtil.IncrementFailed()
				fixityChecker.logStats()
			}
			continue
		}

		// If we got this far, we have enough info to compare checksums.
		eventSaved := fixityChecker.savePremisEvent(result)
		if eventSaved == false {
			fixityChecker.ProcUtil.MessageLog.Error(
				"Requeueing %s because attempt to save event to Fluctus failed",
				result.GenericFile.Identifier)
			result.NsqMessage.Requeue(1 * time.Minute)
			fixityChecker.ProcUtil.IncrementFailed()
		} else {
			fixityChecker.ProcUtil.MessageLog.Info("Finished with %s", result.GenericFile.Identifier)
			fixityChecker.ProcUtil.IncrementSucceeded()
			result.NsqMessage.Finish()
		}
		fixityChecker.logStats()
	}
}

func (fixityChecker *FixityChecker) savePremisEvent(fixityResult *bagman.FixityResult) (bool) {
	premisEvent, err := fixityResult.BuildPremisEvent()
	if err != nil {
		fixityChecker.ProcUtil.MessageLog.Error("Error building PremisEvent for %s: %v",
			fixityResult.GenericFile.Identifier, err)
		return false
	} else {
		_, err := fixityChecker.ProcUtil.FluctusClient.PremisEventSave(
			fixityResult.GenericFile.Identifier,
			"GenericFile",
			premisEvent)
		if err != nil {
			fixityChecker.ProcUtil.MessageLog.Error(
				"Error saving PremisEvent for %s to Fluctus: %v",
				fixityResult.GenericFile.Identifier, err)
			return false
		} else {
			fixityChecker.ProcUtil.MessageLog.Info("Saved PremisEvent for %s",
				fixityResult.GenericFile.Identifier)
		}
	}
	return true
}

func (fixityChecker *FixityChecker) logStats() {
	fixityChecker.ProcUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
		fixityChecker.ProcUtil.Succeeded(), fixityChecker.ProcUtil.Failed())
}

// Fetch the file and calculate its SHA256 digest. This may take hours if the
// file is 250GB. Touch the NSQ message on both sides of the operation to
// prevent it from timing out.
func (fixityChecker *FixityChecker) checkFile() {
	for result := range fixityChecker.FixityChannel {
		fixityChecker.ProcUtil.MessageLog.Info("Checking %s", result.GenericFile.Identifier)
		result.NsqMessage.Touch()
		err := fixityChecker.ProcUtil.S3Client.FetchAndCalculateSha256(result, "")
		// Log usage errors. These shouldn't happen.
		if err != nil && strings.Index(err.Error(), "cannot by nil") > 0 {
			fixityChecker.ProcUtil.MessageLog.Error(err.Error())
		}
		result.NsqMessage.Touch()
		fixityChecker.ResultsChannel <- result
	}
}
