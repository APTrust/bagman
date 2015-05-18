package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
//	"os"
//	"path/filepath"
//	"strings"
	"sync"
//	"time"
)


type Validator struct {
	ValidationChannel   chan *DPNResult
	PostProcessChannel  chan *DPNResult
	ProcUtil            *bagman.ProcessUtil
	// WaitGroup is for running local tests only.
	WaitGroup           sync.WaitGroup
}

func NewValidator(procUtil *bagman.ProcessUtil) (*Validator) {
	validator := &Validator {
		ProcUtil: procUtil,
	}
	workerBufferSize := procUtil.Config.DPNPackageWorker.Workers * 4
	validator.ValidationChannel = make(chan *DPNResult, workerBufferSize)
	validator.PostProcessChannel = make(chan *DPNResult, workerBufferSize)
	for i := 0; i < procUtil.Config.DPNPackageWorker.Workers; i++ {
		go validator.validate()
		go validator.postProcess()
	}
	return validator
}

func (validator *Validator) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var dpnResult *DPNResult
	err := json.Unmarshal(message.Body, dpnResult)
	if err != nil {
		detailedError := fmt.Errorf("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		validator.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	dpnResult.Stage = STAGE_VALIDATE
	validator.ValidationChannel <- dpnResult
	// identifier or dpn identifier
	validator.ProcUtil.MessageLog.Info("Put %s into validation channel",
		dpnResult.BagIdentifier)
	return nil
}

func (validator *Validator) validate() {
	for result := range validator.ValidationChannel {
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}
		var err error
		// Set up a proper validation result object for this bag.
		result.ValidationResult, err = NewValidationResult(result.LocalPath, result.NsqMessage)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf(
				"Could not create ValidationResult for bag %s: %v",
				result.DPNBag.UUID, err)
			validator.PostProcessChannel <- result
		}
		// Now validate the bag. This step can take a long time on
		// large bags, since we may be untarring hundred of gigabytes
		// and then running sha256 checksums on all of the content.
		// Touch the message on both sides of this long-running operation
		// so the NSQ message doesn't time out. ValidateBag() will also
		// touch the message internally.
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}
		// Here's the validation.
		result.ValidationResult.ValidateBag()
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

		// If the bag we're currently processing is a transfer request
		// from another node, we'll have to calculate the sha256
		// checksum on the tag manifest and send that back to the
		// originating node as a receipt. The originating node will
		// usually include a nonce in the transfer request, and we'll
		// have to sign the checksum with that to get the fixity value
		// that the originating node will accept.
		nonce := ""
		if result.TransferRequest != nil {
			nonce = result.TransferRequest.FixityNonce
			validator.ProcUtil.MessageLog.Info("FixityNonce for bag %s is %s",
				result.DPNBag.UUID, nonce)
		} else {
			validator.ProcUtil.MessageLog.Info("No FixityNonce for bag %s", result.DPNBag.UUID)
		}

		// Calculate fixity of the tag manifest to send as receipt.
		result.ValidationResult.CalculateTagManifestDigest(nonce)

		// If our call to ValidateBag() above found any errors, set an
		// error message on the result object so we know this operation
		// has failed, and log whatever errors the validator identified.
		if !result.ValidationResult.IsValid() {
			result.ErrorMessage = "Bag failed validation. See error messages in ValidationResult."
			validator.ProcUtil.MessageLog.Error(result.ErrorMessage)
			for _, message := range result.ValidationResult.ErrorMessages {
				validator.ProcUtil.MessageLog.Error(message)
			}
		}

		// Now everything goes into the post-process channel.
		validator.PostProcessChannel <- result
	}
}

func (validator *Validator) postProcess() {
	for result := range validator.PostProcessChannel {
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}
		if result.ErrorMessage != "" {
			validator.ProcUtil.IncrementFailed()
			validator.SendToTroubleQueue(result)
		} else {

		}
		// If bag failed validation, send to trouble queue
		// If bag is OK:
		//    1) send message to remote node
		//    2) retrieve Xfer request from remote node and
		//       make sure fixity accept is true and request
		//       status is not 'Cancelled'
		//    3) If Xfer is a go, send to storage queue,
		//       otherwise, send to trouble queue

		if result.NsqMessage == nil {
			// This is a test message, running outside production.
			validator.WaitGroup.Done()
		} else {
			result.NsqMessage.Finish()
		}
		validator.ProcUtil.LogStats()
	}
}



func (validator *Validator) SendToTroubleQueue(result *DPNResult) {
	err := bagman.Enqueue(validator.ProcUtil.Config.NsqdHttpAddress,
		validator.ProcUtil.Config.DPNTroubleWorker.NsqTopic, result)
	if err != nil {
		validator.ProcUtil.MessageLog.Error("Could not send '%s' to trouble queue: %v",
			result.BagIdentifier, err)
		validator.ProcUtil.MessageLog.Error("Original error on '%s' was %s",
			result.BagIdentifier, result.ErrorMessage)
	}
}
