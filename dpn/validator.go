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
		result.ValidationResult, err = NewValidationResult(result.LocalPath)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf(
				"Could not create ValidationResult for bag %s: %v",
				result.DPNBag.UUID, err)
			validator.PostProcessChannel <- result
		}
		result.ValidationResult.ValidateBag()
		nonce := ""
		if result.TransferRequest != nil {
			nonce = result.TransferRequest.FixityNonce
			validator.ProcUtil.MessageLog.Info("FixityNonce for bag %s is %s",
				result.DPNBag.UUID, nonce)
		} else {
			validator.ProcUtil.MessageLog.Info("FixityNonce for bag %s", result.DPNBag.UUID)
		}
		result.ValidationResult.CalculateTagManifestDigest(nonce)
		if !result.ValidationResult.IsValid() {
			result.ErrorMessage = "Bag failed validation. See error messages in ValidationResult."
		}
		validator.PostProcessChannel <- result
	}
}

func (validator *Validator) postProcess() {
	for result := range validator.PostProcessChannel {
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}
		// If bag failed validation, send to trouble queue
		// If bag is OK:
		//    1) send message to remote node
		//    2) retrieve Xfer request from remote node and
		//       make sure fixity accept is true and request
		//       status is not 'Cancelled'
		//    3) If Xfer is a go, send to storage queue,
		//       otherwise, send to trouble queue
	}
}
