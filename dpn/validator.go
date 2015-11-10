package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"os"
	"sync"
)


type Validator struct {
	ValidationChannel   chan *DPNResult
	PostProcessChannel  chan *DPNResult
	ProcUtil            *bagman.ProcessUtil
	DPNConfig           *DPNConfig
	LocalRESTClient     *DPNRestClient
	// WaitGroup is for running local tests only.
	WaitGroup           sync.WaitGroup
}

func NewValidator(procUtil *bagman.ProcessUtil, dpnConfig *DPNConfig) (*Validator, error) {
	// Set up a DPN REST client that talks to our local DPN REST service.
	localClient, err := NewDPNRestClient(
		dpnConfig.RestClient.LocalServiceURL,
		dpnConfig.RestClient.LocalAPIRoot,
		dpnConfig.RestClient.LocalAuthToken,
		dpnConfig.LocalNode,
		dpnConfig,
		procUtil.MessageLog)
	if err != nil {
		return nil, err
	}

	validator := &Validator {
		ProcUtil: procUtil,
		LocalRESTClient: localClient,
		DPNConfig: dpnConfig,
	}
	workerBufferSize := procUtil.Config.DPNPackageWorker.Workers * 4
	validator.ValidationChannel = make(chan *DPNResult, workerBufferSize)
	validator.PostProcessChannel = make(chan *DPNResult, workerBufferSize)
	for i := 0; i < procUtil.Config.DPNPackageWorker.Workers; i++ {
		go validator.validate()
		go validator.postProcess()
	}
	return validator, nil
}

func (validator *Validator) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	dpnResult := &DPNResult{}
	err := json.Unmarshal(message.Body, dpnResult)
	if err != nil {
		detailedError := fmt.Errorf("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		validator.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	dpnResult.Stage = STAGE_VALIDATE
	dpnResult.NsqMessage = message
	validator.ValidationChannel <- dpnResult
	// identifier or dpn identifier
	validator.ProcUtil.MessageLog.Info("Put %s into validation channel",
		dpnResult.DPNBag.UUID)
	return nil
}

func (validator *Validator) validate() {
	for result := range validator.ValidationChannel {
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}
		if result.LocalPath == "" {
			result.ErrorMessage = "Cannot validate bag because DPNResult.LocalPath is not set. " +
				"This should be set to the location of the tar file you want to validate."
			validator.PostProcessChannel <- result
			continue
		}
		var err error
		// Set up a proper validation result object for this bag.
		result.ValidationResult, err = NewValidationResult(result.LocalPath, result.NsqMessage)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf(
				"Could not create ValidationResult for bag %s: %v",
				result.DPNBag.UUID, err)
			validator.PostProcessChannel <- result
			continue
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
		if result.TransferRequest != nil && result.TransferRequest.FixityNonce != nil &&
			*result.TransferRequest.FixityNonce != "" {
			nonce = *result.TransferRequest.FixityNonce
			validator.ProcUtil.MessageLog.Info("FixityNonce for bag %s is %s",
				result.DPNBag.UUID, nonce)
		} else {
			validator.ProcUtil.MessageLog.Info("No FixityNonce for bag %s", result.DPNBag.UUID)
		}

		// Calculate fixity of the tag manifest.
		// We were sending this as the receipt to the originating
		// node to verify that we received the bag correctly.
		// For now, we've switched to sending the sha256 checksum
		// of the entire bag. But that may change again. Leave this
		// in for now, so that the ValidationResult has a value in
		// the TagManifestChecksum field.
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

		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
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
			validator.ProcUtil.MessageLog.Error(result.ErrorMessage)
			validator.ProcUtil.IncrementFailed()
			SendToTroubleQueue(result, validator.ProcUtil)
			if bagman.FileExists(result.ValidationResult.TarFilePath) {
				os.Remove(result.ValidationResult.TarFilePath)
				validator.ProcUtil.MessageLog.Debug(
					"Deleting tar file %s", result.ValidationResult.TarFilePath)
			}
			if result.ValidationResult.UntarredPath != "" &&
				result.ValidationResult.UntarredPath != "/" &&
				bagman.FileExists(result.ValidationResult.UntarredPath) {
				validator.ProcUtil.MessageLog.Debug(
					"Deleting directory %s and its contents", result.ValidationResult.UntarredPath)
				os.RemoveAll(result.ValidationResult.UntarredPath)
			}
		} else {
			validator.ProcUtil.IncrementSucceeded()
			SendToRecordQueue(result, validator.ProcUtil)
		}

		if result.NsqMessage == nil {
			// This is a test message, running outside production.
			validator.WaitGroup.Done()
		} else {
			result.NsqMessage.Finish()
		}
		validator.ProcUtil.LogStats()
	}
}


func (validator *Validator) RunTest(result *DPNResult) {
	validator.WaitGroup.Add(1)
	validator.ProcUtil.MessageLog.Info("Putting %s into validation channel",
		result.DPNBag.UUID)
	validator.ValidationChannel <- result
	validator.WaitGroup.Wait()
	if result.ErrorMessage != "" {
		validator.ProcUtil.MessageLog.Error("Failed :( %s", result.ErrorMessage)
		return
	}
	if result.ValidationResult.IsValid() {
		validator.ProcUtil.MessageLog.Info("--- Validation Succeeded! ---")
	} else {
		validator.ProcUtil.MessageLog.Error("Bag failed validation.")
	}
}
