package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
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

		// If this is a transfer request, tell the remote node
		// whether the bag was valid, and what checksum we calculated
		// on the tag manifest.
		if result.TransferRequest != nil {
			validator.updateRemoteNode(result)
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
			SendToTroubleQueue(result, validator.ProcUtil)
			if bagman.FileExists(result.ValidationResult.TarFilePath) {
				os.Remove(result.ValidationResult.TarFilePath)
			}
			if bagman.FileExists(result.ValidationResult.UntarredPath) {
				os.RemoveAll(result.ValidationResult.UntarredPath)
			}
		} else {
			validator.ProcUtil.Succeeded()
			SendToStorageQueue(result, validator.ProcUtil)
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

// We update the remote node for transfer requests only. We don't
// to this for bags we packaged locally.
//
// When we receive a valid bag, tell the remote node that we
// got the bag and it looks OK. Send the tag manifest checksum.
// If the remote node accepts the checksum, we'll send the bag
// off to storage. There could be one of two problems here:
//
// 1. We determined that the bag was not valid. (Bad checksum,
//    missing files, or some similar issue.)
// 2. The remote node did not accept the checksum we calculated
//    on the tag manifest.
//
// In either case, the remote node will set the status of the
// transfer request to 'Cancelled'. If that happens, we'll set
// the error message on the result and we will delete the bag
// without sending it to storage.
//
// If the bag is valid and the remote node accepts our tag
// manifest checksum, this bag will go into the storage queue.
func (validator *Validator) updateRemoteNode(result *DPNResult) {
	if result.TransferRequest == nil {
		result.ErrorMessage = "Cannot update remote node because transfer request is missing."
		return
	}

	// Get a DPN REST client that can talk to the node that
	// this transfer originated from.
	remoteNode, err := validator.LocalRESTClient.DPNNodeGet(result.TransferRequest.FromNode)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Error retrieving node record for '%s' "+
			"from local DPN REST service: %v", result.TransferRequest.FromNode, err)
		return
	}

	authToken := validator.DPNConfig.RemoteNodeTokens[remoteNode.Namespace]
	if authToken == "" {
		result.ErrorMessage = fmt.Sprintf("Cannot get auth token for node %s", remoteNode.Namespace)
		return
	}
	remoteRESTClient, err := NewDPNRestClient(
		remoteNode.APIRoot,
		validator.DPNConfig.RestClient.LocalAPIRoot, // All nodes should be on same version as local
		authToken,
		validator.ProcUtil.MessageLog)

	// Update the transfer request and send it back to the remote node.
	// We'll get an updated transfer request back from that node.
	result.TransferRequest.Status = "Received"
	result.TransferRequest.FixityValue = result.ValidationResult.TagManifestChecksum
	xfer, err := remoteRESTClient.ReplicationTransferUpdate(result.TransferRequest)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Error updating transfer request on remote node: %v", err)
		return
	}
	if *xfer.FixityAccept == false {
		result.ErrorMessage = "Remote node did not accept the fixity value we sent for this bag. " +
			"This cancels the transfer request, and we will not store the bag."
		return
	}
	if xfer.Status == "Cancelled" {
		result.ErrorMessage = "This transfer request has been marked as cancelled on the remote node. " +
			"This bag will not be copied to storage."
	}
}

func (validator *Validator) RunTest(result *DPNResult) {
	validator.WaitGroup.Add(1)
	validator.ProcUtil.MessageLog.Info("Putting %s into validation channel",
		result.BagIdentifier)
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
