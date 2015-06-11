package dpn

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
)

func SendToRecordQueue(result *DPNResult, procUtil *bagman.ProcessUtil) {
	// Record has to record PREMIS event in Fluctus if
	// BagIdentifier is present. It will definitely have
	// to record information in the DPN REST API.
	err := bagman.Enqueue(procUtil.Config.NsqdHttpAddress,
		procUtil.Config.DPNRecordWorker.NsqTopic, result)
	if err != nil {
		bagIdentifier := result.BagIdentifier
		if bagIdentifier == "" {
			bagIdentifier = result.PackageResult.BagBuilder.UUID
		}
		message := fmt.Sprintf("Could not send '%s' (at %s) to record queue: %v",
			bagIdentifier, result.PackageResult.TarFilePath, err)
		result.ErrorMessage += message
		procUtil.MessageLog.Error(message)
		SendToTroubleQueue(result, procUtil)
	}
}

func SendToValidationQueue(result *DPNResult, procUtil *bagman.ProcessUtil) {
	err := bagman.Enqueue(procUtil.Config.NsqdHttpAddress,
		procUtil.Config.DPNValidationWorker.NsqTopic, result)
	if err != nil {
		message := fmt.Sprintf("Could not send '%s' (at %s) to validation queue: %v",
			result.BagIdentifier, result.PackageResult.TarFilePath, err)
		result.ErrorMessage += message
		procUtil.MessageLog.Error(message)
		SendToTroubleQueue(result, procUtil)
	}
}

func SendToStorageQueue(result *DPNResult, procUtil *bagman.ProcessUtil) {
	err := bagman.Enqueue(procUtil.Config.NsqdHttpAddress,
		procUtil.Config.DPNStoreWorker.NsqTopic, result)
	if err != nil {
		message := fmt.Sprintf("Could not send '%s' (at %s) to storage queue: %v",
			result.BagIdentifier, result.PackageResult.TarFilePath, err)
		result.ErrorMessage += message
		procUtil.MessageLog.Error(message)
		SendToTroubleQueue(result, procUtil)
	}
}

func SendToTroubleQueue(result *DPNResult, procUtil *bagman.ProcessUtil) {
	result.ErrorMessage += " This item has been queued for administrative review."
	err := bagman.Enqueue(procUtil.Config.NsqdHttpAddress,
		procUtil.Config.DPNTroubleWorker.NsqTopic, result)
	if err != nil {
		procUtil.MessageLog.Error("Could not send '%s' to trouble queue: %v",
			result.BagIdentifier, err)
		procUtil.MessageLog.Error("Original error on '%s' was %s",
			result.BagIdentifier, result.ErrorMessage)
	}
}
