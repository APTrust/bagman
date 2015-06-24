package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"github.com/nu7hatch/gouuid"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Recorder struct {
	RecordChannel       chan *DPNResult
	PostProcessChannel  chan *DPNResult
	ProcUtil            *bagman.ProcessUtil
	DPNConfig           *DPNConfig
	LocalRESTClient     *DPNRestClient
	RemoteClients       map[string]*DPNRestClient
	// WaitGroup is for running local tests only.
	WaitGroup           sync.WaitGroup
}

type RecordResult struct {
	// Did we create the DPN bag record in our local DPN registry?
	// We do this only if APTrust was the ingest node. If we created
	// the bag, this should be set to the bag's CreatedAt timestamp,
	// as returned by the server.
	DPNBagCreatedAt              time.Time
	// If we created replication requests for this bag, the
	// namespaces of the replicating nodes should go here.
	// Note that this just means we created replication requests;
	// it does not mean those requests have been fulfilled.
	DPNReplicationRequests       []string
	// If this was a local APTrust bag, we create a PREMIS
	// event saying that the bag has been ingested to DPN.
	// The PREMIS event identifier is a UUID string.
	PremisIngestEventId          string
	// PREMIS identifier assignment event ID for bags ingested
	// by APTrust.
	PremisIdentifierEventId      string
	// If this is not an APTrust bag, did we send the copy receipt
	// the remote node that asked us to replicate this bag?
	// If sent the copy receipt, this should be set to the
	// ReplicationTransfer object's UpdatedAt timestamp, as returned
	// by the remote DPN REST server.
	CopyReceiptSentAt            time.Time
	// If this is not an APTrust bag, did we send a message to the
	// remote node describing the outcome of our attempt to copy
	// this bag into long-term storage? If so, set this to the
	// UpdatedAt timestamp of the ReplicationTransfer object, as
	// returned by the remote DPN REST server.
	StorageResultSentAt          time.Time
	// ErrorMessage contains information about an error that occurred
	// at any step during the recording process. If ErrorMessage is
	// an empty string, no error occurred.
	ErrorMessage                 string
}

func NewRecordResult() (*RecordResult) {
	return &RecordResult{
		DPNReplicationRequests: make([]string, 0),
	}
}

func NewRecorder(procUtil *bagman.ProcessUtil, dpnConfig *DPNConfig) (*Recorder, error) {
	// Set up a DPN REST client that talks to our local DPN REST service.
	localClient, err := NewDPNRestClient(
		dpnConfig.RestClient.LocalServiceURL,
		dpnConfig.RestClient.LocalAPIRoot,
		dpnConfig.RestClient.LocalAuthToken,
		procUtil.MessageLog)
	if err != nil {
		return nil, err
	}
	remoteClients, err := GetRemoteClients(localClient, dpnConfig,
		procUtil.MessageLog)
	if err != nil {
		return nil, err
	}
	recorder := &Recorder{
		ProcUtil: procUtil,
		DPNConfig: dpnConfig,
		LocalRESTClient: localClient,
		RemoteClients: remoteClients,
	}
	workerBufferSize := procUtil.Config.DPNRecordWorker.Workers * 10
	recorder.RecordChannel = make(chan *DPNResult, workerBufferSize)
	recorder.PostProcessChannel = make(chan *DPNResult, workerBufferSize)
	for i := 0; i < procUtil.Config.DPNRecordWorker.Workers; i++ {
		go recorder.postProcess()
	}
	for i := 0; i < procUtil.Config.DPNRecordWorker.NetworkConnections; i++ {
		go recorder.record()
	}
	return recorder, nil
}

func (recorder *Recorder) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	result := &DPNResult{}
	err := json.Unmarshal(message.Body, result)
	if err != nil {
		recorder.ProcUtil.MessageLog.Error("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		message.Finish()
		return fmt.Errorf("Could not unmarshal JSON data from nsq")
	}
	result.NsqMessage = message
	result.Stage = STAGE_RECORD
	recorder.ProcUtil.MessageLog.Info(
		"Putting %s bag %s into the record queue. Stage = %s",
		result.DPNBag.AdminNode, result.DPNBag.UUID, result.Stage)
	recorder.RecordChannel <- result
	return nil
}


func (recorder *Recorder) record() {
	for result := range recorder.RecordChannel {
		if result.TransferRequest == nil {
			// This bag was ingested through APTrust.
			// Do we want to try this multiple times?
			// Do we want to requeu on failure?
			// How to distinguish between transient and permanent failure?
			recorder.ProcUtil.MessageLog.Debug("Bag %s (%s) was ingested at APTrust",
				result.DPNBag.UUID, result.BagIdentifier)
			recorder.RecordAPTrustDPNData(result)
		} else {
			// This bag was replicated from another node.
			// Here are a few vars to make our logic a little more clear.
			recorder.ProcUtil.MessageLog.Debug("Bag %s is being replicated from %s",
				result.DPNBag.UUID, result.TransferRequest.FromNode)
			bagWasCopied := (result.CopyResult != nil && result.CopyResult.LocalPath != "")
			bagWasValidated := (result.ValidationResult != nil && result.ValidationResult.TarFilePath != "")
			bagWasStored := result.StorageURL != ""
			storageResultSent := !result.RecordResult.StorageResultSentAt.IsZero()
			copyReceiptSent := !result.RecordResult.CopyReceiptSentAt.IsZero()
			// What do we need to record. Let's see...
			if bagWasStored && !storageResultSent {
				recorder.RecordStorageResult(result)
			} else if bagWasCopied && bagWasValidated && !copyReceiptSent {
				recorder.RecordCopyReceipt(result)
			} else {
				jsonData, jsonErr := json.MarshalIndent(result, "", "  ")
				jsonString := "JSON data not available"
				if jsonErr == nil {
					jsonString = string(jsonData)
				}
				fatalErr := fmt.Errorf("Don't know what to record about bag %s. " +
					"bagWasCopied = %t, bagWasValidated = %t, " +
					"bagWasStored = %t, storageResultSent = %t, " +
					"copyReceiptSent = %t ... JSON dump ---> %t",
					result.DPNBag.UUID, bagWasCopied, bagWasValidated,
					bagWasStored, storageResultSent, copyReceiptSent,
					jsonString)
				fmt.Println(fatalErr.Error())
				recorder.ProcUtil.MessageLog.Fatal(fatalErr)
			}
		}
		recorder.PostProcessChannel <- result
	}
}

func (recorder *Recorder) postProcess() {
	for result := range recorder.PostProcessChannel {
		if result.ErrorMessage != "" {
			// Something went wrong
			if result.Retry == false {
				recorder.ProcUtil.MessageLog.Error(
					"Record failure for bag %s; no more retries. ErrorMessage: %s",
					result.DPNBag.UUID, result.ErrorMessage)
				SendToTroubleQueue(result, recorder.ProcUtil)
			} else {
				recorder.ProcUtil.MessageLog.Error(
					"Record failure for bag %s; will requeue. ErrorMessage: %s",
					result.DPNBag.UUID, result.ErrorMessage)
				if result.NsqMessage != nil {
					result.NsqMessage.Requeue(1 * time.Minute)
					continue
				}
			}
		} else {
			// Nothing went wrong
			storageResultSent := !result.RecordResult.StorageResultSentAt.IsZero()
			copyReceiptSent := !result.RecordResult.CopyReceiptSentAt.IsZero()
			if copyReceiptSent && !storageResultSent {
				// Bag was copied from remote node to local staging
				// area but has not been copied into long-term storage.
				SendToStorageQueue(result, recorder.ProcUtil)
			}
		}

		// If no errors, and the storage result was sent,
		// we're at the end of the line here.
		// All processing is done.
		if result.NsqMessage == nil {
			recorder.WaitGroup.Done()
		} else {
			if result.TransferRequest == nil {
				// Local bag
				recorder.ProcUtil.MessageLog.Info(
					"Ingest complete for bag %s from %s",
					result.DPNBag.UUID, result.DPNBag.AdminNode)
			} else
			{
				// Replicated bag
				recorder.ProcUtil.MessageLog.Info(
					"Replication complete for bag %s from %s",
					result.TransferRequest.UUID, result.TransferRequest.FromNode)
			}
			result.NsqMessage.Finish()
		}
	}
}

// Records data for DPN bags ingested at APTrust.
// 1. Create a new bag record in our local DPN node.
// 2. Create a PREMIS event in Fluctus saying this bag has been copied to DPN.
// 3. Create replication requests for this bag in our local DPN node.
func (recorder *Recorder) RecordAPTrustDPNData(result *DPNResult) {
	recorder.registerNewDPNBag(result)
	if result.ErrorMessage != "" {
		return
	}
	recorder.recordPremisEvents(result)
	if result.ErrorMessage != "" {
		return
	}
	recorder.createReplicationRequests(result)
}

// Create a new DPN bag entry in our local DPN registry. We do this only
// for DPN bags that we ingester here at APTrust.
func (recorder *Recorder) registerNewDPNBag(result *DPNResult) {
	recorder.ProcUtil.MessageLog.Debug("Creating new DPN bag %s (%s) in local registry.",
		result.DPNBag.UUID, result.BagIdentifier)
	dpnBag, err := recorder.LocalRESTClient.DPNBagCreate(result.DPNBag)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Error creating DPN bag %s in our local registry: %s",
			result.DPNBag.UUID, err.Error())
		return
	}
	result.DPNBag = dpnBag
	result.RecordResult.DPNBagCreatedAt = dpnBag.CreatedAt
}

// Record PREMIS events in Fluctus. We do this only for DPN bags that
// we ingested here at APTrust. We create one PREMIS event saying the
// bag was ingested into DPN, and another that gives the DPN identifier.
func (recorder *Recorder) recordPremisEvents(result *DPNResult) {
	now := time.Now()

	recorder.ProcUtil.MessageLog.Debug("Creating ingest PREMIS event for bag %s (%s)",
		result.DPNBag.UUID, result.BagIdentifier)
	ingestUuid, err := uuid.NewV4()
	if err != nil {
		result.ErrorMessage =  fmt.Sprintf("Error generating UUID for DPN " +
			"ingest event for S3 URL: %v", err)
		return
	}
	ingestEvent := &bagman.PremisEvent{
		Identifier:         ingestUuid.String(),
		EventType:          "ingest",
		DateTime:           now,
		Detail:             fmt.Sprintf("Item ingested into DPN with id %s", result.DPNBag.UUID),
		Outcome:            string(bagman.StatusSuccess),
		OutcomeDetail:      result.DPNBag.UUID,
		Object:             "Go uuid library + goamz S3 library",
		Agent:              "http://github.com/nu7hatch/gouuid",
		OutcomeInformation: result.DPNBag.UUID,
	}

	savedIngestEvent, err := recorder.ProcUtil.FluctusClient.PremisEventSave(
		result.BagIdentifier, "IntellectualObject", ingestEvent)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Error creating DPN ingest " +
			"PREMIS event for bag %s: %s", result.DPNBag.UUID, err.Error())
		return
	}
	result.RecordResult.PremisIngestEventId = savedIngestEvent.Identifier
	recorder.ProcUtil.MessageLog.Debug("Created ingest PREMIS event for bag %s (%s). " +
		"Ingest ID is %s", result.DPNBag.UUID, result.BagIdentifier,
		savedIngestEvent.Identifier)


	recorder.ProcUtil.MessageLog.Debug("Creating id assignment PREMIS event for bag %s (%s)",
		result.DPNBag.UUID, result.BagIdentifier)
	idAssignmentUuid, err := uuid.NewV4()
	if err != nil {
		result.ErrorMessage =  fmt.Sprintf("Error generating UUID for identifier " +
			"assignment event for S3 URL: %v", err)
		return
	}
	idEvent := &bagman.PremisEvent{
		Identifier:         idAssignmentUuid.String(),
		EventType:          "identifier_assignment",
		DateTime:           now,
		Detail:             "Assigned new storage identifier",
		Outcome:            string(bagman.StatusSuccess),
		OutcomeDetail:      result.StorageURL,
		Object:             "Go uuid library + APTrust DPN services",
		Agent:              "http://github.com/nu7hatch/gouuid",
		OutcomeInformation: fmt.Sprintf("DPN bag stored at %s", result.StorageURL),
	}

	savedIdEvent, err := recorder.ProcUtil.FluctusClient.PremisEventSave(
		result.BagIdentifier, "IntellectualObject", idEvent)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Error creating DPN identifier assignment " +
			"PREMIS event for bag %s: %s", result.DPNBag.UUID, err.Error())
		return
	}
	result.RecordResult.PremisIdentifierEventId = savedIdEvent.Identifier
	recorder.ProcUtil.MessageLog.Debug("Created id assignment PREMIS event for bag %s (%s). " +
		"Ingest ID is %s", result.DPNBag.UUID, result.BagIdentifier,
		savedIngestEvent.Identifier)
}

// Create replication requests for the DPN bag we just ingested. We do this
// only for bags we ingested.
func (recorder *Recorder) createReplicationRequests(result *DPNResult) {
	localNode, err := recorder.LocalRESTClient.DPNNodeGet(recorder.DPNConfig.LocalNode)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Can't create replication requests: " +
			"unable to get info about our node. %s", err.Error())
		return
	}
	replicateTo := localNode.ChooseNodesForReplication(recorder.DPNConfig.ReplicateToNumNodes)
	for _, toNode := range replicateTo {
		recorder.ProcUtil.MessageLog.Debug("Will replicate to node %s", toNode)
		_, err = recorder.CreateSymLink(result, toNode)
		if err != nil {
			result.ErrorMessage = err.Error()
			return
		}
		xfer := recorder.MakeReplicationTransfer(result, toNode)
		savedXfer, err := recorder.LocalRESTClient.ReplicationTransferCreate(xfer)
		if err != nil {
			result.ErrorMessage = err.Error()
			return
		} else {
			result.RecordResult.DPNReplicationRequests = append(
				result.RecordResult.DPNReplicationRequests, savedXfer.ToNode)
		}
	}
}

func (recorder *Recorder) CreateSymLink(result *DPNResult, toNode string) (string, error) {
	absPath := filepath.Join(recorder.ProcUtil.Config.DPNStagingDirectory,
		result.DPNBag.UUID + ".tar")
	symLink := fmt.Sprintf("%s/dpn.%s/outbound/%s.tar",
		recorder.ProcUtil.Config.DPNHomeDirectory, toNode, result.DPNBag.UUID)
	recorder.ProcUtil.MessageLog.Debug("Creating symlink from '%s' to '%s'",
		symLink, absPath)

	err := os.Symlink(absPath, symLink)
	if err != nil {
		detailedError := fmt.Errorf("Error creating symlink at '%s' pointing to '%s': %v",
			symLink, absPath, err)
		return "", detailedError
	}
	return symLink, nil
}

func (recorder *Recorder) MakeReplicationTransfer(result *DPNResult, toNode string) (*DPNReplicationTransfer) {
	// Sample rsync link:
	// dpn.tdr@devops.aptrust.org:outbound/472218b3-95ce-4b8e-6c21-6e514cfbe43f.tar
	link := fmt.Sprintf("dpn.%s@devops.aptrust.org:outbound/%s.tar",
		toNode, result.DPNBag.UUID)
	return &DPNReplicationTransfer{
		FromNode: recorder.DPNConfig.LocalNode,
		ToNode: toNode,
		UUID: result.DPNBag.UUID,
		FixityAlgorithm: "sha256",
		FixityNonce: "",
		FixityValue: "",
		Status: "Requested",
		Protocol: "R",
		Link: link,
	}
}

// Tell the remote node that we succeeded or failed in copying
// the bag from the remote node to our local staging area.
// (This is about the rsync copy, not the copy to long-term storage.)
//
// We update the remote node for transfer requests only. We don't
// to this for bags we packaged locally.
//
// When we receive a valid bag, tell the remote node that we
// got the bag and it looks OK.  If the remote node accepts the
// checksum, we'll send the bag off to storage. There could be
// one of two problems here:
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
func (recorder *Recorder) RecordCopyReceipt(result *DPNResult) {
	if result.TransferRequest == nil {
		result.ErrorMessage = "Cannot update remote node because transfer request is missing."
		return
	}

	remoteClient, clientExists := recorder.RemoteClients[result.TransferRequest.FromNode]
	if clientExists == false {
		result.ErrorMessage = fmt.Sprintf("Can't send copy receipt to %s: " +
			"Can't get REST client for that node.", result.TransferRequest.FromNode)
		return
	}

	// Update the transfer request and send it back to the remote node.
	// We'll get an updated transfer request back from that node.
	bagValid := result.ValidationResult.IsValid()
	result.TransferRequest.Status = "Received"
	result.TransferRequest.BagValid = &bagValid
	result.TransferRequest.FixityValue = result.BagSha256Digest

	detailedMessage := fmt.Sprintf("xfer request %s status for bag %s " +
		"from remote node %s. " +
		"Setting status to 'Received', BagValid to %t, and checksum to %s",
		result.TransferRequest.ReplicationId, result.TransferRequest.UUID,
		result.TransferRequest.FromNode, *result.TransferRequest.BagValid,
		result.TransferRequest.FixityValue)
	recorder.ProcUtil.MessageLog.Debug("Updating %s", detailedMessage)
	xfer, err := remoteClient.ReplicationTransferUpdate(result.TransferRequest)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Error updating %s: %v", detailedMessage, err)
		return
	}

	// Ok, our update made it through
	result.TransferRequest = xfer
	result.RecordResult.CopyReceiptSentAt = time.Now()

	if xfer.FixityAccept == nil || *xfer.FixityAccept == false {
		recorder.ProcUtil.MessageLog.Debug(
			"Remote node rejected fixity value for xfer request %s (bag %s)",
			result.TransferRequest.ReplicationId, result.TransferRequest.UUID)
		result.ErrorMessage = "Remote node did not accept the fixity value we sent for this bag. " +
			"This cancels the transfer request, and we will not store the bag."
		return
	}
	if xfer.Status == "Cancelled" {
		recorder.ProcUtil.MessageLog.Debug(
			"Remote node says status is 'Cancelled' for xfer request %s (bag %s)",
			result.TransferRequest.ReplicationId, result.TransferRequest.UUID)
		result.ErrorMessage = "This transfer request has been marked as cancelled on the remote node. " +
			"This bag will not be copied to storage."
		return
	}
	recorder.ProcUtil.MessageLog.Debug("Remote node updated xfer request %s (bag %s), " +
		"and set status to %s", xfer.ReplicationId, xfer.UUID, xfer.Status)

}

// Tell the remote node that we managed to copy the bag successfully
// into long-term storage, or that we failed to store it.
//
// Set result.ErrorMessage and result.Retry if there are problems.
func (recorder *Recorder) RecordStorageResult(result *DPNResult) {
	if result.TransferRequest == nil {
		result.ErrorMessage = "Cannot update remote node because transfer request is missing."
		return
	}

	remoteClient, clientExists := recorder.RemoteClients[result.TransferRequest.FromNode]
	if clientExists == false {
		result.ErrorMessage = fmt.Sprintf("Can't send storage receipt to %s: " +
			"Can't get REST client for that node.", result.TransferRequest.FromNode)
		return
	}

	result.TransferRequest.Status = "Stored"
	recorder.ProcUtil.MessageLog.Debug("Updating xfer request %s status for bag %s on remote node %s. " +
		"Setting status to 'Stored', BagValid to %t, and checksum to %s",
		result.TransferRequest.ReplicationId, result.TransferRequest.UUID,
		result.TransferRequest.FromNode, *result.TransferRequest.BagValid,
		result.TransferRequest.FixityValue)
	xfer, err := remoteClient.ReplicationTransferUpdate(result.TransferRequest)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Error updating transfer request on remote node: %v", err)
		return
	}

	// Ok, our update made it through
	result.TransferRequest = xfer
	result.RecordResult.StorageResultSentAt = time.Now()

	recorder.ProcUtil.MessageLog.Debug("Remote node updated xfer request %s (bag %s), " +
		"and set status to %s", xfer.ReplicationId, xfer.UUID, xfer.Status)
}

func (recorder *Recorder) RunTest(result *DPNResult) {
	recorder.WaitGroup.Add(1)
	recorder.ProcUtil.MessageLog.Info("Putting %s into record channel",
		result.DPNBag.UUID)
	recorder.RecordChannel <- result
	recorder.WaitGroup.Wait()
	if result.ErrorMessage != "" {
		recorder.ProcUtil.MessageLog.Error("Failed :( %s", result.ErrorMessage)
		recorder.ProcUtil.MessageLog.Error("Record failed.")
	} else {
		recorder.ProcUtil.MessageLog.Info("--- Record Succeeded! ---")
	}
}
