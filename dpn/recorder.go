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
	var result *DPNResult
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
		if result.DPNBag.AdminNode == "aptrust" {
			// This bag was ingested through APTrust.
			// Do we want to try this multiple times?
			// Do we want to requeu on failure?
			// How to distinguish between transient and permanent failure?
			recorder.RecordAPTrustDPNData(result)
		} else {
			// This bag was replicated from another node.
			// Here are a few vars to make our logic a little more clear.
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
				jsonData, jsonErr := json.Marshal(result)
				jsonString := "JSON data not available"
				if jsonErr == nil {
					jsonString = string(jsonData)
				}
				fatalErr := fmt.Errorf("Don't know what to record about bag %s. JSON dump: %s",
					result.DPNBag.UUID, jsonString)
				panic(fatalErr)
			}
		}
		recorder.PostProcessChannel <- result
	}
}

func (recorder *Recorder) postProcess() {
	for result := range recorder.PostProcessChannel {
		if result.ErrorMessage != "" {
			if result.Retry == false {
				recorder.ProcUtil.MessageLog.Error(
					"Record failure for bag %s; no more retries. ErrorMessage: %s",
					result.DPNBag.UUID, result.ErrorMessage)
				SendToTroubleQueue(result, recorder.ProcUtil)
			} else {
				recorder.ProcUtil.MessageLog.Info(
					"Record failure for bag %s; will requeue. ErrorMessage: %s",
					result.DPNBag.UUID, result.ErrorMessage)
				if result.NsqMessage != nil {
					result.NsqMessage.Requeue(5 * time.Minute)
				}
			}
		}
	}
}

// **** TODO: Write me! ****
// 1. Create a new bag record in our local DPN node.
// 2. Create a PREMIS event in Fluctus saying this bag has been copied to DPN.
// 3. Create replication requests for this bag in our local DPN node.
//
// Set result.ErrorMessage and result.Retry if there are problems.
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
	dpnBag, err := recorder.LocalRESTClient.DPNBagCreate(result.DPNBag)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Error creating DPN bag %s in our local registry: %s",
			result.DPNBag.UUID, err.Error())
		return
	}
	result.DPNBag = dpnBag
}

// Record PREMIS events in Fluctus. We do this only for DPN bags that
// we ingested here at APTrust. We create one PREMIS event saying the
// bag was ingested into DPN, and another that gives the DPN identifier.
func (recorder *Recorder) recordPremisEvents(result *DPNResult) {
	now := time.Now()

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
	symLink := fmt.Sprintf("/home/dpn.%s/outbound/%s.tar", toNode, result.DPNBag.UUID)

	err := os.Symlink(absPath, symLink)
	if err != nil {
		detailedError := fmt.Errorf("Error creating symlink from '%s' to '%s': %v",
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

// **** TODO: Write me! ****
// Tell the remote node that we succeeded or failed in copying
// the bag from the remote node to our local staging area.
// (This is about the rsync copy, not the copy to long-term storage.)
//
// Set result.ErrorMessage and result.Retry if there are problems.
func (recorder *Recorder) RecordCopyReceipt(result *DPNResult) {
	return
}

// **** TODO: Write me! ****
// Tell the remote node that we managed to copy the bag successfully
// into long-term storage, or that we failed to store it.
//
// Set result.ErrorMessage and result.Retry if there are problems.
func (recorder *Recorder) RecordStorageResult(result *DPNResult) {
	return
}


// ---------------------------------------------------------------------
// When storing DPN-bound APTrust bags
//
// condition: ingest_node is aptrust
// do all three in sequence
// ---------------------------------------------------------------------
// Record new bag in local DPN registry (after store)
// Create replication requests in local DPN registry
// Record PREMIS event in Fluctus (after store)

// ---------------------------------------------------------------------
// Replication from remote - record on remote DPN node
//
// condition: ingest_node is not aptrust
// ---------------------------------------------------------------------
// Record rsync receipt (after validation - MOVE CODE FROM validator.go)
//    - do this if stage is validate
// Record successful storage (after bag stored)
//    - do this if stage is store

// ---------------------------------------------------------------------
// Restore?
// ---------------------------------------------------------------------
