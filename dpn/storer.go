package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"github.com/crowdmob/goamz/s3"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Storer struct {
	StorageChannel      chan *DPNResult
	CleanupChannel      chan *DPNResult
	BagCreateChannel    chan *DPNResult
	PostProcessChannel  chan *DPNResult
	ProcUtil            *bagman.ProcessUtil
	DPNConfig           *DPNConfig
	LocalRESTClient     *DPNRestClient
	// WaitGroup is for running local tests only.
	WaitGroup           sync.WaitGroup
}

func NewStorer(procUtil *bagman.ProcessUtil, dpnConfig *DPNConfig) (*Storer, error) {
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

	storer := &Storer{
		LocalRESTClient: localClient,
		ProcUtil: procUtil,
		DPNConfig: dpnConfig,
	}
	workerBufferSize := procUtil.Config.DPNStoreWorker.Workers * 10
	storer.StorageChannel = make(chan *DPNResult, workerBufferSize)
	storer.BagCreateChannel = make(chan *DPNResult, workerBufferSize)
	storer.CleanupChannel = make(chan *DPNResult, workerBufferSize)
	storer.PostProcessChannel = make(chan *DPNResult, workerBufferSize)
	for i := 0; i < procUtil.Config.DPNStoreWorker.Workers; i++ {
		go storer.createBagRecord()
		go storer.cleanup()
		go storer.postProcess()
	}
	for i := 0; i < procUtil.Config.DPNStoreWorker.NetworkConnections; i++ {
		go storer.store()
	}
	return storer, nil
}

func (storer *Storer) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	result := &DPNResult{}
	err := json.Unmarshal(message.Body, result)
	if err != nil {
		storer.ProcUtil.MessageLog.Error("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		message.Finish()
		return fmt.Errorf("Could not unmarshal JSON data from nsq")
	}
	if result.BagMd5Digest == "" || result.BagSha256Digest == "" {
		errMsg := "Bag cannot be stored because DPNResult is missing either md5 or sha256 checksum."
		storer.ProcUtil.MessageLog.Error(errMsg)
		result.ErrorMessage = errMsg
		SendToTroubleQueue(result, storer.ProcUtil)
		message.Finish()
		return fmt.Errorf(errMsg)
	}
	result.NsqMessage = message
	result.Stage = STAGE_STORE
	bagIdentifier := result.BagIdentifier
	if bagIdentifier == "" {
		bagIdentifier = "DPN Replication Bag"
	}
	storer.ProcUtil.MessageLog.Info("Putting %s into the storage queue (%s)",
		result.TarFilePath(), bagIdentifier)
	storer.StorageChannel <- result
	return nil
}

// TODO for 2.0: Don't store if StoreRequested == false
func (storer *Storer) store() {
	for result := range storer.StorageChannel {
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

		// By the time we get our hands on this replication request,
		// it may be many hours old. Fetch the request again from the
		// originating node and make sure it hasn't been cancelled.
		if result.TransferRequest != nil {
			storer.refetchReplicationRequest(result)
		}
		if result.ErrorMessage != "" {
			storer.PostProcessChannel <- result
			continue
		}
		// Remember that TransferRequest will be nil
		// for bags we built ourselves.
		if result.TransferRequest != nil && result.TransferRequest.Cancelled {
			result.ErrorMessage += " Replication request was cancelled."
			result.Stage = STAGE_CANCELLED
			result.Retry = false
			storer.PostProcessChannel <- result
			continue
		} else if result.TransferRequest != nil && result.TransferRequest.StoreRequested == false {
			// TODO: Would be nice to alert someone about this.
			result.ErrorMessage += " Replication request was not cancelled, " +
				"but StoreRequested was set to false. SOMETHING IS WRONG HERE!"
			result.Stage = STAGE_CANCELLED
			result.Retry = false
			storer.PostProcessChannel <- result
			continue
		}

		err := os.MkdirAll(filepath.Dir(result.TarFilePath()), 0755)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error creating directory '%s': %v",
				filepath.Dir(result.TarFilePath()), err)
			storer.PostProcessChannel <- result
			continue
		}

		// Now we'll open a file reader and stream the tar file
		// up to S3.
		reader, err := os.Open(result.TarFilePath())
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error opening file '%s': %v",
				result.TarFilePath(), err)
			reader.Close()
			storer.PostProcessChannel <- result
			continue
		}
		fileInfo, err := reader.Stat()
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Cannot stat file '%s': %v",
				result.TarFilePath(), err)
			reader.Close()
			storer.PostProcessChannel <- result
			continue
		}
		options, err := storer.GetS3Options(result)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error generating S3 options: %v", err)
			reader.Close()
			storer.PostProcessChannel <- result
			continue
		}

		bagUUID := ""
		if result.DPNBag != nil {
			bagUUID = result.DPNBag.UUID
		} else {
			bagUUID = result.PackageResult.BagBuilder.UUID
		}
		fileName := fmt.Sprintf("%s.tar", bagUUID)
		url := ""
		if fileInfo.Size() > bagman.S3_LARGE_FILE {
			url, err = storer.ProcUtil.S3Client.SaveLargeFileToS3(
				storer.ProcUtil.Config.DPNPreservationBucket,
				fileName,
				"application/x-tar",
				reader,
				fileInfo.Size(),
				options,
				bagman.S3_CHUNK_SIZE)
		} else {
			url, err = storer.ProcUtil.S3Client.SaveToS3(
				storer.ProcUtil.Config.DPNPreservationBucket,
				fileName,
				"application/x-tar",
				reader,
				fileInfo.Size(),
				options)
		}

		// Close the reader
		reader.Close()
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error saving file to S3/Glacier: %v", err)
			storer.PostProcessChannel <- result
			continue
		}

		// Record where we stored the file
		result.StorageURL = url

		// Update the transfer request, if there is one.
		if result.TransferRequest != nil {
			result.TransferRequest.Stored = true
		}
		if result.ErrorMessage != "" {
			result.ErrorMessage = fmt.Sprintf("Bag was stored, but we couldn't " +
				"update the replication request on the originating node: %v", err)
			storer.PostProcessChannel <- result
			continue
		}

		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

		// This channel really only applies to bags we created
		// at our own node. (Not replication requests.)
		storer.BagCreateChannel <- result
	}
}

// Get the replication request from the originating node and
// make sure it's still valid.
func (storer *Storer) refetchReplicationRequest(result *DPNResult) {
	storer.makeReplicationRequest(result, "GET")
}

func (storer *Storer) makeReplicationRequest(result *DPNResult, whatKind string) {

	if result.TransferRequest == nil {
		// This is a local bag and there is no replication request.
		storer.ProcUtil.MessageLog.Error("makeReplicationRequest was called for bag %s (%s), " +
			"which has no associated replication request. Is this a local bag?",
			result.BagIdentifier, result.PackageResult.BagBuilder.UUID)
		return
	}

	// Get a REST client that can talk the remote node.
	// This client will be configured with the correct URL
	// and API token to talk to the node that issues the
	// replication request.
	remoteClient, err := storer.LocalRESTClient.GetRemoteClient(
		result.TransferRequest.FromNode,  // the node we want to talk to
		storer.DPNConfig,
		storer.ProcUtil.MessageLog)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Could not create a REST client to connect to node %s: %v",
			result.TransferRequest.FromNode, err)
		return
	}

	var xferRequest *DPNReplicationTransfer
	if whatKind == "GET" {
		storer.ProcUtil.MessageLog.Debug("Replication request id: %s, " +
			"FromNode: %s, ToNode: %s", result.TransferRequest.ReplicationId,
			result.TransferRequest.FromNode, result.TransferRequest.ToNode)
		xferRequest, err = remoteClient.ReplicationTransferGet(result.TransferRequest.ReplicationId)
	} else if whatKind == "POST" {
		xferRequest, err = remoteClient.ReplicationTransferUpdate(result.TransferRequest)
	} else {
		panic("makeReplicationRequest doesn't understand that kind of request")
	}


	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Could not get replication record %s from node %s: %v",
			result.TransferRequest.ReplicationId, result.TransferRequest.FromNode, err)
		return
	}
	result.TransferRequest = xferRequest

}

func (storer *Storer) createBagRecord() {
	for result := range storer.BagCreateChannel {
		// If result has a local identifier, it's a bag we created
		// here at the local node (as opposed to a bag we're just
		// replicating to fulfill a transfer request). Since we
		// created this bag here, we need to create a Bag record
		// for it in DPN. We could also check result.PackageResult
		// to determine if bag was made here. That will be non-nil
		// for bags we built, nil for bags built elsewhere.
		bagWasCreatedHere := result.BagIdentifier != ""
		bagStoredSuccessfully := (result.ErrorMessage == "" && result.StorageURL != "")
		if bagWasCreatedHere && bagStoredSuccessfully {
			storer.ProcUtil.MessageLog.Debug("Creating bag record for %s with md5 %s and sha256 %s",
				result.BagIdentifier, result.BagMd5Digest, result.BagSha256Digest)
			fileInfo, err := os.Stat(result.TarFilePath())
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("Cannot stat %s to get file size: %v",
					result.TarFilePath(), err)
				storer.CleanupChannel <- result
				continue
			}
			newBag := &DPNBag{
				UUID: result.PackageResult.BagBuilder.UUID,
				LocalId: result.BagIdentifier,
				Size: uint64(fileInfo.Size()),
				FirstVersionUUID: result.PackageResult.BagBuilder.UUID,
				Version: 1,
				IngestNode: storer.DPNConfig.LocalNode,
				AdminNode: storer.DPNConfig.LocalNode,
				BagType: "D",
				// No spec yet on how to specify rights; cannot be nil
				Rights: make([]string, 0),
				// No spec yet on how to specify interpretive; cannot be nil
				Interpretive: make([]string, 0),
				ReplicatingNodes: []string{"aptrust"},
			}
			digest := &DPNMessageDigest{
				Value: result.TagManifestDigest,
				Algorithm: "sha256",
				Node: storer.DPNConfig.LocalNode,
				Bag: newBag.UUID,
				CreatedAt: time.Now().UTC(),
			}
			result.DPNBag = newBag
			result.MessageDigest = digest
		}
		storer.CleanupChannel <- result
	}
}


func (storer *Storer) cleanup() {
	for result := range storer.CleanupChannel {
		thisIsNotATest := (result.NsqMessage != nil)
		storageSucceeded := (result.ErrorMessage == "" && result.StorageURL != "")
		// If this bag came from another node, we can delete it after storing it.
		// If it came from our node, we need to keep it around until it's been
		// replicated.
		thisBagCameFromAnotherNode := (result.ProcessedItemId == 0)
		if storageSucceeded && thisIsNotATest && thisBagCameFromAnotherNode {
			err := os.Remove(result.TarFilePath())
			if err != nil {
				storer.ProcUtil.MessageLog.Warning("Error cleaning up %s: %v",
					result.TarFilePath(), err)
			} else {
				storer.ProcUtil.MessageLog.Info(
					"After successful upload, deleted local DPN bag at %s",
					result.TarFilePath())
			}
		}
		storer.PostProcessChannel <- result
	}
}


func (storer *Storer) postProcess() {
	for result := range storer.PostProcessChannel {
		bagIdentifier := result.BagIdentifier
		if bagIdentifier == "" {
			bagIdentifier = result.DPNBag.UUID
		}
		if result.ErrorMessage == "" && result.StorageURL != "" {
			// SUCCESS :)
			storer.ProcUtil.MessageLog.Info("Bag %s successfully stored at %s",
				bagIdentifier, result.StorageURL)
			storer.ProcUtil.IncrementSucceeded()
			// Send to queue for recording in Fluctus and/or DPN REST
			if result.NsqMessage != nil {
				result.NsqMessage.Finish()
				SendToRecordQueue(result, storer.ProcUtil)
			}
		} else {
			// FAILURE :(
			storer.ProcUtil.MessageLog.Error(result.ErrorMessage)
			storer.ProcUtil.IncrementFailed()
			// Item failed after max attempts. Put in trouble queue
			// for admin review.
			if result.NsqMessage != nil {
				if result.NsqMessage.Attempts >= uint16(storer.ProcUtil.Config.DPNStoreWorker.MaxAttempts) {
					// No more retries
					result.NsqMessage.Finish()
					SendToTroubleQueue(result, storer.ProcUtil)
				} else {
					storer.ProcUtil.MessageLog.Info("Requeuing %s (%s)",
						bagIdentifier, result.TarFilePath())
					result.NsqMessage.Requeue(1 * time.Minute)
				}
			}
		}
		if result.NsqMessage == nil {
			// This is a test message, running outside production.
			storer.WaitGroup.Done()
		}
	}
}

func (storer *Storer) GetS3Options(result *DPNResult) (s3.Options, error) {
	// Prepare metadata for save to S3
	s3Metadata := make(map[string][]string)
	if result.BagIdentifier != "" {
		s3Metadata["aptrust-bag"] = []string{result.BagIdentifier}
	}
	// Save to S3 with the base64-encoded md5 sum
	base64md5, err := bagman.Base64EncodeMd5(result.BagMd5Digest)
	if err != nil {
		return s3.Options{}, err
	}
	options := storer.ProcUtil.S3Client.MakeOptions(base64md5, s3Metadata)
	return options, nil
}

func (storer *Storer) RunTest(result *DPNResult) {
	storer.WaitGroup.Add(1)
	storer.ProcUtil.MessageLog.Info("Putting %s into digest channel",
		result.BagIdentifier)
	storer.StorageChannel <- result
	storer.WaitGroup.Wait()
	fmt.Println("Storer is done")
}
