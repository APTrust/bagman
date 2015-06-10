package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"os/exec"
	"path/filepath"
	"sync"
)

// copier.go copies tarred bags from other nodes via rsync.
// This is used when replicating content from other nodes.
// For putting together DPN bags from APTrust files, see fetcher.go.

type Copier struct {
	LookupChannel       chan *DPNResult
	CopyChannel         chan *DPNResult
	PostProcessChannel  chan *DPNResult
	DPNConfig           *DPNConfig
	ProcUtil            *bagman.ProcessUtil
	LocalClient         *DPNRestClient
	RemoteClients       map[string]*DPNRestClient
	// WaitGroup is for running local tests only.
	WaitGroup           sync.WaitGroup
}

type CopyResult struct {
	NSQMessage      *nsq.Message  `json:"-"`
	Link            string
	LocalPath       string
	ErrorMessage    string
	RsyncStdout     string
	RsyncStderr     string
	Sha256Digest    string
	InfoMessage     string
	BagWasCopied    bool
}

func NewCopier(procUtil *bagman.ProcessUtil, dpnConfig *DPNConfig) (*Copier, error) {
	localClient, err := NewDPNRestClient(
		dpnConfig.RestClient.LocalServiceURL,
		dpnConfig.RestClient.LocalAPIRoot,
		dpnConfig.RestClient.LocalAuthToken,
		procUtil.MessageLog)
	if err != nil {
		return nil, fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	remoteClients, err := GetRemoteClients(localClient, dpnConfig,
		procUtil.MessageLog)
	if err != nil {
		return nil, err
	}
	copier := &Copier {
		DPNConfig: dpnConfig,
		ProcUtil: procUtil,
		LocalClient: localClient,
		RemoteClients: remoteClients,
	}
	workerBufferSize := procUtil.Config.DPNPackageWorker.Workers * 4
	copier.LookupChannel = make(chan *DPNResult, workerBufferSize)
	copier.CopyChannel = make(chan *DPNResult, workerBufferSize)
	copier.PostProcessChannel = make(chan *DPNResult, workerBufferSize)
	for i := 0; i < procUtil.Config.DPNPackageWorker.Workers; i++ {
		go copier.doLookup()
		go copier.doCopy()
		go copier.postProcess()
	}
	return copier, nil
}

func (copier *Copier) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	var dpnResult *DPNResult
	err := json.Unmarshal(message.Body, dpnResult)
	if err != nil {
		detailedError := fmt.Errorf("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		copier.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// Set up the copy result
	dpnResult.CopyResult = &CopyResult{
		NSQMessage: message,
		Link: dpnResult.TransferRequest.Link,
		BagWasCopied: false,
	}

	// Start processing.
	dpnResult.Stage = STAGE_COPY
	copier.LookupChannel <- dpnResult
	copier.ProcUtil.MessageLog.Info("Put %s into copy channel",
		dpnResult.BagIdentifier)
	return nil
}

// Look up the DPN bag on the admin node. Although we already have the
// bag object as bart of the DPNResult object, this request may have been
// sitting in the queue for many hours, and the replication request may
// have been fulfilled or cancelled in that time. So check the status on
// the authoritative node to avoid unnecessarily processing what might
// be hundreds of gigs of data.
func (copier *Copier) doLookup() {
	for result := range copier.LookupChannel {
		// Get a client to talk to the FromNode
		remoteClient := copier.RemoteClients[result.TransferRequest.FromNode]

		// If we can find out for sure that this replication request should
		// not be processed, then don't process it...
		xfer, _ := remoteClient.ReplicationTransferGet(
			result.TransferRequest.ReplicationId)
		if xfer != nil && xfer.Status != "Requested" {
			message := fmt.Sprintf(
				"Cancelling copy of ReplicationId %s (bag %s) because " +
					"replication status on %s is %s",
				result.TransferRequest.ReplicationId,
				result.TransferRequest.UUID,
				result.TransferRequest.FromNode,
				xfer.Status)
			copier.ProcUtil.MessageLog.Info(message)
			result.CopyResult.InfoMessage = message
			result.TransferRequest = xfer
			copier.PostProcessChannel <- result
			continue
		}
		// ...otherwise, proceed with processing.
		copier.CopyChannel <- result
	}
}

// Copy the file from the remote node to our local staging area
// and calculate the Sha256 digest.
func (copier *Copier) doCopy() {
	for result := range copier.CopyChannel {
		localPath := filepath.Join(
			copier.ProcUtil.Config.DPNStagingDirectory,
			fmt.Sprintf("%s.tar", result.TransferRequest.UUID))
		rsyncCommand := GetRsyncCommand(result.TransferRequest.Link, localPath)

		// Touch message on both sides of rsync, so NSQ doesn't time out.
		if result.CopyResult.NSQMessage != nil {
			result.CopyResult.NSQMessage.Touch()
		}
		output, err := rsyncCommand.CombinedOutput()
		if result.CopyResult.NSQMessage != nil {
			result.CopyResult.NSQMessage.Touch()
		}
		if err != nil {
			result.CopyResult.ErrorMessage = fmt.Sprintf("%s: %s",
				err.Error(), string(output))
		} else {
			result.CopyResult.LocalPath = localPath
			result.CopyResult.BagWasCopied = true

			// Touch message on both sides of digest, so NSQ doesn't time out.
			if result.CopyResult.NSQMessage != nil {
				result.CopyResult.NSQMessage.Touch()
			}
			sha256Digest, err := CalculateSha256Digest(localPath)
			if result.CopyResult.NSQMessage != nil {
				result.CopyResult.NSQMessage.Touch()
			}

			if err != nil {
				result.CopyResult.ErrorMessage = err.Error()
			} else {
				result.CopyResult.Sha256Digest = sha256Digest
			}
		}
		copier.PostProcessChannel <- result
	}
}

func (copier *Copier) postProcess() {
	// On success, send to validation queue.
	// Otherwise, send to trouble queue.
	// for result := range copier.PostProcessChannel {

	// }
}

func (copier *Copier) RunTest(bagIdentifier string) (*DPNResult) {
	dpnResult := NewDPNResult(bagIdentifier)
	copier.WaitGroup.Add(1)
	copier.ProcUtil.MessageLog.Info("Putting %s into lookup channel",
		dpnResult.BagIdentifier)
	copier.CopyChannel <- dpnResult
	copier.WaitGroup.Wait()
	return dpnResult
}

// Returns a command object for copying from the remote location to
// the local filesystem. The copy is done via rsync over ssh, and
// the command will capture stdout and stderr. The copyFrom param
// should be a valid scp target in this format:
//
// remoteuser@remotehost:/remote/dir/bag.tar
//
// The copyTo param should be an absolute path on a locally-accessible
// file system, such as:
//
// /mnt/dpn/data/bag.tar
//
// Using this assumes a few things:
//
// 1. You have rsync installed.
// 2. You have an ssh client installed.
// 3. You have an entry in your ~/.ssh/config file specifying
//    connection and key information for the remote host.
//
// Usage:
//
// command := GetRsyncCommand("aptrust@tdr:bag.tar", "/mnt/dpn/bag.tar")
// err := command.Run()
// if err != nil {
//    ... do something ...
// }
//
// -- OR --
//
// output, err := command.CombinedOutput()
// if err != nil {
//    fmt.Println(err.Error())
//    fmt.Println(string(output))
// }

//
func GetRsyncCommand(copyFrom, copyTo string) (*exec.Cmd) {
//	rsync -avz -e ssh remoteuser@remotehost:/remote/dir /this/dir/
	return exec.Command("rsync", "-avz", "-e",  "ssh", copyFrom, copyTo)
}