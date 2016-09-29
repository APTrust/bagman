package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
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
	LocalPath       string
	ErrorMessage    string
	RsyncStdout     string
	RsyncStderr     string
	InfoMessage     string
	BagWasCopied    bool
}

func NewCopier(procUtil *bagman.ProcessUtil, dpnConfig *DPNConfig) (*Copier, error) {
	localClient, err := NewDPNRestClient(
		dpnConfig.RestClient.LocalServiceURL,
		dpnConfig.RestClient.LocalAPIRoot,
		dpnConfig.RestClient.LocalAuthToken,
		dpnConfig.LocalNode,
		dpnConfig,
		procUtil.MessageLog)

	// HACK: ProcUtil assumes the volume it should be managing is
	// the APTrust volume. In this case, it's the DPN volume.
	// DPNStagingDirectory
	volume, err := bagman.NewVolume(procUtil.Config.DPNStagingDirectory, procUtil.MessageLog)
	if err != nil {
		message := fmt.Sprintf("Exiting. Cannot init Volume object: %v", err)
		fmt.Fprintln(os.Stderr, message)
		procUtil.MessageLog.Fatal(message)
	}
	procUtil.Volume = volume

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
	workerBufferSize := procUtil.Config.DPNCopyWorker.Workers * 4
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
	dpnResult := &DPNResult{}
	err := json.Unmarshal(message.Body, dpnResult)
	if err != nil {
		detailedError := fmt.Errorf("Could not unmarshal JSON data from nsq. " +
			"Error is: %v    JSON is: %s", err.Error(), string(message.Body))
		copier.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// Set up the copy result
	dpnResult.CopyResult = &CopyResult{
		BagWasCopied: false,
	}
	dpnResult.NsqMessage = message

	// Start processing.
	dpnResult.Stage = STAGE_COPY
	copier.LookupChannel <- dpnResult
	copier.ProcUtil.MessageLog.Info("Put %s from %s into copy channel",
		dpnResult.DPNBag.UUID, dpnResult.DPNBag.AdminNode)
	return nil
}

// Look up the DPN bag on the admin node. Although we already have the
// bag object as part of the DPNResult object, this request may have been
// sitting in the queue for many hours, and the replication request may
// have been fulfilled or cancelled in that time. So check the status on
// the authoritative node to avoid unnecessarily processing what might
// be hundreds of gigs of data.
func (copier *Copier) doLookup() {
	for result := range copier.LookupChannel {
		// Get a client to talk to the FromNode
		remoteClient := copier.RemoteClients[result.TransferRequest.FromNode]

		copier.ProcUtil.MessageLog.Debug(
			"Looking up ReplicationId %s, bag %s, on node %s ",
				result.TransferRequest.ReplicationId,
				result.TransferRequest.Bag,
				result.TransferRequest.FromNode)


		// If we can find out for sure that this replication request should
		// not be processed, then don't process it...
		xfer, _ := remoteClient.ReplicationTransferGet(
			result.TransferRequest.ReplicationId)
		if xfer != nil && (xfer.Cancelled || xfer.Stored) {
			cancelMessage := "request was cancelled"
			if xfer.Stored {
				cancelMessage = "item has already been stored"
			}
			message := fmt.Sprintf(
				"Cancelling copy of ReplicationId %s (bag %s) from %s because %s",
				result.TransferRequest.ReplicationId,
				result.TransferRequest.Bag,
				result.TransferRequest.FromNode,
				cancelMessage)
			copier.ProcUtil.MessageLog.Info(message)
			result.CopyResult.InfoMessage = message
			result.Retry = false
			result.TransferRequest = xfer
			copier.PostProcessChannel <- result
			continue
		}
		// ...otherwise, proceed with processing.
		copier.CopyChannel <- result
	}
}

// Copy the file from the remote node to our local staging area.
// Calculate checksums.
func (copier *Copier) doCopy() {
	for result := range copier.CopyChannel {

		// Make sure we have enough room on the volume
		// to download and unpack this bag.
		err := copier.ProcUtil.Volume.Reserve(uint64(float64(result.DPNBag.Size) * float64(2.1)))
		if err != nil {
			// Not enough room on disk
			msg := fmt.Sprintf(
				"Requeueing %s from %s (%d bytes) - not enough disk space: %v",
				result.TransferRequest.ReplicationId,
				result.TransferRequest.FromNode,
				result.DPNBag.Size, err)
			copier.ProcUtil.MessageLog.Warning(msg)
			if result.NsqMessage != nil {
				result.NsqMessage.Requeue(1 * time.Hour)
			} else {
				result.ErrorMessage = msg
				result.CopyResult.ErrorMessage = msg
				copier.WaitGroup.Done()
			}
			continue
		}

		localPath := filepath.Join(
			copier.ProcUtil.Config.DPNStagingDirectory,
			fmt.Sprintf("%s.tar", result.TransferRequest.Bag))

		if !bagman.FileExists(copier.ProcUtil.Config.DPNStagingDirectory) {
			os.MkdirAll(copier.ProcUtil.Config.DPNStagingDirectory, 0755)
		}

		// DEBUG - use for tracing 'file not found'
		// fmt.Printf("Rsync link is %s\n", result.TransferRequest.Link)

		copier.ProcUtil.MessageLog.Info("Rsync link is %s", result.TransferRequest.Link)
		rsyncCommand := GetRsyncCommand(result.TransferRequest.Link,
			localPath, copier.DPNConfig.UseSSHWithRsync)

		// Touch message on both sides of rsync, so NSQ doesn't time out.
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}
		output, err := rsyncCommand.CombinedOutput()
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}
		if err != nil {
			result.CopyResult.ErrorMessage = fmt.Sprintf("%s: %s",
				err.Error(), string(output))
		} else {
			result.LocalPath = localPath
			result.CopyResult.LocalPath = localPath
			result.CopyResult.BagWasCopied = true
			// TODO: This is not necessary. We just need to calculate the checksum
			// of the SHA-256 manifest
			fileDigest, err := bagman.CalculateDigests(localPath)
			if result.NsqMessage != nil {
				result.NsqMessage.Touch()
			}
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("Could not calculate checksums on '%s': %v",
					result.PackageResult.TarFilePath, err)
				copier.PostProcessChannel <- result
				continue
			}
			result.BagMd5Digest = fileDigest.Md5Digest
			result.BagSha256Digest = fileDigest.Sha256Digest
			result.BagSize = fileDigest.Size
		}
		copier.PostProcessChannel <- result
	}
}

func (copier *Copier) postProcess() {
	// On success, send to validation queue.
	// Otherwise, send to trouble queue.
	for result := range copier.PostProcessChannel {
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}
		result.ErrorMessage = result.CopyResult.ErrorMessage

		// On error, log and send to trouble queue if the error is fatal
		if result.ErrorMessage != "" {
			copier.ProcUtil.MessageLog.Error(result.ErrorMessage)
			copier.ProcUtil.IncrementFailed()
			if result.Retry == false {
				SendToTroubleQueue(result, copier.ProcUtil)
			}
			if bagman.FileExists(result.CopyResult.LocalPath) {
				os.Remove(result.CopyResult.LocalPath)
				copier.ProcUtil.MessageLog.Debug(
					"Deleting bag file %s", result.CopyResult.LocalPath)
			}
		} else if result.CopyResult.BagWasCopied == false {
			// We didn't copy the bag, but there was no error.
			// This happens when the transfer request is marked
			// as completed or cancelled on the remote node.
			// Count this as success, because we did what we're
			// supposed to do in this case, which is nothing.
			copier.ProcUtil.IncrementSucceeded()
		} else {
			// We successfully copied the bag. Send it on to
			// the validation queue.
			copier.ProcUtil.IncrementSucceeded()
			SendToValidationQueue(result, copier.ProcUtil)
		}

		if result.NsqMessage == nil {
			// This is a test message, running outside production.
			copier.WaitGroup.Done()
		} else {
			result.NsqMessage.Finish()
		}
		copier.ProcUtil.LogStats()

	}
}

func (copier *Copier) RunTest(dpnResult *DPNResult) {
	copier.WaitGroup.Add(1)
	copier.ProcUtil.MessageLog.Info("Putting %s into lookup channel",
		dpnResult.BagIdentifier)
	copier.CopyChannel <- dpnResult
	copier.WaitGroup.Wait()
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
func GetRsyncCommand(copyFrom, copyTo string, useSSH bool) (*exec.Cmd) {
//	rsync -avz -e ssh remoteuser@remotehost:/remote/dir /this/dir/
	if useSSH {
		return exec.Command("rsync", "-avzW", "-e",  "ssh", copyFrom, copyTo, "--inplace")
	}
	return exec.Command("rsync", "-avzW", "--inplace", copyFrom, copyTo)
}
