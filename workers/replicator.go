// replicator.go copies S3 files from one bucket to another.
// This is used to replicate files in one S3 region (Virginia)
// to another region (Oregon).

package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"os"
	"path/filepath"
	"time"
)

type ReplicationObject struct {
	File       *bagman.File
	NsqMessage *nsq.Message
}

type Replicator struct {
	ReplicationChannel  chan *ReplicationObject
	S3ReplicationClient *bagman.S3Client
	ProcUtil            *bagman.ProcessUtil
}

func NewReplicator(procUtil *bagman.ProcessUtil) (*Replicator) {
	replicationClient, _ := bagman.NewS3Client(aws.USWest2)
	replicator := &Replicator{
		ProcUtil: procUtil,
		S3ReplicationClient: replicationClient,
	}
	workerBufferSize := procUtil.Config.StoreWorker.Workers * 10
	replicator.ReplicationChannel = make(chan *ReplicationObject, workerBufferSize)
	for i := 0; i < procUtil.Config.StoreWorker.Workers; i++ {
		go replicator.replicate()
	}
	return replicator
}

// MessageHandler handles messages from the queue, putting each
// item into the replication channel.
func (replicator *Replicator) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var file bagman.File
	err := json.Unmarshal(message.Body, &file)
	if err != nil {
		replicator.ProcUtil.MessageLog.Error("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		message.Finish()
		return fmt.Errorf("Could not unmarshal JSON data from nsq")
	}
	if file.NeedsSave == false && replicator.ReplicatedFileExists(file.S3UUID()){
		// This occurs when the depositor has uploaded a new version
		// of an existing bag, but this particiluar file within the
		// bag did not change.
		replicator.ProcUtil.MessageLog.Info("File %s has not changed since it was last saved " +
			"and is confirmed to exist in replication bucket with UUID %s",
			file.Identifier, file.S3UUID())
		message.Finish()
		return nil
	}
	if replicator.ReplicatedFileExists(file.Uuid) {
		replicator.ProcUtil.MessageLog.Info("File %s already exists in replication bucket",
			file.Identifier)
		message.Finish()
		return nil
	}
	replicationObject := &ReplicationObject{
		NsqMessage: message,
		File: &file,
	}

	// Unfortunately, we're probably running on a machine that's
	// also running either ingest or restore. The Volume monitor
	// can know how much disk space those processes are using, but
	// not how much they have reserved. So until we have a volume
	// monitoring service, all we can do is pad the estimate of
	// how much space we might need.
	err = replicator.ProcUtil.Volume.Reserve(uint64(file.Size * 2))
	if err != nil {
		// Not enough room on disk
		replicator.ProcUtil.MessageLog.Warning("Requeueing %s (%d bytes) - not enough disk space",
			file.Identifier, file.Size)
		message.Requeue(10 * time.Minute)
		return nil
	}

	replicator.ReplicationChannel <- replicationObject
	replicator.ProcUtil.MessageLog.Debug("Put %s (%d bytes) into replication queue",
		file.Identifier, file.Size)
	return nil
}

func (replicator *Replicator) ReplicatedFileExists(fileUUID string) (bool) {
	exists, err := replicator.S3ReplicationClient.Exists(
		replicator.ProcUtil.Config.ReplicationBucket,
		fileUUID)
	if err != nil {
		replicator.ProcUtil.MessageLog.Warning("Error checking S3 file exists " +
			"in replication bucket: %v", err)
		replicator.ProcUtil.MessageLog.Info("File %s will be sent to replication " +
			"because we're not sure it's already there", fileUUID)
		return false
    }
	return exists
}

func (replicator *Replicator) replicate() {
	for replicationObject := range replicator.ReplicationChannel {
		replicator.ProcUtil.MessageLog.Info("Starting %s",
			replicationObject.File.Identifier)
		url, err := replicator.CopyAndSaveEvent(replicationObject)
		if err != nil {
			replicationObject.File.ReplicationError = err.Error()
			// If we failed too many times, send this into the failure
			// queue. Otherwise, just requeue and try again.
			if (replicationObject.NsqMessage.Attempts >=
				uint16(replicator.ProcUtil.Config.ReplicationWorker.MaxAttempts)) {
				replicator.SendToTroubleQueue(replicationObject.File)
				replicationObject.NsqMessage.Finish()
				replicator.ProcUtil.IncrementFailed()
			} else {
				replicator.ProcUtil.MessageLog.Error(
					"Requeuing %s (%s) because copy failed. Error: %v",
					replicationObject.File.Identifier,
					replicationObject.File.StorageURL,
					err)
				replicationObject.NsqMessage.Requeue(5 * time.Minute)
				replicator.ProcUtil.IncrementFailed()
			}
		} else {
			// Success!
			replicator.ProcUtil.MessageLog.Info("Finished %s. Replication " +
				"copy is at %s.",
				replicationObject.File.Identifier,
				url)
			replicationObject.NsqMessage.Finish()
			replicator.ProcUtil.IncrementSucceeded()
		}
		replicator.ProcUtil.MessageLog.Info(
			"**STATS** Succeeded: %d, Failed: %d",
			replicator.ProcUtil.Succeeded(),
			replicator.ProcUtil.Failed())
	}
}

// Copy the file to S3 in Oregon and save the replication
// PremisEvent to Fluctus. Returns the S3 URL of the newly-saved file
// or an error.
func (replicator *Replicator) CopyAndSaveEvent(replicationObject *ReplicationObject) (string, error) {
	url, err := replicator.CopyFile(replicationObject)
	if err != nil {
		return "", err
	}
	event, err := replicator.SaveReplicationEvent(replicationObject.File, url)
	if err != nil {
		return "", err
	}
	replicator.ProcUtil.MessageLog.Info(
		"Saved replication PremisEvent for %s (%s) with event identifier %s",
		replicationObject.File.Identifier,
		replicationObject.File.Uuid,
		event.Identifier)
	return url, nil
}

// Copies a file from one bucket to another, across regions,
// including all of APTrust's custom metadata. Returns the URL
// of the destination file (that should be in the replication
// bucket in Oregon), or an error.
//
// This does NOT use PUT COPY internally because PUT COPY is
// limited to files of 5GB or less, and we'll have many files
// over 5GB. The copy operation downloads data from the S3
// preservation bucket and uploads it to the replication bucket.
//
// As long as we're running in the same region as our S3
// preservation bucket (USEast), the download should be fast
// and free. Running this code outside of USEast will be
// slow and expensive, since we'll have to pay for the bandwidth
// of BOTH download and upload.
func (replicator *Replicator) CopyFile(replicationObject *ReplicationObject) (string, error) {
	replicator.ProcUtil.MessageLog.Info("Starting copy of %s (%s)",
		replicationObject.File.Identifier, replicationObject.File.Uuid)
	// Copy options include the md5 sum of the file we're copying
	// and all of our custom meta data.
	copyOptions, err := replicator.GetCopyOptions(replicationObject.File)
	if err != nil {
		return "", err
	}

	// Touch before dowload, because large files can take a long time!
	replicationObject.NsqMessage.Touch()

	localPath, err := replicator.DownloadFromPreservation(replicationObject.File)
	if err != nil {
		return "", err
	}

	reader, err := os.Open(localPath)
	if err != nil {
		return "", err
	}

	// Touch again before upload, because large files are slow
	replicationObject.NsqMessage.Touch()

	// Replication client is configured to us USWest-2 (Oregon),
	// but the bucket name should be enough.
	url := ""
	if replicationObject.File.Size <= bagman.S3_LARGE_FILE {
		url, err = replicator.S3ReplicationClient.SaveToS3(
			replicator.ProcUtil.Config.ReplicationBucket,
			replicationObject.File.Uuid,
			replicationObject.File.MimeType,
			reader,
			replicationObject.File.Size,
			copyOptions)
	} else {
		url, err = replicator.S3ReplicationClient.SaveLargeFileToS3(
			replicator.ProcUtil.Config.ReplicationBucket,
			replicationObject.File.Uuid,
			replicationObject.File.MimeType,
			reader,
			replicationObject.File.Size,
			copyOptions,
			bagman.S3_CHUNK_SIZE)
	}

	// Touch so NSQ knows we're not dead yet!
	replicationObject.NsqMessage.Touch()

	// Delete the local file.
	delErr := os.Remove(localPath)
	if delErr != nil {
		replicator.ProcUtil.MessageLog.Warning("Could not delete local file %s: %v",
			localPath, delErr)
	} else {
		replicator.ProcUtil.Volume.Release(uint64(replicationObject.File.Size * 2))
	}

	if err == nil {
		replicator.ProcUtil.MessageLog.Info("Finished copy of %s (%s)",
			replicationObject.File.Identifier,
			replicationObject.File.Uuid)
	}

	return url, err
}

// Returns S3 options, including the md5 checksum and APTrust's custom
// metadata. These options must accompany the file copy.
func (replicator *Replicator) GetCopyOptions(file *bagman.File) (s3.Options, error) {
	// Copy all of the meta data
	resp, err := replicator.ProcUtil.S3Client.Head(
		replicator.ProcUtil.Config.PreservationBucket,
		file.Uuid)
	if err != nil {
		detailedErr :=  fmt.Errorf("Head request for %s at %s returned error: %v",
			file.Uuid,
			replicator.ProcUtil.Config.PreservationBucket,
			err)
		return s3.Options{}, detailedErr
	}
	if resp.StatusCode != 200 {
		detailedErr := fmt.Errorf(
			"Can't get S3 metadata for file %s. " +
			"Head request for %s/%s returned HTTP Status Code %d",
			file.Identifier,
			replicator.ProcUtil.Config.PreservationBucket,
			file.Uuid,
			resp.StatusCode)
		return s3.Options{}, detailedErr
	}

	s3Metadata := make(map[string][]string)
	s3Metadata["md5"]         = []string{ file.Md5 }
	s3Metadata["institution"] = []string{ resp.Header["X-Amz-Meta-Institution"][0] }
	s3Metadata["bag"]         = []string{ resp.Header["X-Amz-Meta-Bag"][0] }
	s3Metadata["bagpath"]     = []string{ file.Path }

	base64md5, err := bagman.Base64EncodeMd5(file.Md5)
	if err != nil {
		return s3.Options{}, err
	}

	options := replicator.S3ReplicationClient.MakeOptions(base64md5, s3Metadata)
	return options, nil
}

// Copies a file from the preservation bucket to a local file
// and returns the path to the local file.
func (replicator *Replicator) DownloadFromPreservation(file *bagman.File) (string, error) {
	// Make sure we have a folder to put the file in.
	replicationDir := replicator.ProcUtil.Config.ReplicationDirectory
	if _, err := os.Stat(replicationDir); os.IsNotExist(err) {
		err = os.MkdirAll(replicationDir, 0755)
		if err != nil {
			return "", err
		}
	}

	// Now copy the file from S3 to local path.
	localPath := filepath.Join(replicationDir, file.Uuid)
	err := replicator.ProcUtil.S3Client.FetchToFileWithoutChecksum(
		replicator.ProcUtil.Config.PreservationBucket,
		file.Uuid,
		localPath)
	if err != nil {
		detailedErr := fmt.Errorf("Cannot read file %s from %s: %v",
			file.Uuid,
			replicator.ProcUtil.Config.PreservationBucket,
			err)
		return "", detailedErr
	}

	replicator.ProcUtil.MessageLog.Info("Downloaded %s", localPath)
	return localPath, nil
}

// Saves the replication PremisEvent to Fluctus.
// Param url is the S3 URL we just saved that file to.
// That should be in Oregon.
func (replicator *Replicator) SaveReplicationEvent(file *bagman.File, url string) (*bagman.PremisEvent, error) {
	replicationEvent, err := file.ReplicationEvent(url)
	if err != nil {
		return nil, err
	}
	replicator.ProcUtil.MessageLog.Info("Saving replication PremisEvent for %s (%s)",
		file.Identifier, file.Uuid)
	savedEvent, err := replicator.ProcUtil.FluctusClient.PremisEventSave(
		file.Identifier, "GenericFile", replicationEvent)
	if err != nil {
		return nil, err
	}
	return savedEvent, nil
}

// Puts an item into the trouble queue.
func (replicator *Replicator) SendToTroubleQueue(file *bagman.File) {
	err := bagman.Enqueue(
		replicator.ProcUtil.Config.NsqdHttpAddress,
		replicator.ProcUtil.Config.FailedReplicationWorker.NsqTopic,
		file)
	if err != nil {
		replicator.ProcUtil.MessageLog.Error(
			"Could not send '%s' (%s) to failed replication queue: %v\n",
			file.Identifier,
			file.Uuid,
			err)
	} else {
		message := fmt.Sprintf("Failed to copy '%s' (%s) to replication bucket. " +
			"This item is going into the failed replication queue.",
			file.Identifier,
			file.Uuid)
		replicator.ProcUtil.MessageLog.Warning(message)
	}
}
