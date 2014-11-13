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
	if replicator.ReplicatedFileExists(&file) {
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

func (replicator *Replicator) ReplicatedFileExists(file *bagman.File) (bool) {
	exists, _ := replicator.S3ReplicationClient.Exists(
		replicator.ProcUtil.Config.PreservationBucket,
		file.Uuid)
	return exists
}

func (replicator *Replicator) replicate() {
	for replicationObject := range replicator.ReplicationChannel {
		replicator.ProcUtil.MessageLog.Info("Starting %s",
			replicationObject.File.Identifier)
		url, err := replicator.CopyFile(replicationObject)
		if err != nil {
			replicator.ProcUtil.MessageLog.Error(
				"Requeuing %s (%s) because copy failed. Error: %v",
				replicationObject.File.Identifier,
				replicationObject.File.StorageURL,
				err)
			replicationObject.NsqMessage.Requeue(5 * time.Minute)
		} else {
			replicator.ProcUtil.MessageLog.Info("Finished %s. Replication " +
				"copy is at %s.",
				replicationObject.File.Identifier,
				url)
			replicationObject.NsqMessage.Finish()
		}
	}
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
	replicationDir := filepath.Join(
		replicator.ProcUtil.Config.PreservationBucket,
		"replication")
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
