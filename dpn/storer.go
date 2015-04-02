package dpn

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"github.com/crowdmob/goamz/s3"
	"io"
	"os"
	"sync"
	"time"
)

// StorageResult maintains information about the state of
// an attempt to store a DPN bag in AWS Glacier.
type StorageResult struct {

	// The NSQ message. This will be always have a value
	// in production, and will be nil when running local
	// developer tests.
	NsqMessage      *nsq.Message  `json:"-"`

	// BagIdentifier is the APTrust bag identifier. If this is
	// a non-empty value, it means this bag came from APTrust,
	// and we will need to record a PREMIS event noting that
	// it was ingested into DPN. If the bag identifier is empty,
	// this bag came from somewhere else. We're just replicating
	// and we don't need to store a PREMIS event in Fluctus.
	BagIdentifier   string

	// UUID is the DPN identifier for this bag.
	UUID            string

	// The path to the bag, which is stored on disk as a tar file.
	TarFilePath     string

	// The URL of this file in Glacier. This will be empty until
	// we actually manage to store the file.
	StorageURL      string

	// A message describing what went wrong in the storage process.
	// If we have a StorageURL and ErrorMessage is empty,
	// storage succeeded.
	ErrorMessage    string

	// The file's md5 digest. We need this to copy to Amazon S3/Glacier.
	Md5Digest       string

	// Should we try again to store this object? Usually, this is
	// true if we encounter network errors, false if there's some
	// fatal error, like TarFilePath cannot be found.
	Retry           bool
}

type Storer struct {
	DigestChannel       chan *StorageResult
	StorageChannel      chan *StorageResult
	CleanupChannel      chan *StorageResult
	PostProcessChannel  chan *StorageResult
	ProcUtil            *bagman.ProcessUtil
	// WaitGroup is for running local tests only.
	WaitGroup           sync.WaitGroup
}

func NewStorer(procUtil *bagman.ProcessUtil) (*Storer) {
	storer := &Storer{
		ProcUtil: procUtil,
	}
	workerBufferSize := procUtil.Config.DPNStoreWorker.Workers * 10
	storer.DigestChannel = make(chan *StorageResult, workerBufferSize)
	storer.StorageChannel = make(chan *StorageResult, workerBufferSize)
	storer.CleanupChannel = make(chan *StorageResult, workerBufferSize)
	storer.PostProcessChannel = make(chan *StorageResult, workerBufferSize)
	for i := 0; i < procUtil.Config.DPNStoreWorker.Workers; i++ {
		go storer.calculateDigest()
		go storer.cleanup()
		go storer.postProcess()
	}
	for i := 0; i < procUtil.Config.DPNStoreWorker.NetworkConnections; i++ {
		go storer.store()
	}
	return storer
}

func (storer *Storer) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var result *StorageResult
	err := json.Unmarshal(message.Body, result)
	if err != nil {
		storer.ProcUtil.MessageLog.Error("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		message.Finish()
		return fmt.Errorf("Could not unmarshal JSON data from nsq")
	}
	result.NsqMessage = message
	bagIdentifier := result.BagIdentifier
	if bagIdentifier == "" {
		bagIdentifier = "DPN Replication Bag"
	}
	storer.ProcUtil.MessageLog.Info("Putting %s into the storage queue (%s)",
		result.TarFilePath, bagIdentifier)
	storer.DigestChannel <- result
	return nil
}

func (storer *Storer) calculateDigest() {
	for result := range storer.DigestChannel {
		if result.Md5Digest != "" {
			storer.StorageChannel <- result
		}
		md5Hash := md5.New()
		reader, err := os.Open(result.TarFilePath)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error opening file '%s': %v",
				result.TarFilePath, err)
			storer.PostProcessChannel <- result
			continue
		}
		fileInfo, err := reader.Stat()
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Cannot stat file '%s': %v",
				result.TarFilePath, err)
			storer.PostProcessChannel <- result
			continue
		}
		bytesWritten, err := io.Copy(md5Hash, reader)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error running md5 checksum on file '%s': %v",
				result.TarFilePath, err)
			storer.PostProcessChannel <- result
			continue
		}
		if bytesWritten != fileInfo.Size() {
			result.ErrorMessage = fmt.Sprintf("Error running md5 checksum on file '%s': " +
				"read only %d of %d bytes.",
				result.TarFilePath, bytesWritten, fileInfo.Size())
			storer.PostProcessChannel <- result
			continue
		}
		reader.Close()
		result.Md5Digest = fmt.Sprintf("%x", md5Hash.Sum(nil))
		storer.StorageChannel <- result
	}
}

func (storer *Storer) store() {
	for result := range storer.StorageChannel {
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

		reader, err := os.Open(result.TarFilePath)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error opening file '%s': %v",
				result.TarFilePath, err)
			storer.PostProcessChannel <- result
			continue
		}
		fileInfo, err := reader.Stat()
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Cannot stat file '%s': %v",
				result.TarFilePath, err)
			storer.PostProcessChannel <- result
			continue
		}
		options, err := storer.GetS3Options(result)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error generating S3 options: %v", err)
			storer.PostProcessChannel <- result
			continue
		}

		fileName := fmt.Sprintf("%s.tar", result.UUID)
		url, err := storer.ProcUtil.S3Client.SaveToS3(
			storer.ProcUtil.Config.DPNPreservationBucket,
			fileName,
			"application/x-tar",
			reader,
			fileInfo.Size(),
			options)
		reader.Close()
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error saving file to S3/Glacier: %v", err)
			storer.PostProcessChannel <- result
			continue
		}

		result.StorageURL = url

		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

		storer.CleanupChannel <- result
	}
}

func (storer *Storer) cleanup() {
	for result := range storer.CleanupChannel {
		if result.ErrorMessage == "" && result.StorageURL != "" {
			err := os.Remove(result.TarFilePath)
			if err != nil {
				storer.ProcUtil.MessageLog.Warning("Error cleaning up %s: %v",
					result.TarFilePath, err)
			} else {
				storer.ProcUtil.MessageLog.Info(
					"After successful upload, deleted local DPN bag at %s",
					result.TarFilePath)
			}
		}
		storer.PostProcessChannel <- result
	}
}

func (storer *Storer) postProcess() {
	for result := range storer.PostProcessChannel {
		bagIdentifier := result.BagIdentifier
		if bagIdentifier == "" {
			bagIdentifier = result.UUID
		}
		if result.ErrorMessage == "" && result.StorageURL != "" {
			// SUCCESS :)
			storer.ProcUtil.MessageLog.Info("Bag %s successfully stored at %s",
				bagIdentifier, result.StorageURL)
			storer.ProcUtil.IncrementSucceeded()
			// Send to queue for recording in Fluctus and/or DPN REST
			if result.NsqMessage != nil {
				result.NsqMessage.Finish()
				storer.SendToRecordQueue(result)
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
					storer.SendToTroubleQueue(result)
				} else {
					storer.ProcUtil.MessageLog.Info("Requeuing %s (%s)",
						bagIdentifier, result.TarFilePath)
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

func (storer *Storer) GetS3Options(result *StorageResult) (s3.Options, error) {
	// Prepare metadata for save to S3
	s3Metadata := make(map[string][]string)
	if result.BagIdentifier != "" {
		s3Metadata["aptrust-bag"] = []string{result.BagIdentifier}
	}
	// Save to S3 with the base64-encoded md5 sum
	base64md5, err := bagman.Base64EncodeMd5(result.Md5Digest)
	if err != nil {
		return s3.Options{}, err
	}
	options := storer.ProcUtil.S3Client.MakeOptions(base64md5, s3Metadata)
	return options, nil
}


func (storer *Storer) SendToRecordQueue(result *StorageResult) {
	// Record has to record PREMIS event in Fluctus if
	// BagIdentifier is present. It will definitely have
	// to record information in the DPN REST API.
	err := bagman.Enqueue(storer.ProcUtil.Config.NsqdHttpAddress,
		storer.ProcUtil.Config.DPNRecordWorker.NsqTopic, result)
	if err != nil {
		bagIdentifier := result.BagIdentifier
		if bagIdentifier == "" {
			bagIdentifier = result.UUID
		}
		message := fmt.Sprintf("Could not send '%s' (at %s) to record queue: %v",
			bagIdentifier, result.TarFilePath, err)
		result.ErrorMessage += message
		storer.ProcUtil.MessageLog.Error(message)
		storer.SendToTroubleQueue(result)
	}
}

func (storer *Storer) SendToTroubleQueue(result *StorageResult) {
	result.ErrorMessage += " This item has been queued for administrative review."
	err := bagman.Enqueue(storer.ProcUtil.Config.NsqdHttpAddress,
		storer.ProcUtil.Config.DPNTroubleWorker.NsqTopic, result)
	if err != nil {
		storer.ProcUtil.MessageLog.Error("Could not send '%s' to trouble queue: %v",
			result.BagIdentifier, err)
		storer.ProcUtil.MessageLog.Error("Original error on '%s' was %s",
			result.BagIdentifier, result.ErrorMessage)
	}
}

func (storer *Storer) RunTest(result *StorageResult) {
	storer.WaitGroup.Add(1)
	storer.ProcUtil.MessageLog.Info("Putting %s into digest channel",
		result.BagIdentifier)
	storer.DigestChannel <- result
	storer.WaitGroup.Wait()
	fmt.Println("Storer is done")
}
