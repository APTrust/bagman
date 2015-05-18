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

type Storer struct {
	DigestChannel       chan *DPNResult
	StorageChannel      chan *DPNResult
	CleanupChannel      chan *DPNResult
	PostProcessChannel  chan *DPNResult
	ProcUtil            *bagman.ProcessUtil
	// WaitGroup is for running local tests only.
	WaitGroup           sync.WaitGroup
}

func NewStorer(procUtil *bagman.ProcessUtil) (*Storer) {
	storer := &Storer{
		ProcUtil: procUtil,
	}
	workerBufferSize := procUtil.Config.DPNStoreWorker.Workers * 10
	storer.DigestChannel = make(chan *DPNResult, workerBufferSize)
	storer.StorageChannel = make(chan *DPNResult, workerBufferSize)
	storer.CleanupChannel = make(chan *DPNResult, workerBufferSize)
	storer.PostProcessChannel = make(chan *DPNResult, workerBufferSize)
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
	var result *DPNResult
	err := json.Unmarshal(message.Body, result)
	if err != nil {
		storer.ProcUtil.MessageLog.Error("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		message.Finish()
		return fmt.Errorf("Could not unmarshal JSON data from nsq")
	}
	result.NsqMessage = message
	result.Stage = STAGE_STORE
	bagIdentifier := result.BagIdentifier
	if bagIdentifier == "" {
		bagIdentifier = "DPN Replication Bag"
	}
	storer.ProcUtil.MessageLog.Info("Putting %s into the storage queue (%s)",
		result.PackageResult.TarFilePath, bagIdentifier)
	storer.DigestChannel <- result
	return nil
}

func (storer *Storer) calculateDigest() {
	for result := range storer.DigestChannel {
		if result.StorageResult.Md5Digest != "" {
			storer.StorageChannel <- result
		}
		md5Hash := md5.New()
		reader, err := os.Open(result.PackageResult.TarFilePath)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error opening file '%s': %v",
				result.PackageResult.TarFilePath, err)
			storer.PostProcessChannel <- result
			continue
		}
		fileInfo, err := reader.Stat()
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Cannot stat file '%s': %v",
				result.PackageResult.TarFilePath, err)
			storer.PostProcessChannel <- result
			continue
		}
		bytesWritten, err := io.Copy(md5Hash, reader)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error running md5 checksum on file '%s': %v",
				result.PackageResult.TarFilePath, err)
			storer.PostProcessChannel <- result
			continue
		}
		if bytesWritten != fileInfo.Size() {
			result.ErrorMessage = fmt.Sprintf("Error running md5 checksum on file '%s': " +
				"read only %d of %d bytes.",
				result.PackageResult.TarFilePath, bytesWritten, fileInfo.Size())
			storer.PostProcessChannel <- result
			continue
		}
		reader.Close()
		result.StorageResult.Md5Digest = fmt.Sprintf("%x", md5Hash.Sum(nil))
		storer.StorageChannel <- result
	}
}

func (storer *Storer) store() {
	for result := range storer.StorageChannel {
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

		reader, err := os.Open(result.PackageResult.TarFilePath)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error opening file '%s': %v",
				result.PackageResult.TarFilePath, err)
			storer.PostProcessChannel <- result
			continue
		}
		fileInfo, err := reader.Stat()
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Cannot stat file '%s': %v",
				result.PackageResult.TarFilePath, err)
			storer.PostProcessChannel <- result
			continue
		}
		options, err := storer.GetS3Options(result)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Error generating S3 options: %v", err)
			storer.PostProcessChannel <- result
			continue
		}

		fileName := fmt.Sprintf("%s.tar", result.PackageResult.BagBuilder.UUID)
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

		result.StorageResult.StorageURL = url

		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

		storer.CleanupChannel <- result
	}
}

func (storer *Storer) cleanup() {
	for result := range storer.CleanupChannel {
		if result.ErrorMessage == "" && result.StorageResult.StorageURL != "" {
			err := os.Remove(result.PackageResult.TarFilePath)
			if err != nil {
				storer.ProcUtil.MessageLog.Warning("Error cleaning up %s: %v",
					result.PackageResult.TarFilePath, err)
			} else {
				storer.ProcUtil.MessageLog.Info(
					"After successful upload, deleted local DPN bag at %s",
					result.PackageResult.TarFilePath)
			}
		}
		storer.PostProcessChannel <- result
	}
}

func (storer *Storer) postProcess() {
	for result := range storer.PostProcessChannel {
		bagIdentifier := result.BagIdentifier
		if bagIdentifier == "" {
			bagIdentifier = result.PackageResult.BagBuilder.UUID
		}
		if result.ErrorMessage == "" && result.StorageResult.StorageURL != "" {
			// SUCCESS :)
			storer.ProcUtil.MessageLog.Info("Bag %s successfully stored at %s",
				bagIdentifier, result.StorageResult.StorageURL)
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
						bagIdentifier, result.PackageResult.TarFilePath)
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
	base64md5, err := bagman.Base64EncodeMd5(result.StorageResult.Md5Digest)
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
	storer.DigestChannel <- result
	storer.WaitGroup.Wait()
	fmt.Println("Storer is done")
}
