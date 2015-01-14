// BucketReader gets a list of files awaiting processing in the S3
// intake buckets and adds each item to the queue for processing.
package main

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"github.com/crowdmob/goamz/aws"
	"os"
	"strings"
	"time"
)

// Send S3 files to queue in batches of 500.
// Wait X milliseconds between batches. The wait time is really
// only necessary when the queue is running on an AWS small EC2
// instance, where the number of open network connections is
// severely restricted.
const (
	batchSize        = 500
	waitMilliseconds = 5000
)

var workReader *bagman.WorkReader
var statusCache map[string]*bagman.ProcessStatus

func main() {
	var err error = nil
	workReader, err = workers.InitializeReader()
	workReader.MessageLog.Info("bucket_reader started")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Initialization failed for bucket_reader: %v", err)
		os.Exit(1)
	}
	run()
}

type DateParseError struct {
    message   string
}
func (e DateParseError) Error() string { return e.message }

func run() {
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		workReader.MessageLog.Error(err.Error())
		return
	}
	bucketSummaries, errors := s3Client.CheckAllBuckets(workReader.Config.ReceivingBuckets)
	for _, err := range errors {
		workReader.MessageLog.Error(err.Error())
	}
	loadStatusCache()
	url := fmt.Sprintf("%s/mput?topic=%s", workReader.Config.NsqdHttpAddress, workReader.Config.PrepareWorker.NsqTopic)
	workReader.MessageLog.Debug("Sending S3 file info to %s", url)
	s3Files := filterLargeFiles(bucketSummaries)
	workReader.MessageLog.Debug("%d S3 Files are within our size limit",
		len(s3Files))
	filesToProcess := s3Files
	// SkipAlreadyProcessed will almost always be true.
	// The exception is when we want to reprocess items to test new code.
	if workReader.Config.SkipAlreadyProcessed == true {
		workReader.MessageLog.Info("Skipping already processed files, because config says so")
		filesToProcess = filterProcessedFiles(s3Files)
	} else {
		workReader.MessageLog.Info("Reprocessing already processed files, because config says so")
	}
	start := 0
	end := bagman.Min(len(filesToProcess), batchSize)
	workReader.MessageLog.Info("%d Unprocessed files", len(filesToProcess))
	for start <= end {
		batch := filesToProcess[start:end]
		workReader.MessageLog.Info("Queuing batch of %d items", len(batch))
		genericSlice := make([]interface{}, len(batch))
		for i := range batch {
			genericSlice[i] = batch[i]
		}
		bagman.QueueToNSQ(url, genericSlice)
		if err != nil {
			workReader.MessageLog.Fatal(err.Error())
		}
		logBatch(batch)
		start = end + 1
		if start < len(filesToProcess) {
			end = bagman.Min(len(filesToProcess), start+batchSize)
		}
		// Sleep so we don't max out connections on EC2 small.
		// The utility server is running a lot of other network I/O
		// in addition to our queue.
		time.Sleep(time.Millisecond * waitMilliseconds)
	}
}

// filterLargeFiles returns only those S3 files that are not larger
// than config.MaxFileSize. This is useful when running tests and
// demos on your local machine, so that you can limit your test
// runs to files under 100k (or whatever you set in config.json).
// You don't want to pull down lots of multi-gig files when you're
// just running local tests. In production, set maxFileSize to
// zero, or to some huge value to get all files.
func filterLargeFiles(bucketSummaries []*bagman.BucketSummary) (s3Files []*bagman.S3File) {
	for _, bucketSummary := range bucketSummaries {
		for _, key := range bucketSummary.Keys {
			s3File := &bagman.S3File{
				BucketName: bucketSummary.BucketName,
				Key: key,
			}
			if workReader.Config.MaxFileSize == 0 || key.Size < workReader.Config.MaxFileSize {
				// OK. Process this.
				s3Files = append(s3Files, s3File)
			} else {
				// Too big. Add a record to fluctus so partner admin can see it.
				tellFluctusWeWontProcessThis(s3File)
			}
		}
	}
	return s3Files
}

func getStatusRecord(s3File *bagman.S3File) (status *bagman.ProcessStatus, err error) {
	bagDate, err := time.Parse(bagman.S3DateFormat, s3File.Key.LastModified)
	if err != nil {
		msg := fmt.Sprintf("Cannot parse S3File mod date '%s'. "+
			"File %s will be re-processed.",
			s3File.Key.LastModified, s3File.Key.Key)
		return nil, DateParseError { message: msg, }
	}
	etag := strings.Replace(s3File.Key.ETag, "\"", "", 2)
	status = findInStatusCache(etag, s3File.Key.Key, bagDate)
	if status == nil {
		status, err = workReader.FluctusClient.GetBagStatus(etag, s3File.Key.Key, bagDate)
	}
	return status, err
}

// Remove S3 files that have been processed successfully.
// No need to reprocess those!
func filterProcessedFiles(s3Files []*bagman.S3File) (filesToProcess []*bagman.S3File) {
	for _, s3File := range s3Files {
		status, err := getStatusRecord(s3File)
		if err != nil {
			_, isDateParseError := err.(DateParseError)
			if isDateParseError {
				workReader.MessageLog.Error(err.Error())
				filesToProcess = append(filesToProcess, s3File)
				continue
			}
		}
		if err != nil {
			workReader.MessageLog.Error("Cannot get Fluctus bag status for %s. "+
				"Will re-process bag. Error was %v", s3File.Key.Key, err)
			filesToProcess = append(filesToProcess, s3File)
		} else if status == nil || status.ShouldTryIngest() {
			reason := "Bag has not yet been successfully processed."
			if status == nil {
				err = createFluctusRecord(s3File, true)
				if err != nil {
					// TODO: Notify someone?
					workReader.MessageLog.Error("Could not create Fluctus ProcessedItem "+
						"for %s: %v", s3File.Key.Key, err)
				}
			}
			workReader.MessageLog.Info("Will process bag %s: %s", s3File.Key.Key, reason)
			filesToProcess = append(filesToProcess, s3File)
		} else if status.Status != "Failed" && workReader.Config.SkipAlreadyProcessed == true {
			workReader.MessageLog.Debug("Skipping %s: already processed successfully.", s3File.Key.Key)
		} else if status.Retry == false {
			workReader.MessageLog.Debug("Skipping %s: retry flag is set to false.", s3File.Key.Key)
		}
	}
	return filesToProcess
}

// Loads status of all bags received in the past two hours from fluctus
// in a single call.
func loadStatusCache() {
	twoHoursAgo := time.Now().Add(time.Hour * -2)
	statusRecords, err := workReader.FluctusClient.BulkStatusGet(twoHoursAgo)
	if err != nil {
		workReader.MessageLog.Warning("Could not get bulk status records")
	} else {
		workReader.MessageLog.Info("Got %d status records from the fluctopus\n", len(statusRecords))
		statusCache = make(map[string]*bagman.ProcessStatus, len(statusRecords))
		for i := range statusRecords {
			record := statusRecords[i]
			key := fmt.Sprintf("%s%s%s", record.ETag, record.Name, record.BagDate)
			statusCache[key] = record
		}
	}
}

// Finds the status of the specified tar bag in the cache that
// we retrieved from Fluctus. The cache can save us hundreds or
// thousands of HTTP calls each time the bucket reader runs.
func findInStatusCache(etag, name string, bagDate time.Time) *bagman.ProcessStatus {
	key := fmt.Sprintf("%s%s%s", etag, name, bagDate)
	item, exists := statusCache[key]
	if exists {
		//workReader.MessageLog.Debug("Found item in cache: %s\n", name)
		return item
	}
	//workReader.MessageLog.Debug("Item not in cache. Will have to ask the fluctopus for %s\n", name)
	return nil
}

func tellFluctusWeWontProcessThis(s3File *bagman.S3File) {
	status, _ := getStatusRecord(s3File)
	if status == nil {
		err := createFluctusRecord(s3File, false)
		if err != nil {
			// TODO: Notify someone?
			workReader.MessageLog.Error("Could not create Fluctus ProcessedItem "+
				"for %s: %v", s3File.Key.Key, err)
		} else {
			workReader.MessageLog.Info("%s will not be processed because it is %d bytes " +
				"and the size limit for this system is %d bytes.",
				s3File.Key.Key, s3File.Key.Size, workReader.Config.MaxFileSize)
		}
	}
}

func createFluctusRecord(s3File *bagman.S3File, tryToIngest bool) (err error) {
	status := &bagman.ProcessStatus{}
	status.Date = time.Now().UTC()
	status.Action = "Ingest"
	status.Name = s3File.Key.Key
	bagDate, _ := time.Parse(bagman.S3DateFormat, s3File.Key.LastModified)
	status.BagDate = bagDate
	status.Bucket = s3File.BucketName
	// Strip the quotes off the ETag
	status.ETag = strings.Replace(s3File.Key.ETag, "\"", "", 2)
	status.Stage = bagman.StageReceive
	status.Institution = bagman.OwnerOf(s3File.BucketName)
	status.Reviewed = false

	if tryToIngest == true {
		status.Note = "Item is in receiving bucket. Processing has not started."
		status.Status = bagman.StatusPending
		status.Retry = true
	} else {
		status.Note = fmt.Sprintf("Item will not be processed because it is %d bytes " +
			"and the size limit for this system is %d bytes.",
			s3File.Key.Size, workReader.Config.MaxFileSize)
		status.Status = bagman.StatusFailed
		status.Retry = false
	}
	status.Outcome = string(status.Status)


	err = workReader.FluctusClient.UpdateProcessedItem(status)
	if err != nil {
		return err
	}
	workReader.MessageLog.Info("Created Fluctus ProcessedItem for %s\n",
		s3File.Key.Key)
	return nil
}

func logBatch(s3Files []*bagman.S3File) {
	for _, s3File := range s3Files {
		workReader.MessageLog.Info("Put %s into fetch queue", s3File.Key.Key)
	}
}
