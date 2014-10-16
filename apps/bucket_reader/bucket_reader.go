// BucketReader gets a list of files awaiting processing in the S3
// intake buckets and adds each item to the queue for processing.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"github.com/crowdmob/goamz/aws"
	"net/http"
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

func run() {
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		workReader.MessageLog.Error(err.Error())
		return
	}
	bucketSummaries, err := s3Client.CheckAllBuckets(workReader.Config.ReceivingBuckets)
	if err != nil {
		workReader.MessageLog.Error(err.Error())
		return
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
		enqueue(url, batch)
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
			if workReader.Config.MaxFileSize == 0 || key.Size < workReader.Config.MaxFileSize {
				s3Files = append(s3Files, &bagman.S3File{
					BucketName: bucketSummary.BucketName,
					Key: key})
			}
		}
	}
	return s3Files
}

// Remove S3 files that have been processed successfully.
// No need to reprocess those!
func filterProcessedFiles(s3Files []*bagman.S3File) (filesToProcess []*bagman.S3File) {
	for _, s3File := range s3Files {
		bagDate, err := time.Parse(bagman.S3DateFormat, s3File.Key.LastModified)
		if err != nil {
			workReader.MessageLog.Error("Cannot parse S3File mod date '%s'. "+
				"File %s will be re-processed.",
				s3File.Key.LastModified, s3File.Key.Key)
			filesToProcess = append(filesToProcess, s3File)
			continue
		}
		etag := strings.Replace(s3File.Key.ETag, "\"", "", 2)
		status := findInStatusCache(etag, s3File.Key.Key, bagDate)
		if status == nil {
			status, err = workReader.FluctusClient.GetBagStatus(etag, s3File.Key.Key, bagDate)
		}
		if err != nil {
			workReader.MessageLog.Error("Cannot get Fluctus bag status for %s. "+
				"Will re-process bag. Error was %v", s3File.Key.Key, err)
			filesToProcess = append(filesToProcess, s3File)
		} else if status == nil || status.ShouldTryIngest() {
			reason := "Bag has not yet been successfully processed."
			if status == nil {
				err = createFluctusRecord(s3File)
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

func createFluctusRecord(s3File *bagman.S3File) (err error) {
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
	status.Status = bagman.StatusPending
	status.Note = "Item is in receiving bucket. Processing has not started."
	status.Institution = bagman.OwnerOf(s3File.BucketName)
	status.Outcome = string(status.Status)
	status.Reviewed = false

	// Retry should be true until we know for sure that there
	// is something wrong with the bag.
	status.Retry = true

	err = workReader.FluctusClient.UpdateProcessedItem(status)
	if err != nil {
		return err
	}
	workReader.MessageLog.Info("Created Fluctus ProcessedItem for %s\n",
		s3File.Key.Key)
	return nil
}

// enqueue adds a batch of items to the nsqd work queue
func enqueue(url string, s3Files []*bagman.S3File) {
	jsonData := make([]string, len(s3Files))
	for i, s3File := range s3Files {
		json, err := json.Marshal(s3File)
		if err != nil {
			workReader.MessageLog.Error("Error marshalling s3 file to JSON: %v", err)
		} else {
			jsonData[i] = string(json)
			workReader.MessageLog.Info("Put %s into fetch queue", s3File.Key.Key)
		}
	}
	batch := strings.Join(jsonData, "\n")
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(batch)))
	if err != nil {
		workReader.MessageLog.Error("nsqd returned an error: %v", err)
	}
	if resp == nil {
		msg := "No response from nsqd. Is it running? bucket_reader is quitting."
		workReader.MessageLog.Error(msg)
		fmt.Println(msg)
		os.Exit(1)
	} else if resp.StatusCode != 200 {
		workReader.MessageLog.Error("nsqd returned status code %d on last mput", resp.StatusCode)
	}
}
