// BucketReader gets a list of files awaiting processing in the S3
// intake buckets and adds each item to the queue for processing.
package main

import (
	"bytes"
	"flag"
	"log"
	"fmt"
	"strings"
	"time"
	"encoding/json"
	"net/http"
	"github.com/APTrust/bagman"
)

// Send S3 files to queue in batches of 500.
// Wait X milliseconds between batches. The wait time is really
// only necessary when the queue is running on an AWS small EC2
// instance, where the number of open network connections is
// severely restricted.
const (
	batchSize = 500
	waitMilliseconds = 5000
)
var config bagman.Config
var jsonLog *log.Logger
var messageLog *log.Logger


func main() {
	initialize()
	run()
}

func initialize() {
	// Load the config or die.
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	config = bagman.LoadRequestedConfig(requestedConfig)
	jsonLog, messageLog = bagman.InitLoggers(config.LogDirectory, "bucket_reader")
}


func run() {
	bucketSummaries, err := bagman.CheckAllBuckets(config.Buckets)
	if err != nil {
		messageLog.Println("[ERROR]", err)
		return
	}
	url := fmt.Sprintf("%s/mput?topic=%s", config.NsqdHttpAddress,
		config.BagProcessorTopic)
	messageLog.Printf("[INFO] Sending S3 file info to %s \n", url)
	s3Files := filterLargeFiles(bucketSummaries)
	start := 0
	end := min(len(s3Files) - 1, batchSize)
	for start < end {
		batch := s3Files[start:end]
		enqueue(url, batch)
		start = end + 1
		if start < len(s3Files) {
			end = min(len(s3Files) - 1, start + batchSize)
		}
		// Sleep so we don't max out connections on EC2 small.
		// The utility server is running a lot of other network I/O
		// in addition to our queue.
		time.Sleep(time.Millisecond * waitMilliseconds)
	}
}

// min returns the minimum of x or y. The Math package has this function
// but you have to cast to floats.
func min (x, y int) (int) {
	if x < y {
		return x
	} else {
		return y
	}
}


// filterLargeFiles returns only those S3 files that are not larger
// than config.MaxFileSize. This is useful when running tests and
// demos on your local machine, so that you can limit your test
// runs to files under 100k (or whatever you set in config.json).
// You don't want to pull down lots of multi-gig files when you're
// just running local tests. In production, set maxFileSize to
// zero, or to some huge value to get all files.
func filterLargeFiles (bucketSummaries []*bagman.BucketSummary) (s3Files []*bagman.S3File) {
	for _, bucketSummary := range bucketSummaries {
		for _, key := range bucketSummary.Keys {
			if config.MaxFileSize == 0 || key.Size < config.MaxFileSize {
				s3Files = append(s3Files, &bagman.S3File{bucketSummary.BucketName, key})
			}
		}
	}
	return s3Files
}

// enqueue adds a batch of items to the nsqd work queue
func enqueue(url string, s3Files []*bagman.S3File) {
	jsonData := make([]string, len(s3Files))
	for i, s3File := range s3Files {
		json, err := json.Marshal(s3File)
		if err != nil {
			messageLog.Printf("[ERROR] Error marshalling s3 file to JSON: %v", err)
		} else {
			jsonData[i] = string(json)
			messageLog.Println("[INFO]", "Put", s3File.Key.Key, "into fetch queue")
			//fmt.Println(s3File.Key.Key)
		}
	}
	batch := strings.Join(jsonData, "\n")
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(batch)))
	if err != nil {
		messageLog.Printf("[ERROR] nsqd returned an error: %v", err)
	}
	if resp.StatusCode != 200 {
		messageLog.Printf("[ERROR] nsqd returned status code %d on last mput", resp.StatusCode)
	}
}
