// BucketReader gets a list of files awaiting processing in the S3 intake buckets
// and adds each item to the queue for processing.
package main

import (
	"bytes"
	"flag"
	"log"
	"fmt"
	"encoding/json"
	"net/http"
	"github.com/APTrust/bagman"
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
	bagman.PrintConfig(config)
}


func run() {
	bucketSummaries, err := bagman.CheckAllBuckets(config.Buckets)
	if err != nil {
		messageLog.Println("[ERROR]", err)
		return
	}
	url := fmt.Sprintf("%s/put?topic=%s", config.NsqdHttpAddress,
		config.BagProcessorTopic)
	messageLog.Printf("[INFO] Sending S3 file info to %s \n", url)
	for _, bucketSummary := range bucketSummaries {
		for _, key := range bucketSummary.Keys {
			if key.Size < config.MaxFileSize {
				// TODO: Set attempt number correctly when queue is working.
				s3File := bagman.S3File{bucketSummary.BucketName, key, nil}
				enqueue(url, s3File)
			}
		}
	}
}

// enqueue adds an item to the nsqd work queue
func enqueue(url string, s3File bagman.S3File) {
	jsonData, err := json.Marshal(s3File)
	if err != nil {
		messageLog.Printf("[ERROR] Error marshalling s3 file to JSON: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		messageLog.Printf("[ERROR] nsqd returned an error: %v", err)
	}
	// TODO: Check for 200 response
	fmt.Println(resp)
	messageLog.Println("[INFO]", "Put", s3File.Key.Key, "into fetch queue")
}
