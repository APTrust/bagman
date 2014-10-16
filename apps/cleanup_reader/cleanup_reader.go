// cleanup_reader periodically checks the processed items list
// in Fluctus for items that should be deleted. It queues those
// items for cleanup in nsqd.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"net/http"
	"os"
	"strings"
	"time"
)

// Queue delete requests in batches of 50.
// Wait X milliseconds between batches.
const (
	batchSize        = 50
	waitMilliseconds = 1000
)

var workReader *bagman.WorkReader

func main() {
	var err error = nil
	workReader, err = workers.InitializeReader()
	workReader.MessageLog.Info("cleanup_reader started")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Initialization failed for cleanup_reader: %v", err)
		os.Exit(1)
	}
	run()
}

func run() {
	url := fmt.Sprintf("%s/mput?topic=%s", workReader.Config.NsqdHttpAddress,
		workReader.Config.BagDeleteWorker.NsqTopic)
	workReader.MessageLog.Info("Sending files to clean up to %s", url)

	results, err := workReader.FluctusClient.GetReviewedItems()
	if err != nil {
		workReader.MessageLog.Fatalf("Error getting reviewed items: %v", err)
	}

	workReader.MessageLog.Info("Found %d items to clean up", len(results))

	start := 0
	end := bagman.Min(len(results), batchSize)
	for start <= end {
		batch := results[start:end]
		workReader.MessageLog.Info("Queuing batch of %d items", len(batch))
		enqueue(url, batch)
		start = end + 1
		if start < len(results) {
			end = bagman.Min(len(results), start+batchSize)
		}
		time.Sleep(time.Millisecond * waitMilliseconds)
	}
}

// enqueue adds a batch of items to the nsqd work queue
func enqueue(url string, results []*bagman.CleanupResult) {
	jsonData := make([]string, len(results))
	for i, result := range results {
		json, err := json.Marshal(result)
		if err != nil {
			workReader.MessageLog.Error("Error marshalling cleanup result to JSON: %s", err.Error())
		} else {
			jsonData[i] = string(json)
			workReader.MessageLog.Info("Put %s into cleanup queue", result.BagName)
		}
	}
	batch := strings.Join(jsonData, "\n")
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(batch)))
	if err != nil {
		workReader.MessageLog.Error("nsqd returned an error: %s", err.Error())
	}
	if resp == nil {
		msg := "No response from nsqd. Is it running? cleanup_reader is quitting."
		workReader.MessageLog.Error(msg)
		fmt.Println(msg)
		os.Exit(1)
	} else if resp.StatusCode != 200 {
		workReader.MessageLog.Error("nsqd returned status code %d on last mput", resp.StatusCode)
	}
}
