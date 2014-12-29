// cleanup_reader periodically checks the processed items list
// in Fluctus for bags that should be deleted from the receiving
// buckets. It queues those items for cleanup in nsqd.
package main

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"os"
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
		genericSlice := make([]interface{}, len(batch))
		for i := range batch {
			genericSlice[i] = batch[i]
		}
		bagman.QueueToNSQ(url, genericSlice)
		logBatch(batch)
		start = end + 1
		if start < len(results) {
			end = bagman.Min(len(results), start+batchSize)
		}
		time.Sleep(time.Millisecond * waitMilliseconds)
	}
}

func logBatch(results []*bagman.CleanupResult) {
	for _, result := range results {
		workReader.MessageLog.Info("Put %s into cleanup queue", result.BagName)
	}
}
