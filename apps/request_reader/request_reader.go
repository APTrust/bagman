// request_reader periodically checks the processed items list
// in Fluctus for the following:
//
// 1. Intellectual objects that users want to restore.
// 2. Generic files that users want to delete.
//
// It queues those items in nsqd.
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
	var err error
	workReader, err = workers.InitializeReader()
	workReader.MessageLog.Info("request_reader started")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Initialization failed for request_reader: %v", err)
		os.Exit(1)
	}
	queueAllRestorationItems()
	queueAllDeletionItems()
}

// Find all the Intellectual Objects that need to be restored & add them to
// the NSQ restoration queue.
func queueAllRestorationItems() {
	workReader.MessageLog.Info("Asking Fluctus for objects to restore")
	results, err := workReader.FluctusClient.RestorationItemsGet("")
	if err != nil {
		workReader.MessageLog.Fatalf("Error getting items for restoration: %v", err)
	}
	workReader.MessageLog.Info("Found %d items to restore", len(results))
	queueBatch(results, "restoration")
}

// Find all the Generic Files that need to be deleted & add them to
// the NSQ restoration queue.
func queueAllDeletionItems() {
	workReader.MessageLog.Info("Asking Fluctus for files to delete")
	results, err := workReader.FluctusClient.DeletionItemsGet("")
	if err != nil {
		workReader.MessageLog.Fatalf("Error getting items for deletion: %v", err)
	}
	workReader.MessageLog.Info("Found %d items to delete", len(results))
	queueBatch(results, "delete")
}


// Queues a batch of items for restoration or deletion,
// depending on the url param.
func queueBatch(results []*bagman.ProcessStatus, queueName string) {
	start := 0
	end := bagman.Min(len(results), batchSize)
	for start <= end {
		batch := results[start:end]
		workReader.MessageLog.Info("Queuing batch of %d items", len(batch))
		url := fmt.Sprintf("%s/mput?topic=%s", workReader.Config.NsqdHttpAddress,
			workReader.Config.RestoreWorker.NsqTopic)
		if queueName == "delete" {
			url = fmt.Sprintf("%s/mput?topic=%s", workReader.Config.NsqdHttpAddress,
				workReader.Config.FileDeleteWorker.NsqTopic)
		}
		genericSlice := make([]interface{}, len(batch))
		for i := range batch {
			genericSlice[i] = batch[i]
		}
		bagman.QueueToNSQ(url, genericSlice)
		logBatch(batch, queueName)
		start = end + 1
		if start < len(results) {
			end = bagman.Min(len(results), start+batchSize)
		}
		time.Sleep(time.Millisecond * waitMilliseconds)
	}
}

func logBatch(statusList []*bagman.ProcessStatus, queueName string) {
	for _, processStatus := range statusList {
		workReader.MessageLog.Info("Put %s/%s into %s queue",
			processStatus.Institution, processStatus.Name, queueName)
	}
}
