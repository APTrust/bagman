// fixity_reader periodically queries Fluctus for GenericFiles
// that haven't had a fixity check in X days. The number of
// days is specified in the config file. It then queues those
// items for fixity check in nsqd.
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
	batchSize        = 500
	waitMilliseconds = 1000
)

var workReader *bagman.WorkReader

func main() {
	var err error = nil
	workReader, err = workers.InitializeReader()
	workReader.MessageLog.Info("fixity_reader started")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Initialization failed for fixity_reader: %v", err)
		os.Exit(1)
	}
	run()
}

func run() {
	url := fmt.Sprintf("%s/mput?topic=%s", workReader.Config.NsqdHttpAddress,
		workReader.Config.FixityWorker.NsqTopic)
	workReader.MessageLog.Info("Sending files needing fixity check to %s", url)

	daysAgo := time.Duration(workReader.Config.MaxDaysSinceFixityCheck * -24) * time.Hour
	sinceWhen := time.Now().UTC().Add(daysAgo)
	genericFiles, err := workReader.FluctusClient.GetFilesNotCheckedSince(sinceWhen)

	if err != nil {
		workReader.MessageLog.Fatalf("Error getting items items needing fixity check: %v", err)
	}

	workReader.MessageLog.Info("Found %d items needing fixity check", len(genericFiles))

	// Create ProcessedItem records for these? Or do this in Rails?

	start := 0
	end := bagman.Min(len(genericFiles), batchSize)
	for start <= end {
		batch := genericFiles[start:end]
		workReader.MessageLog.Info("Queuing batch of %d items", len(batch))
		enqueue(url, batch)
		start = end + 1
		if start < len(genericFiles) {
			end = bagman.Min(len(genericFiles), start+batchSize)
		}
		time.Sleep(time.Millisecond * waitMilliseconds)
	}
}

// enqueue adds a batch of items to the nsqd work queue
func enqueue(url string, genericFiles []*bagman.GenericFile) {
	jsonData := make([]string, len(genericFiles))
	for i, genericFile := range genericFiles {
		fixityResult := bagman.NewFixityResult(genericFile)
		json, err := json.Marshal(fixityResult)
		if err != nil {
			workReader.MessageLog.Error("Error marshalling FixityResult to JSON: %s", err.Error())
		} else {
			jsonData[i] = string(json)
			workReader.MessageLog.Info("Put %s into fixity_check queue (%s)",
				genericFile.Identifier, genericFile.URI)
		}
	}
	batch := strings.Join(jsonData, "\n")
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(batch)))
	if err != nil {
		workReader.MessageLog.Error("nsqd returned an error: %s", err.Error())
	}
	if resp == nil {
		msg := "No response from nsqd. Is it running? fixity_reader is quitting."
		workReader.MessageLog.Error(msg)
		fmt.Println(msg)
		os.Exit(1)
	} else if resp.StatusCode != 200 {
		workReader.MessageLog.Error("nsqd returned status code %d on last mput", resp.StatusCode)
	}
}
