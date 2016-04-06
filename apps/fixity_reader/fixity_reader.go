// fixity_reader periodically queries Fluctus for GenericFiles
// that haven't had a fixity check in X days. The number of
// days is specified in the config file. It then queues those
// items for fixity check in nsqd. This runs as a daily cron job
// on the production server.
//
// It calculates the 'sinceWhen' date from the MaxDaysSinceFixityCheck
// setting in the config file, but you can override that setting with
// the command-line flag -date.
//
// Sample Usage:
//
// fixity_reader -config=<config> [-date='2014-11-19T19:16:38Z']
package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"os"
	"time"
)

// Ideally, we'd get a batch of 200 or more items at a time,
// but Fedora/Solr is too slow. See PivotalTracker #112606953.
// When that's fixed, we can increase the batch size.
const (
	batchSize = 100
)

var workReader *bagman.WorkReader
var cmdLineDate = flag.String("date", "", "Find files with no fixity check since this date")
var limit = flag.Int("limit", 0, "Queue only this many items, then stop")

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
	sinceWhen := getSinceWhenDate()
	start := 0
	rows := batchSize
	workReader.MessageLog.Info("Fetching files not checked since %s in batches of %d",
		sinceWhen.Format(time.RFC822Z), rows)
	if *limit > 0 {
		workReader.MessageLog.Info("Will queue up to %d items, in chunks of %d",
			*limit, batchSize)
	}
	for {
		fileCount, err := fetchAndQueueBatch(sinceWhen, start, rows)
		if err != nil {
			workReader.MessageLog.Error("Error getting items items needing fixity check: %v", err)
			break
		}
		if fileCount == 0 {
			workReader.MessageLog.Info("Last request returned 0 items needing fixity.")
			workReader.MessageLog.Info("Finished getting data from Fluctus")
			break
		} else {
			workReader.MessageLog.Info("Found %d items needing fixity check", fileCount)
			start += rows
			if *limit > 0 && *limit <= start {
				workReader.MessageLog.Info("Queued %d items. Stopping because limit is %d", start, *limit)
				workReader.MessageLog.Info("Did I queue a few too many? It's because I queue in batches of %d",
					batchSize)
				break
			}
		}
	}
}

// Get the date we should be checking against. We're looking for
// files with no fixity date since this date. User can pass the
// date in on the command like using the -date flag, but typically
// we will just calculate the date based on the config file settings.
func getSinceWhenDate() (time.Time) {
	var err error
	daysAgo := time.Duration(workReader.Config.MaxDaysSinceFixityCheck * -24) * time.Hour
	sinceWhen := time.Now().UTC().Add(daysAgo)
	if cmdLineDate != nil && *cmdLineDate != "" {
		sinceWhen, err = time.Parse(time.RFC3339, *cmdLineDate)
		if err != nil {
			workReader.MessageLog.Error("Expected date format '2006-01-02T15:04:05Z' but got %s",
				*cmdLineDate)
			workReader.MessageLog.Fatal(err)
		}
		workReader.MessageLog.Info("Using date '%s' passed in on command line", *cmdLineDate)
	} else {
		workReader.MessageLog.Info(
			"Calculated date from config.MaxDaysSinceFixityCheck: %d days ago",
			workReader.Config.MaxDaysSinceFixityCheck)
	}
	return sinceWhen
}

// Fetches a batch of generic files needing fixity check and queues them
// in NSQ. Returns the number of items queued.
func fetchAndQueueBatch(sinceWhen time.Time, start, rows int) (int, error) {
	url := fmt.Sprintf("%s/mput?topic=%s", workReader.Config.NsqdHttpAddress,
		workReader.Config.FixityWorker.NsqTopic)
	genericFiles, err := workReader.FluctusClient.GetFilesNotCheckedSince(sinceWhen, start, rows)
	if err != nil {
		return 0, err
	}
	fileCount := len(genericFiles)
	if fileCount > 0 {
		genericSlice := make([]interface{}, len(genericFiles))
		for i := range genericFiles {
			genericSlice[i] = genericFiles[i]
		}
		bagman.QueueToNSQ(url, genericSlice)
	}
	return fileCount, nil
}

func logBatch(genericFiles []*bagman.GenericFile) {
	for _, genericFile := range genericFiles {
		workReader.MessageLog.Info("Put %s into fixity_check queue (%s)",
			genericFile.Identifier, genericFile.URI)
	}
}
