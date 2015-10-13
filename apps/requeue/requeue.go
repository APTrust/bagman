// requeue puts a blob of json into the specified queue
// so that we can reprocess it. This is particularly useful
// for items in the trouble queue, whose JSON status is
// written into /mnt/apt/logs/ingest_failures on the ingest
// server and /mnt/apt/logs/replication_failures on the
// restore server. After fixing the bug that caused the failure,
// you can put the item back in the queue, and processing will
// pick up where it left off.
package main

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"os"
	"strings"
	"time"
)

// *************************************************************************
// This file contains stubs and notes. It doesn't work yet.
// Maybe you can fix that on the plane, Andrew!
// *************************************************************************

// Usage:
//
// ./requeue -config=<config> <filename>
// ./requeue -config=<config> <filename*>
//
// TODO:
//
// 1. Open file.
// 2. Change retry to true.
// 3. Figure out which queue it should go into and put it there.


var procUtil *bagman.ProcessUtil
var statusCache map[string]*bagman.ProcessStatus

func main() {
	var err error = nil
	procUtil = workers.CreateProcUtil()
	procUtil.MessageLog.Info("requeue started")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Initialization failed for requeue: %v", err)
		os.Exit(1)
	}
	run()
}

type DateParseError struct {
    message   string
}
func (e DateParseError) Error() string { return e.message }

func run() {

	// Item will be an instance of bagman.ProcessResult
	// item := readItemFromFile()

	// Get the status record
	// statusRecord := getStatusRecord(item.S3File)

	// Set retry to true on the status record...
	// [Do we really need this? Do we check retry after initial ingest steps?]
	// statusRecord.Retry = true

	// Send status record back to Fluctus
	// err = procUtil.FluctusClient.SendProcessedItem(statusRecord)

	// Prepare the item for the queue by resetting the appropriate
	// retry flags AND figuring out which queue it belongs in.
	// (Where in the process did it fail?)
	// queueName := prepareForQueue(item)

	// Topic here will depend on where in the process the item failed.
	// url := fmt.Sprintf("%s/put?topic=%s", procUtil.Config.NsqdHttpAddress,
	//	procUtil.Config.PrepareWorker.NsqTopic)
	// err = bagman.QueueToNSQ(url, item)
	// if err != nil {
	// 	procUtil.MessageLog.Fatal(err.Error())
	// }
}


// Reset the retry flag(s) and return the name of the queue this should go into.
func prepareForQueue() () {

}

// CHANGE: This should retrieve the status record, set retry to true, then save it.
// Use bagman.FluctusClient.SendProcessedItem to update the status record.
func getStatusRecord(s3File *bagman.S3File) (status *bagman.ProcessStatus, err error) {
	bagDate, err := time.Parse(bagman.S3DateFormat, s3File.Key.LastModified)
	if err != nil {
		msg := fmt.Sprintf("Cannot parse S3File mod date '%s'. "+
			"File %s will be re-processed.",
			s3File.Key.LastModified, s3File.Key.Key)
		return nil, DateParseError { message: msg, }
	}
	etag := strings.Replace(s3File.Key.ETag, "\"", "", 2)
	status, err = procUtil.FluctusClient.GetBagStatus(etag, s3File.Key.Key, bagDate)
	return status, err
}
