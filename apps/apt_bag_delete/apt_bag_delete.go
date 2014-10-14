package main

import (
	"github.com/APTrust/bagman/workers"
)

/*
apt_bag_delete.go deletes tar files from the partners' S3 receiving buckets
after those files have been successfully ingested.

If you want to clean up failed bits of multipart S3 uploads in the
preservation bucket, see multiclean.go.
*/
func main() {
	procUtil := workers.CreateProcUtil()
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.BagDeleteWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("apt_bag_delete started")
	bagDeleter := workers.NewBagDeleter(procUtil)
	consumer.SetHandler(bagDeleter)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}
