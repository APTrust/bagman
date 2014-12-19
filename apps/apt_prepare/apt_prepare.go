package main

import (
	"github.com/APTrust/bagman/workers"
)

// apt_prepare receives messages from nsqd describing
// items in the S3 receiving buckets. It fetches, untars,
// and validates tar files, then queues them for storage,
// if they untar and validate successfully.
func main() {
	procUtil := workers.CreateProcUtil()
	procUtil.MessageLog.Info("Connecting to NSQLookupd at %s", procUtil.Config.NsqLookupd)
	procUtil.MessageLog.Info("NSQDHttpAddress is %s", procUtil.Config.NsqdHttpAddress)
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.PrepareWorker)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}
	procUtil.MessageLog.Info("apt_prepare started")
	bagPreparer := workers.NewBagPreparer(procUtil)
	consumer.SetHandler(bagPreparer)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}
