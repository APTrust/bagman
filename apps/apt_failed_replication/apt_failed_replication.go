package main

import (
	"github.com/APTrust/bagman/workers"
)

// apt_failed_replication dumps information about failed attempts
// to copy generic files to the replication bucket in Oregon.
func main() {
	procUtil := workers.CreateProcUtil()
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.FailedReplicationWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("apt_failed_replication started")
	processor := workers.NewFailedReplicationProcessor(procUtil)
	consumer.AddHandler(processor)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}
