package main

import (
	"github.com/APTrust/bagman/workers"
)

// apt_replicate copies items from the S3 preservation bucket
// in Virginia to the S3 replication bucket in Oregon.
func main() {
	procUtil := workers.CreateProcUtil()
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.ReplicationWorker)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}
	procUtil.MessageLog.Info("apt_replicate started")
	replicator := workers.NewReplicator(procUtil)
	consumer.AddHandler(replicator)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}
