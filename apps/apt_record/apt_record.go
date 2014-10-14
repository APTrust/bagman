package main

import (
	"github.com/APTrust/bagman/workers"
)

/*
apt_record records bag metadata in Fluctus, including
info about Intellectual Objects, Generic Files and Premis Events.
*/
func main() {
	procUtil := workers.CreateProcUtil()
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.RecordWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("apt_record started")
	bagRecorder := workers.NewBagRecorder(procUtil)
	consumer.SetHandler(bagRecorder)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}
